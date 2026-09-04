# -*- coding: utf-8 -*-
"""
astrbot_plugin_mcserver_monitor
===============================

直连 Minecraft（Java 版）服务器地址的监控插件——只检测服务器，不依赖任何面板。

原理：按周期对每台服务器地址执行 Minecraft Server List Ping（0x00 握手 + 0x00 状态请求），
从服务器返回的状态 JSON 中读取：
- 在线/离线（能否连通并正常应答）；
- 当前在线人数 / 最大在线人数（players.online / players.max）；
- 在线玩家名单（players.sample，被服务器隐藏时为空）。

功能：
- 多服务器监控：/mcadd /mcmod /mcdel 指令快捷增、改、删服务器；
- 事件推送：开服（离线→在线）、关服（在线→离线）、在线人数/玩家名单变动，
  推送到 /mcntfy 订阅的群/私聊会话；
- 每日统计：记录每台服务器当天出现过的【去重】玩家名单，每天 0 点自动总结推送，
  并持久化存档（/mcsyn 可随时查看当天进度）。

要求 AstrBot >= 4.16。纯 asyncio 标准库实现，无第三方依赖。
兼容 Paper / Spigot / Bukkit / Forge / Fabric，以及 BungeeCord / Velocity 等群组代理。
（基岩版 Bedrock 使用 RakNet 协议，与本插件不兼容。）
"""

from __future__ import annotations

import asyncio
import json
import re
import socket
from datetime import datetime
from typing import Any, Optional

from astrbot.api import logger
from astrbot.api.event import AstrMessageEvent, MessageChain, filter
from astrbot.api.star import Context, Star, register

#: 握手时声明的协议版本（服务器做状态查询时会忽略客户端声明值，任意值均可）
MC_PROTOCOL_VERSION = 765
#: 状态响应 JSON 的最大字节数，防止恶意超大响应
MAX_STATUS_JSON = 32767
#: 每日统计存档保留天数
DAILY_HISTORY_DAYS = 60
#: 消息中玩家名单最多列出的条数
PLAYER_LIST_CAP = 40

#: 时钟函数（可注入以便测试 0 点滚动）
NOW_FN = datetime.now


# ---------------------------------------------------------------------------
# Minecraft Server List Ping 协议（最小实现）
# ---------------------------------------------------------------------------


def encode_varint(value: int) -> bytes:
    """把整数编码为 Minecraft VarInt。"""
    out = bytearray()
    while True:
        byte = value & 0x7F
        value >>= 7
        if value:
            out.append(byte | 0x80)
        else:
            out.append(byte)
            return bytes(out)


async def read_varint(reader: asyncio.StreamReader, max_bytes: int = 5) -> int:
    """从流中读取一个 VarInt。"""
    value = 0
    for i in range(max_bytes):
        b = await reader.readexactly(1)
        value |= (b[0] & 0x7F) << (7 * i)
        if not (b[0] & 0x80):
            return value
    raise ValueError("VarInt 编码过长")


def encode_string(text: str) -> bytes:
    """编码 Minecraft 字符串（VarInt 长度前缀 + UTF-8）。"""
    data = text.encode("utf-8")
    return encode_varint(len(data)) + data


class ProbeError(Exception):
    """服务器探测失败（视为离线）。"""


def parse_address(address: str, default_port: int = 25565) -> tuple[str, int]:
    """把配置的服务器地址解析为 (host, port)。支持 'host:port' 形式。"""
    addr = (address or "").strip()
    if not addr:
        raise ProbeError("未配置服务器地址")
    if addr.startswith("["):  # IPv6 字面量，如 [::1]:25565
        m = re.match(r"^\[([^\]]+)\](?::(\d+))?$", addr)
        if m:
            host = m.group(1)
            port = int(m.group(2)) if m.group(2) else default_port
            return host, port
        return addr.strip("[]"), default_port
    if ":" in addr:
        host, _, port_str = addr.rpartition(":")
        if port_str.isdigit():
            return host.strip(), int(port_str)
    return addr, default_port


def _strip_color(text: str) -> str:
    """去掉 Minecraft 格式代码（§x）并压缩空白。"""
    text = re.sub(r"\u00a7.", "", text)
    return " ".join(text.split())


def _flatten_chat(desc: Any) -> str:
    """把 MC JSON 聊天组件（1.19+ 的 description）展平为纯文本。"""
    if isinstance(desc, str):
        return desc
    if isinstance(desc, dict):
        parts: list[str] = []
        text = desc.get("text")
        if text:
            parts.append(str(text))
        extra = desc.get("extra")
        if isinstance(extra, list):
            for e in extra:
                parts.append(_flatten_chat(e))
        return "".join(parts)
    return str(desc)


async def status_query(
    host: str,
    port: int,
    timeout: float = 5.0,
    protocol_version: int = MC_PROTOCOL_VERSION,
) -> dict:
    """执行一次 Server List Ping，返回服务器状态 JSON 字典。

    失败时抛出 ProbeError（视为服务器离线）。
    """
    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(host, port), timeout
        )
    except asyncio.TimeoutError:
        raise ProbeError("连接超时")
    except socket.gaierror:
        raise ProbeError("无法解析主机名")
    except ConnectionRefusedError:
        raise ProbeError("连接被拒绝（服务器未开启或端口错误）")
    except OSError as e:
        raise ProbeError(f"网络错误: {e}")

    try:
        # 1) 握手包：包ID 0x00 + 协议版本 + 主机名 + 端口(u16) + 下一状态=1(状态查询)
        handshake = (
            b"\x00"
            + encode_varint(protocol_version)
            + encode_string(host)
            + port.to_bytes(2, "big")
            + b"\x01"
        )
        writer.write(encode_varint(len(handshake)) + handshake)
        await writer.drain()

        # 2) 状态请求包：包ID 0x00
        status_req = encode_varint(0x00)
        writer.write(encode_varint(len(status_req)) + status_req)
        await writer.drain()

        # 3) 读取响应：包长(VarInt) + 包ID(VarInt) + 状态JSON
        async def _exchange():
            packet_len = await read_varint(reader)
            if packet_len < 1 or packet_len > 1 + MAX_STATUS_JSON + 10:
                raise ProbeError("响应包长度异常")
            packet = await reader.readexactly(packet_len)
            # 解析包内：包ID + 字符串长度 + JSON
            pos = 0
            pid = 0
            for _ in range(5):
                b = packet[pos]
                pos += 1
                pid |= (b & 0x7F) << (7 * _)
                if not (b & 0x80):
                    break
            if pid != 0x00:
                raise ProbeError(f"意外响应类型: {pid}")
            str_len = 0
            for _ in range(5):
                b = packet[pos]
                pos += 1
                str_len |= (b & 0x7F) << (7 * _)
                if not (b & 0x80):
                    break
            if str_len > MAX_STATUS_JSON:
                raise ProbeError("状态响应过大")
            payload = packet[pos : pos + str_len]
            return json.loads(payload.decode("utf-8"))

        status = await asyncio.wait_for(_exchange(), timeout)
        return status
    except asyncio.TimeoutError:
        raise ProbeError("服务器响应超时")
    except (ConnectionResetError, ConnectionAbortedError, EOFError):
        raise ProbeError("服务器未响应（连接被重置）")
    except asyncio.IncompleteReadError:
        raise ProbeError("响应不完整")
    except json.JSONDecodeError:
        raise ProbeError("服务器返回了无法解析的响应")
    except (ValueError, UnicodeDecodeError) as e:
        raise ProbeError(f"协议解析失败: {e}")
    except OSError as e:
        raise ProbeError(f"网络错误: {e}")
    finally:
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass


def parse_status(status: dict) -> dict:
    """把服务器状态 JSON 归一化为监控快照。"""
    players = status.get("players") or {}
    sample_raw = players.get("sample") or []
    names: list[str] = []
    for item in sample_raw:
        if isinstance(item, dict) and item.get("name"):
            names.append(_strip_color(str(item["name"])))

    version = status.get("version") or {}
    motd = _strip_color(_flatten_chat(status.get("description", "")))
    return {
        "online": True,
        "motd": motd,
        "version": str(version.get("name") or ""),
        "protocol": version.get("protocol"),
        "online_players": int(players.get("online") or 0),
        "max_players": int(players.get("max") or 0),
        "players": names,
        "reason": "",
    }


# ---------------------------------------------------------------------------
# 插件主体
# ---------------------------------------------------------------------------


@register(
    "astrbot_plugin_mcserver_monitor",
    "dsh-bot",
    "Minecraft 服务器直连监控与推送",
    "1.1.0",
    "",
)
class MCServerMonitor(Star):
    """直连 Minecraft 服务器地址的只读监控插件（支持多服务器 + 每日统计）。

    指令：
    /mcadd <名称> <地址>  快捷添加服务器（如：/mcadd 生存服 play.example.com:25565）
    /mcmod <名称> <地址>  修改已添加服务器的地址
    /mcdel <名称>         删除服务器（历史存档保留）
    /mcsta                查询服务器状态・在线玩家名单・订阅推送的会话数
    /mcsyn                查看今日已统计的去重游玩人数
    /mcntfy               订阅：把当前群/私聊加入服务器事件推送目标
    /mcuntfy              取消订阅当前会话
    /mcconfig             查看当前生效配置
    """

    def __init__(self, context: Context, config: Any = None):
        super().__init__(context)
        self.config = config

        self._poll_task: Optional[asyncio.Task] = None

        #: 已订阅推送目标的 unified_msg_origin 会话列表
        self._sessions: set[str] = set()
        #: 服务器列表：[{"id", "name", "address", "port"}]
        self._servers: list[dict] = []
        #: 每台服务器上一轮探测快照（首次轮询仅记录基线，不触发推送）
        self._prev: dict[str, dict] = {}
        #: 今日已出现的去重玩家集合：{server_id: set(name)}
        self._daily: dict[str, set] = {}
        #: 当前统计日期字符串 YYYY-MM-DD
        self._day: str = NOW_FN().strftime("%Y-%m-%d")
        #: 历史存档：[{"date", "players": {server_id: [names]}}]（保留最近 N 天）
        self._history: list[dict] = []
        #: 已推送过总结的日期（避免重启重复推送）
        self._summarized: set[str] = set()
        self._warned_no_server = False

    # ------------------------------------------------------------- 配置读取

    def _cfg(self, key: str, default: Any = None) -> Any:
        """读取插件配置项（兼容 AstrBotConfig 与普通 dict）。"""
        conf = self.config
        if conf is None:
            return default
        try:
            v = conf.get(key)
        except AttributeError:
            v = getattr(conf, key, None)
        return default if v is None else v

    def _interval(self) -> float:
        try:
            return max(3.0, float(self._cfg("poll_interval", 10)))
        except (TypeError, ValueError):
            return 10.0

    def _timeout(self) -> float:
        try:
            return max(1.0, float(self._cfg("ping_timeout", 5)))
        except (TypeError, ValueError):
            return 5.0

    def _address_of(self, rec: dict) -> str:
        return f"{rec['address']}:{rec['port']}" if rec["port"] != 25565 else rec["address"]

    # ------------------------------------------------------------- 生命周期

    async def initialize(self) -> None:
        """插件激活时：加载订阅列表/服务器/每日统计，处理跨天，启动轮询任务。"""
        try:
            sessions = await self.get_kv_data("sessions", [])
            self._sessions = set(str(s) for s in (sessions or []))
        except Exception as e:
            logger.warning(f"读取推送订阅数据失败: {e}")
            self._sessions = set()

        try:
            servers = await self.get_kv_data("servers", [])
            self._servers = [dict(s) for s in (servers or [])]
        except Exception as e:
            logger.warning(f"读取服务器列表失败: {e}")
            self._servers = []

        # 兼容旧版单服务器配置：首次启用时自动导入为服务器列表
        if not self._servers:
            await self._seed_server_from_config()

        try:
            daily = await self.get_kv_data("daily", None)
            history = await self.get_kv_data("history", [])
            summarized = await self.get_kv_data("daily_summarized", [])
            self._history = list(history or [])
            self._summarized = set(str(d) for d in (summarized or []))
            today = NOW_FN().strftime("%Y-%m-%d")
            if daily and daily.get("date"):
                saved_day = str(daily.get("date"))
                saved_players = daily.get("players") or {}
                if saved_day < today:
                    # 跨天（含插件停了一夜的情况）：先总结已保存的那一天，再开始新的一天
                    self._day = saved_day
                    self._daily = {
                        str(k): set(str(v) for v in names)
                        for k, names in saved_players.items()
                    }
                    await self._finalize_day(today)
                elif saved_day == today:
                    self._day = today
                    self._daily = {
                        str(k): set(str(v) for v in names)
                        for k, names in saved_players.items()
                    }
        except Exception as e:
            logger.warning(f"读取每日统计失败: {e}")

        if not self._servers:
            logger.warning(
                "MC 服务器监控插件：尚未添加任何服务器，请用 /mcadd 名称 地址 添加，"
                "或在插件设置中填写 server_address 后重载插件"
            )
        else:
            summary = "、".join(
                f"「{s['name']}」{self._address_of(s)}" for s in self._servers
            )
            logger.info(
                f"MC 服务器监控插件已初始化，共 {len(self._servers)} 台服务器：{summary}；"
                f"已订阅推送目标 {len(self._sessions)} 个"
            )

        self._poll_task = asyncio.create_task(self._poll_loop())

    async def terminate(self) -> None:
        """插件停用/重载时：保存数据并取消轮询任务。"""
        try:
            await self._save_daily()
            await self._save_servers()
        except Exception as e:
            logger.warning(f"保存数据失败: {e}")
        if self._poll_task:
            self._poll_task.cancel()
            try:
                await self._poll_task
            except (asyncio.CancelledError, Exception):
                pass
            self._poll_task = None
        logger.info("MC 服务器监控插件已停止")

    async def _seed_server_from_config(self) -> None:
        """把旧版配置中的 server_address 导入为第一台服务器（仅列表为空时）。"""
        address = str(self._cfg("server_address", "") or "").strip()
        if not address:
            return
        try:
            host, port = parse_address(
                address, int(self._cfg("server_port", 25565) or 25565)
            )
        except ProbeError as e:
            logger.warning(f"导入 server_address 失败: {e}")
            return
        self._servers = [
            {"id": "s1", "name": host, "address": host, "port": port}
        ]
        await self._save_servers()
        logger.info(f"已从配置导入服务器「{host}」（{host}:{port}）")

    # ------------------------------------------------------------- KV 持久化

    async def _save_servers(self) -> None:
        try:
            await self.put_kv_data("servers", self._servers)
        except Exception as e:
            logger.warning(f"保存服务器列表失败: {e}")

    async def _save_daily(self) -> None:
        try:
            await self.put_kv_data(
                "daily",
                {
                    "date": self._day,
                    "players": {
                        rid: sorted(names) for rid, names in self._daily.items()
                    },
                },
            )
            await self.put_kv_data("history", self._history)
            await self.put_kv_data("daily_summarized", sorted(self._summarized))
        except Exception as e:
            logger.warning(f"保存每日统计失败: {e}")

    async def _save_sessions(self) -> None:
        try:
            await self.put_kv_data("sessions", sorted(self._sessions))
        except Exception as e:
            logger.warning(f"保存订阅列表失败: {e}")

    # ------------------------------------------------------------- 推送

    async def _push(self, text: str) -> int:
        """向所有已订阅会话主动推送一条消息。"""
        if not self._sessions:
            return 0
        chain = MessageChain().message(text)
        sent = 0
        for session in list(self._sessions):
            try:
                ok = await self.context.send_message(session, chain)
                if ok:
                    sent += 1
                else:
                    logger.warning(f"推送失败（未找到对应平台）：{session}")
            except Exception as e:
                logger.warning(f"推送异常 {session}: {e}")
        if self._cfg("debug", False):
            logger.debug(f"已向 {sent}/{len(self._sessions)} 个会话推送：{text[:50]}")
        return sent

    # ------------------------------------------------------------- 轮询监控

    async def _poll_loop(self) -> None:
        """后台轮询任务：定期探测所有服务器并对比推送事件。"""
        while True:
            try:
                await self._poll_once()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                if self._cfg("debug", False):
                    logger.warning(f"轮询异常: {e}")
            await asyncio.sleep(self._interval())

    async def _probe(self, rec: dict) -> dict:
        """探测一台服务器，返回归一化快照；失败视为离线快照。"""
        try:
            status = await status_query(
                rec["address"], rec["port"], timeout=self._timeout()
            )
            return parse_status(status)
        except ProbeError as e:
            if self._cfg("debug", False):
                logger.debug(f"探测失败（视为离线）: {rec['name']} - {e}")
            return {
                "online": False,
                "motd": "",
                "version": "",
                "protocol": None,
                "online_players": 0,
                "max_players": 0,
                "players": [],
                "reason": str(e),
            }

    async def _poll_once(self) -> None:
        if not self._servers:
            if not self._warned_no_server:
                logger.warning("MC 服务器监控：未添加任何服务器，跳过轮询。")
                self._warned_no_server = True
            return

        today = NOW_FN().strftime("%Y-%m-%d")
        if today != self._day:
            await self._finalize_day(today)

        for rec in list(self._servers):
            try:
                snap = await self._probe(rec)
            except Exception as e:
                if self._cfg("debug", False):
                    logger.warning(f"探测 {rec['name']} 异常: {e}")
                continue
            await self._handle_snapshot(rec, snap)
            await self._track_daily(rec, snap)

    async def _handle_snapshot(self, rec: dict, snap: dict) -> None:
        sid = rec["id"]
        prev = self._prev.get(sid)
        if prev is None:
            # 首次轮询：记录基线，不推送（避免插件启动/重载造成误报）
            self._prev[sid] = snap
            st = "在线" if snap["online"] else f"离线（{snap['reason']}）"
            logger.info(
                f"MC 服务器监控基线：「{rec['name']}」{st}，"
                f"在线 {snap['online_players']}/{snap['max_players']}"
            )
            return

        name = rec["name"]
        # 1) 开服：离线 -> 在线
        if snap["online"] and not prev["online"]:
            if self._cfg("notify_on_start", True):
                await self._push(
                    f"服务器「{name}」已开服 喵~"
                    f"当前在线 {snap['online_players']}/{snap['max_players']}"
                )
        # 2) 关服：在线 -> 离线
        elif (not snap["online"]) and prev["online"]:
            if self._cfg("notify_on_stop", True):
                await self._push(f"服务器「{name}」已关服。")

        # 3) 人数变动：仅在持续在线时比较
        if snap["online"] and prev["online"]:
            if self._cfg("notify_on_player_change", True):
                await self._handle_player_change(rec, prev, snap)

        self._prev[sid] = snap

    def _bullet_player_list(self, players: list[str], cap: int = PLAYER_LIST_CAP) -> str:
        """把玩家名整理为“• name”每行一条的格式。"""
        if not players:
            return ""
        shown = players[:cap]
        lines = [f"• {p}" for p in shown]
        if len(players) > cap:
            lines.append(f"…… 等 {len(players)} 人")
        return "\n".join(lines)

    async def _handle_player_change(self, rec: dict, prev: dict, snap: dict) -> None:
        cur_names = set(snap["players"])
        prev_names = set(prev["players"])
        joined = sorted(cur_names - prev_names)
        left = sorted(prev_names - cur_names)
        name = rec["name"]
        cur, mx = snap["online_players"], snap["max_players"]

        count_changed = snap["online_players"] != prev["online_players"]
        names_available = bool(prev_names or cur_names)
        use_details = bool(self._cfg("notify_join_leave_details", True))

        if not joined and not left and not count_changed:
            return

        # 有名单且开启名单播报：播报具体玩家名（名单不可用时回退到人数变化）
        if (joined or left) and use_details and names_available:
            msg_parts: list[str] = []
            if joined:
                msg_parts.append(
                    f"「{name}」\n ※ {'、'.join(joined)} 加入了游戏喵~（{cur}/{mx}）"
                )
            if left:
                msg_parts.append(
                    f"「{name}」\n ※ {'、'.join(left)} 退出了游戏喵~（{cur}/{mx}）"
                )
            bullet_list = self._bullet_player_list(snap["players"])
            if bullet_list:
                msg_parts.append(f"当前在线：\n{bullet_list}")
            await self._push("\n".join(msg_parts))
            return

        # 只有人数变化（或未启用名单播报）：播报人数
        direction = "增加" if cur > prev["online_players"] else "减少"
        if cur != prev["online_players"]:
            parts = [
                f"「{name}」\n ※ 在线人数{direction}："
                f"{prev['online_players']} → {cur}（{cur}/{mx}）"
            ]
            bullet_list = self._bullet_player_list(snap["players"])
            if bullet_list:
                parts.append(f"当前在线：\n{bullet_list}")
            await self._push("\n".join(parts))

    # ------------------------------------------------------------- 每日统计

    async def _track_daily(self, rec: dict, snap: dict) -> None:
        """把在线玩家累积进今日去重集合（仅在线且名单可用时）。"""
        if not snap["online"] or not snap["players"]:
            return
        sid = rec["id"]
        bucket = self._daily.setdefault(sid, set())
        new_names = set(snap["players"]) - bucket
        if new_names:
            bucket |= new_names
            if self._cfg("debug", False):
                logger.debug(
                    f"每日统计「{rec['name']}」新增 {len(new_names)} 位玩家，"
                    f"今日累计 {len(bucket)} 位"
                )
            await self._save_daily()

    async def _finalize_day(self, new_today: str) -> None:
        """总结 self._day 这一天：推送总结、归档存档、重置为新的一天。"""
        day = self._day
        blocks: list[str] = []
        for rec in self._servers:
            names = sorted(self._daily.get(rec["id"], set()))
            if names:
                blocks.append(
                    f"「{rec['name']}」今日共有 {len(names)} 位玩家：\n"
                    + self._bullet_player_list(names)
                )
            else:
                blocks.append(f"「{rec['name']}」今日共有 0 位玩家。")
        summary = f" {day} 服务器游玩总结：\n" + "\n".join(blocks)

        if self._cfg("notify_daily_summary", True) and day not in self._summarized:
            await self._push(summary)
            self._summarized.add(day)

        # 归档：仅保存有记录的服务器
        archived = {
            rid: sorted(names)
            for rid, names in self._daily.items()
            if names
        }
        self._history.append({"date": day, "players": archived})
        self._history = self._history[-DAILY_HISTORY_DAYS:]

        # 重置为新的一天
        self._daily = {}
        self._day = new_today
        await self._save_daily()
        logger.info(f"已完成 {day} 的每日总结，开始统计 {new_today}")

    # ------------------------------------------------------------- 指令

    @filter.command("mcntfy")
    async def mc_ntfy(self, event: AstrMessageEvent):
        """订阅当前会话为服务器事件推送目标。"""
        session = event.unified_msg_origin
        self._sessions.add(session)
        await self._save_sessions()
        yield event.plain_result(
            f"✅ 已订阅服务器事件推送，当前共 {len(self._sessions)} 个目标。\n"
        )

    @filter.command("mcuntfy")
    async def mc_untfy(self, event: AstrMessageEvent):
        """取消订阅当前会话。"""
        session = event.unified_msg_origin
        if session in self._sessions:
            self._sessions.discard(session)
            await self._save_sessions()
            yield event.plain_result(
                f"✅ 已取消订阅，剩余 {len(self._sessions)} 个推送目标。"
            )
        else:
            yield event.plain_result("ℹ️ 当前会话未订阅推送。可用 /mcntfy 订阅。")

    @filter.command("mcadd")
    async def mc_add(self, event: AstrMessageEvent, label: str, address: str):
        """快捷添加服务器：/mcadd 名称 地址"""
        label = (label or "").strip()
        address = (address or "").strip()
        if not label or not address:
            yield event.plain_result(
                "用法：/mcadd 名称 地址\n例如：/mcadd 生存服 play.example.com:25565"
            )
            return
        if any(s["name"] == label for s in self._servers):
            yield event.plain_result(
                f"❌ 已存在名为「{label}」的服务器，如需修改地址请用 /mcmod {label} 新地址"
            )
            return
        try:
            host, port = parse_address(
                address, int(self._cfg("server_port", 25565) or 25565)
            )
        except ProbeError as e:
            yield event.plain_result(f"❌ 地址无效：{e}")
            return
        sid = f"s{len(self._servers) + 1}"
        self._servers.append({"id": sid, "name": label, "address": host, "port": port})
        await self._save_servers()
        yield event.plain_result(
            f"✅ 已添加服务器「{label}」（{host}:{port}）。\n"
            f"可用 /mcsta 查看状态，/mcntfy 订阅事件推送。"
        )

    @filter.command("mcmod")
    async def mc_mod(self, event: AstrMessageEvent, label: str, address: str):
        """修改服务器的地址：/mcmod 名称 新地址"""
        label = (label or "").strip()
        address = (address or "").strip()
        if not label or not address:
            yield event.plain_result(
                "用法：/mcmod 名称 新地址\n例如：/mcmod 生存服 new.example.com:25565"
            )
            return
        rec = next((s for s in self._servers if s["name"] == label), None)
        if not rec:
            yield event.plain_result(f"❌ 未找到名为「{label}」的服务器，可用 /mcadd 添加。")
            return
        try:
            host, port = parse_address(
                address, int(self._cfg("server_port", 25565) or 25565)
            )
        except ProbeError as e:
            yield event.plain_result(f"❌ 地址无效：{e}")
            return
        old = f"{rec['address']}:{rec['port']}"
        rec["address"], rec["port"] = host, port
        # 地址变更后清空该服务器的基线，避免误报开关服
        self._prev.pop(rec["id"], None)
        await self._save_servers()
        yield event.plain_result(
            f"✅ 已修改服务器「{label}」：{old} → {host}:{port}"
        )

    @filter.command("mcdel")
    async def mc_del(self, event: AstrMessageEvent, label: str):
        """删除服务器：/mcdel 名称"""
        label = (label or "").strip()
        if not label:
            yield event.plain_result("用法：/mcdel 名称")
            return
        rec = next((s for s in self._servers if s["name"] == label), None)
        if not rec:
            yield event.plain_result(f"❌ 未找到名为「{label}」的服务器。")
            return
        self._servers = [s for s in self._servers if s["id"] != rec["id"]]
        self._prev.pop(rec["id"], None)
        self._daily.pop(rec["id"], None)
        await self._save_servers()
        await self._save_daily()
        yield event.plain_result(f"✅ 已删除服务器「{label}」。历史存档保留。")

    @filter.command("mcsta")
    async def mc_sta(self, event: AstrMessageEvent):
        """查询服务器状态・在线玩家名单・订阅推送的会话数。"""
        text = await self._build_status_text()
        yield event.plain_result(text)

    @filter.command("mcsyn")
    async def mc_syn(self, event: AstrMessageEvent):
        """查看今日已统计的去重游玩人数。"""
        text = await self._build_summary_text()
        yield event.plain_result(text)

    @filter.command("mcconfig")
    async def mc_config(self, event: AstrMessageEvent):
        """查看当前生效配置。"""
        yield event.plain_result(self._build_config_text())

    # ------------------------------------------------------------- 输出构建

    async def _build_status_text(self) -> str:
        if not self._servers:
            return "⚠️ 尚未添加服务器。用 /mcadd 名称 地址 添加。"
        lines = [f"服务器状态（{len(self._servers)} 台）："]
        snaps = await asyncio.gather(*[self._probe(r) for r in self._servers])
        for rec, snap in zip(self._servers, snaps):
            addr = self._address_of(rec)
            if snap["online"]:
                lines.append(
                    f"✅ {rec['name']}（{addr}）・在线・{snap['online_players']}/{snap['max_players']}"
                )
                if snap["players"]:
                    tip = "、".join(snap["players"][:20])
                    if len(snap["players"]) > 20:
                        tip += f" 等 {len(snap['players'])} 人"
                    lines.append(f"在线玩家：{tip}")
                else:
                    lines.append("在线玩家：（服务器未提供名单或暂无人）")
            else:
                lines.append(f"❌ {rec['name']}（{addr}）・离线・{snap['reason']}")
        lines.append(f"订阅推送的会话：{len(self._sessions)} 个")
        return "\n".join(lines)

    async def _build_summary_text(self) -> str:
        day = self._day
        if not self._servers:
            return f"今日（{day}）：尚未添加服务器。用 /mcadd 名称 地址 添加。"
        blocks: list[str] = [f"（{day}）今日服务器统计："]
        for rec in self._servers:
            names = sorted(self._daily.get(rec["id"], set()))
            if names:
                blocks.append(
                    f"「{rec['name']}」今日已有 {len(names)} 位玩家游玩：\n"
                    + self._bullet_player_list(names)
                )
            else:
                blocks.append(f"「{rec['name']}」今日已有 0 位玩家游玩。")
        return "\n".join(blocks)

    def _build_config_text(self) -> str:
        if self._servers:
            server_desc = "、".join(
                f"{s['name']}({self._address_of(s)})" for s in self._servers
            )
        else:
            server_desc = "（尚未添加，可在设置填写 server_address 或 /mcadd）"
        lines = [
            "当前插件配置：",
            f"  服务器：{server_desc}",
            f"  探测超时：{self._timeout():.0f} 秒",
            f"  轮询间隔：{self._interval():.0f} 秒",
            f"  推送开关：开服={bool(self._cfg('notify_on_start', True))} "
            f"关服={bool(self._cfg('notify_on_stop', True))} "
            f"人数={bool(self._cfg('notify_on_player_change', True))} "
            f"每日总结={bool(self._cfg('notify_daily_summary', True))}",
            f"  玩家名播报：{bool(self._cfg('notify_join_leave_details', True))}",
            f"  已订阅目标数：{len(self._sessions)}",
        ]
        return "\n".join(lines)