# astrbot_plugin_mcserver_monitor

**MC 服务器消息订阅（AstrBot）**

直连 Minecraft（Java 版）服务器地址，周期执行 **Server List Ping** 探测：
检测 **开服、关服、在线人数与玩家名单变动**，并主动推送到 `/mcntfy` 订阅的群/私聊。
支持**添加多台服务器**，并记录**每台服务器当天去重游玩人数**，**每天 0 点自动总结推送**。


![兼容](https://img.shields.io/badge/AstrBot-%3E%3D4.16-blue)
![兼容](https://img.shields.io/badge/Minecraft-Java%20Edition-brightgreen)

---

## 功能

- 🔍 **直连探测**：用 Minecraft Server List Ping 协议直接询问服务器，拿到
  在线/离线、当前/最大人数、玩家名单、版本、简介
- 🗂 **多服务器**：`/mcadd` `/mcmod` `/mcdel` 指令快捷增、改、删服务器
- 🟢 **开服通知**：离线 → 在线 时推送「已开服」
- 🔴 **关服通知**：在线 → 离线 时推送「已关服」
- 🟡 **人数变动通知**：在线期间监测加入/退出，可播报具体玩家名
  （服务器开放列表时；否则自动回退为仅播报人数变化）
- 📊 **每日去重统计**：记录每台服务器当天出现过的**不重复**玩家，**每天 0 点自动总结推送**，
  历史存档保留 60 天（`/mcsyn` 随时查看当天进度）
- 📋 `/mcsta`：查询服务器状态・在线玩家名单・订阅推送的会话数
- **订阅制**：`/mcntfy` 把当前群/私聊加入推送目标，事件主动推送到所有订阅会话
- **纯只读**：插件从不向服务器发送任何控制指令，安全无副作用

## 安装

1. 将 `astrbot_plugin_mcserver_monitor` 整个文件夹拷贝到 AstrBot 的
   `data/plugins/` 目录下（或打包后经 WebUI「插件管理 → 手动安装」导入）；
2. 在 WebUI 插件管理中**重启/重载该插件**；
3. 添加服务器（二选一）：
   - 首次启用：在插件**设置**页填写 `server_address`，保存后重载，自动导入为第一台服务器；
   - 群里直接发 `/mcadd 名称 地址`（可随时添加多条）；
4. 群里发 `/mcsta` 查看状态，发 `/mcntfy` 订阅推送。

## 配置项（插件设置页）

| 配置项 | 类型 | 默认 | 说明 |
| --- | --- | --- | --- |
| `server_address` | string | 空 | **首次启用一次性导入**为第一台服务器；之后请用 `/mcadd` 管理 |
| `server_port` | int | 25565 | `/mcadd` `/mcmod` 中地址未带端口时使用的默认端口 |
| `ping_timeout` | int | 5 | 单次探测超时（秒），建议 3~8 |
| `poll_interval` | int | 10 | 轮询间隔（秒），建议 5~30 |
| `notify_on_start` | bool | true | 开服通知开关 |
| `notify_on_stop` | bool | true | 关服通知开关 |
| `notify_on_player_change` | bool | true | 人数变动通知开关 |
| `notify_join_leave_details` | bool | true | 是否播报具体玩家名（否则只报人数） |
| `notify_daily_summary` | bool | true | 每日 0 点总结推送开关 |
| `debug` | bool | false | 详细日志 |

## 指令

| 指令 | 说明 | 权限 |
| --- | --- | --- |
| `/mcadd 名称 地址` | 快捷添加服务器（如 `/mcadd 生存服 play.example.com:25565`） | 全员 |
| `/mcmod 名称 地址` | 修改已添加服务器的地址 | 全员 |
| `/mcdel 名称` | 删除服务器（历史存档保留） | 全员 |
| `/mcsta` | 查询服务器状态・在线玩家名单・订阅推送的会话数 | 全员 |
| `/mcsyn` | 查看今日已统计的去重游玩人数 | 全员 |
| `/mcntfy` | 订阅当前会话为推送目标 | 全员 |
| `/mcuntfy` | 取消订阅当前会话 | 全员 |
| `/mcconfig` | 查看当前配置 | 全员 |

## 每日总结示例（每天 0 点自动推送）

```
📊 2026-09-03 服务器游玩总结：
「生存服」今日共有 12 位玩家：
• Alice
• Bob
• Charlie
……
「空岛服」今日共有 0 位玩家。
```

> 统计口径：当天任何一次探测出现在线名单中的玩家算 1 人，同一天重复出现不重复计数。
> 归档保留最近 60 天；插件重启、跨天重启都会补发总结，不会重复推送同一天。

## 工作原理

- 插件激活后在 AstrBot 事件循环中启动 **asyncio 轮询任务**，按 `poll_interval`
  对**每台**服务器发起 Minecraft Server List Ping：
  - 握手包：`0x00` + 协议版本 + 主机名 + 端口 + 下一状态=1；
  - 状态请求包：`0x00`；
  - 服务器返回状态 JSON（`players.online/max/sample`、`version.name`、`description`）。
- 探测结果与各服务器上一轮快照对比，检测 **离线→在线（开服）**、**在线→离线（关服）**、
  **在线玩家集合/人数变化（进服/退服）** 事件；
- 事件通过 `context.send_message(session, MessageChain)` **主动推送**到所有
  `/mcntfy` 订阅的会话（订阅、服务器列表、每日统计均持久化于 AstrBot 数据目录）。
- 插件启动后**第一轮探测只作为基线**，不推送，避免重载造成误报。
- 服务器无响应/拒绝连接/超时均视为**离线**，恢复响应即视为开服。

## 兼容性说明

- ✅ Paper / Spigot / Bukkit / Purpur / Forge / Fabric（Java 版）；
- ✅ BungeeCord / Velocity 等群组代理（探测到的是代理对外显示的状态）；
- ❌ 基岩版（Bedrock）使用 RakNet 协议，本插件不支持；
- ⚠️ 若服务器在配置中关闭了“服务器列表”查询（enable-status=off），
  玩家名单可能为空（人数仍可拿到，名单为空时自动降级为人数播报，每日统计无法记录玩家名）。

## 常见问题

- **`/mcsta` 显示“连接被拒绝”**：服务器没开、端口填错，或被防火墙/安全组拦截。
- **一直“连接超时”**：服务器屏蔽了陌生 IP 探测，或 ping_timeout 过小。
- **有在线人数但没玩家名单**：服务器关闭了列表查询或隐藏在线玩家；
  此时只播报人数变化，每日统计只能记人数级数据。
- **推送不生效**：确认目标会话已 `/mcntfy`（`/mcsta` 末尾会显示订阅推送的会话数），
  且平台适配器支持主动推送（QQ 官方 API 平台 `qq_official` 不支持
  `context.send_message`，请用 aiocqhttp 等）。
- **想改服务器地址**：`/mcmod 名称 新地址`（改完后自动清基线，不会误报开关服）。
