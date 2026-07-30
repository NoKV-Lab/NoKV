<p align="center">
  <img src="../public/img/community/nokv-lingtai-banner-zh.png" alt="NoKV × Lingtai — 设计共建伙伴合作" width="100%" />
</p>

# NoKV × Lingtai：design-partner 合作

发布于 2026 年 6 月 23 日；集成状态更新于 2026 年 7 月 30 日。

**NoKV** 与 **Lingtai**
（[Lingtai-AI/lingtai](https://github.com/Lingtai-AI/lingtai)）正以
design partner（设计共建伙伴）的关系，共同探索长时间运行 Agent 的持久化工作区。
本文记录合作关系和技术边界，不代表生产就绪声明。

## 两个项目，一个文件系统形态的工作区

- **Lingtai（灵台）** 是一个 local-first 的 Agent 运行时。长期存在的
  Agent 把状态、信箱、日志和工件保存在磁盘项目目录中，并且仍可使用普通文件
  工具直接检查。Lingtai 表示其已有一个活跃的早期开发者社区。
- **NoKV** 是面向对象存储、多 Agent 工作区的持久化元数据控制面。它提供文件
  系统形态的命名空间、shard-local 原子发布、带租约的历史快照，以及 CoW
  fork-to-restore 原语；规划、语义记忆和编排仍由 Agent 运行时负责。

Lingtai 为 Agent 提供文件系统形态的“家”。NoKV 提供存储和元数据原语，使这类
工作区在保留普通文件访问方式的同时，具备持久化、恢复和审计基础。

## 我们正在共同验证什么

- **工作区检查点与恢复：** 固定一个稳定的历史视图，把已提交工作区恢复到新的
  CoW 目标工作区，而不是原地修改源工作区。
- **shard-local 崩溃一致发布：** 在同一个 metadata owner 内原子发布单个工件，
  或一组 checkpoint 文件。
- **显式溯源：** 保存摘要以及运行时主动写入的 provenance 字段，使工件能够关联
  到产生它的运行元数据。
- **可查询的工作区元数据：** 查询运行时和应用显式记录的元数据。NoKV 不会自行
  推断语义依赖图。

NoKV 侧的 Workbench MCP 适配器、受保护的 Lingtai 18 工具契约、快照租约生命周期，
以及持久化恢复验收路径现已存在。具体 Lingtai 发行版是否可用，仍取决于发行版本、
能力探测和 preflight 检查。

## 当前边界

- 原始 Workbench profile 有 17 个基础工具；只有所有相关 owner 都确认能力后，
  才会把 `workbench_restore` 作为第 18 个工具暴露。
- 快照 pin 带租约。checkpoint 名称只是用于发现的别名，不是永久 GC root，也不会
  冻结实时工作区。
- fork-to-restore 仅支持 same-shard，并保持源工作区不变。NoKV 当前不提供跨 shard
  的原子恢复或发布事务。
- Workbench 路径约束不等于鉴权、RBAC 或租户策略。生产级身份边界、实时工作区冻结
  和 metadata 高可用仍需要单独完成工程硬化。

两个项目都仍处于 pre-1.0 阶段并在快速迭代。可复现的工作负载证据和下游可用状态
会在具备条件后另行发布。

## 联系方式

- NoKV：hello@nokv.io
- Lingtai：lingtai2026@gmail.com

## 加入社群

<img src="../public/img/community/lingtai-seal.svg" width="72" alt="LingTai seal" />

- Discord（NoKV）：https://discord.gg/c5PZapnwPh
- Slack（NoKV）：CNCF 社区 Slack 中的 NoKV 频道（先在 https://slack.cncf.io 加入，再进频道 https://cloud-native.slack.com/archives/C0BBDBYE3H6 ）
- 微信群（Lingtai）：请发送邮件到 `lingtai2026@gmail.com` 获取社群信息
