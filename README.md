# 轨检观测副本

轨检资料中心用这套程序把白班巡检设备的具名读数复制到夜班离线工作站。白班保有可按位置复核的权威账，调度员指定工作站负责的线路段，夜班依据一份完整截面的明细、缺测清单和整数微米合计交班。使用 Python 3.11 标准库和两个独立的 SQLite 文件。`east` 表示东段，`west` 表示西段。

```text
python3 -B check_project.py
python3 -B -m unittest discover -s tests -t .
python3 -B demo.py
```

标准核验入口由控制台在物化项目时提供。演示和测试各自在临时目录建立两个库。演示显示交班明细和分段统计、乱序下的完整前缀、换订时的截面、迟到旧件的处理状态和重开后补领的回执。

## 一天的交班

1. 白班登记东段 E=5、F 缺测、西段 W=17，分别得到源位置 1、2、3。
2. 调度员给工作站 D 开通东段。位置 3 的初装件固定入队，接收提交前 D 报未就绪。
3. 夜班读到 E 和 F，东段有 2 条、1 条缺测、合计 5 微米。空读数在 CSV 中为空字段，有效零写为 `0`。
4. 白班将 E 更正为 7 并撤回 F。位置 5 只含 E=7，原位置 3 仍能读到 E=5 和 F 缺测。
5. 白班再将 E 更正为 6 和 5。位置 7 的件先到时仍可核对位置 5，位置 6 到齐后接收端连续推进至 7。
6. 夜班用请求号 R 改订西段，等待时仍读到东段 E=5。白班可以继续登记 W=19 和东段更正，投递者也可以暂扣已发行的件。
7. 西段交接生效时只读到切点 7 的 W=17。西段下一件把值更正为 19。迟到的东段件保持旧版本资格，即使位置更高也不能写入当前可见账。
8. 夜班重开接收库后，按 R 补领原切点回执。当前报告继续体现后续更正的位置。接收提交失败时仍读旧完整账，原请求保持待决，可续办。

## Python 接口

- `issuance.subscriptions.open_source(path)` 与 `replica.receive.open_replica(path)` 打开并初始化各自的连接。初始化可重复执行。调用者负责关闭连接，连接可用 `contextlib.closing` 管理。每个独立工作者使用自己的连接。
- `ledger.records.record(source, operation, observation_id, section=None, reading=None)` 接受 `register`、`correct`、`withdraw`。登记和更正须提供合法段号，撤回只传观测号。成功返回源位置，录入错误引发 `RecordRejected`。读数只接受 Python `int` 或 `None`。撤回后的观测号仍被占用。`ledger.records.record_work_order(source, operations)` 按同一规则整单受理一组操作，返回整段的源位置元组。`ledger.section_query.query_section_at(source, position, sections)` 返回该位置的有序源截面；越界位置及工单内部位置引发 `SourcePositionRejected`。
- `issuance.subscriptions.subscribe(source, subscription_id, replica_id, sections)` 返回已入队的初装件。订阅号和副本号在源库各自唯一。段集合去重并按段号排序，允许空集合。订阅冲突或非法参数引发 `SubscriptionRejected`。`issuance.increment.issue_next_increment(source, subscription_id)` 发行下一件固定增量（单个事实件或一张整工单），暂时无下一件时返回 `None`。`issue_available_increments` 返回这一轮发行的所有件。
- `delivery.resend.deliver(source, receiver, envelope_id, replica_id)` 投递原件，返回 `applied`、`waiting` 或 `retired`。`resend_pending` 按发行顺序续投，`hold` 可指定本轮暂扣的原件号。`deliver` 的 `lose_reply=True` 在接收提交后、源确认前引发 `ReplyLost`，用于确定性重现回包丢失。目标错配或同号异内容引发 `EnvelopeRejected`。数据库错误继续向调用者报告。
- `replica.scope_request.change_scope(receiver, subscription_id, request_id, sections)` 返回待决 `ScopeRequest` 或已提交的 `ScopeReceipt`。同参可续办，同号异参和同时开启第二笔不同请求引发 `ScopeRequestRejected`。`delivery.runner.forward_pending_scope_request` 将已提交的请求转达给源库。`advance_replication` 转达请求、发行可用增量并续投。各操作分别提交。可以单独调用这些步骤安排断线和重投。
- `audit.details.read(receiver, replica_id)` 返回 `AuditReport`。未安装时引发 `ReplicaNotReady`。行按段号和观测号排序，自带最近一次有效源事实位置。`audit.export.export_details(report)` 和 `export_totals(report)` 从同一个报告生成带位置的 CSV。每个已订段都提供记录数、缺测数和有效整数和，空段各项为零。`audit.receipts.receipt(receiver, subscription_id, request_id)` 返回原请求号及切点，未完成和不存在分别引发 `ReceiptPending` 和 `ReceiptNotFound`。

固定件使用确定性的 JSON 编码，观测行字段为 `observation_id`、`section`、`reading` 和 `source_position`。`reading` 在件中使用规范十进制文本或 JSON 空值，Python 接口和核账结果保留整数类型与精度。普通增量跨过范围外事件时只携带进度。固定件含订阅号、目标副本、范围版本、源位置、种类和确定的件号。重投沿用原件，接收端持久保存已提交终态。未确认的原件留在源库。换订后提前到的新范围件可继续续投，首次激活事务公布的截面精确对应回执切点。

## 勘误工单

`ledger.records.record_work_order(source, operations)` 把一组互相关联的勘误作为同一张工单受理。`operations` 为非空列表，每项是 `record` 的 2～4 个位置参数 `(operation, observation_id, section=None, reading=None)`（元组或列表均可）。按输入顺序沿用原校验，同一工单的后续操作能看到前序操作的结果。成功返回按输入顺序排列的源位置元组，整单占用下一段连续位置；空列表、非法操作参数或非法事实引发 `RecordRejected`；数据库错误沿原接口传播。整单在一个事务内提交，失败不留下事实或工单边界，下一次合法写入仍从原下一个位置开始。

`LedgerHistory.work_order_bounds(position)` 给出某位置所属工单的首末位置，`events_between` 仍按位置和原顺序读到每个实际事实（含撤回）。`query_section_at` 只接受核账边界：位置 0、独立 `record` 的位置和工单末位；工单内部位置（首位置到末位之前）及其他无效位置引发 `SourcePositionRejected`。工单内的同值更正同样推进相应事实与有效行的来源位置。旧库中的历史单笔继续各自形成截面。

发行端把每张工单作为一个固定件（`kind == 'work_order'`）发行：件号按首位置确定，载荷含首末位置和逐位置的步骤投影。每个步骤携带自己的 `source_position`；只有本订阅范围内的有效行才带正文，范围外事实只计位置，向外迁移可交付原范围中的删除，整单零可见变更时仍是覆盖全部位置的进度件。工单前序缺失、待切换范围或本范围未齐时接收端暂存并回报 `waiting`；只有前序连续且整单齐备才在一个接收事务内提交范围内行、工单末位和本单接纳终态，核账行来源仍为该行最后生效事实的原位置。

发行与接收的交接不变量：件号只由订阅、范围版本、种类和首位置决定，因此重开或丢回包后原件复投兑现已提交终态而不重复应用；载荷逐位置覆盖单元且每行自带源位置，接收端据此拒绝缺位、越界、越范围或同号异内容；范围内最终可见行与源在该单元的投影逐行相容。发行进度与固定内容同一事务提交（`cursor` 到达工单末位），因此重开后继续发行或按原件号重投都不会漏发或重发。换订切点必须是可核账位置，工单内部位置会被 `ScopeIssuanceRejected` 拒绝；迟到旧范围件不改变当前账。

回归覆盖初装和固定原件重投、同值多行、缺测与零、版本化源账、跨段更正、乱序、范围外位置、身份冲突、一致读回、换订接续、提交拒绝、回包丢失和重开，并新增工单的整单提交/回滚、内部位置拒绝、整单暂扣到齐、丢回包复投、提交拒绝续投、换订与旧库在途 V1 件联动。接收提交拒绝通过 SQLite 的事务授权器触发。项目范围为同一源账及独立副本，不涉及多源合流、同副本并行换订或源历史压缩。
