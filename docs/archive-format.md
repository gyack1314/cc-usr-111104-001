# 连接档案约定

`archiver` 把每个 TCP 连接收到的字节原样留存，只记录帧边界与拒收区间，不做定位解算。所有样本都是合成字节。

## 目录布局

```
archive.json               清单：{"version":1,"next_conn":N}，编号只增不复用
segments/conn-NNNNNN.raw   该连接收到的原始字节（O_EXCL 创建，只追加）
meta/conn-NNNNNN.json      连接日志：state=open/closed/interrupted、字节数、关闭原因
index/conn-NNNNNN.jsonl    帧/拒收记录 + commit 行（派生数据，可整体重建）
```

原始段是唯一事实来源；索引只是原始段的派生缓存，任何时候都可由
`python -m archiver.rebuild` 从原始段重建。

## 记录格式（index/*.jsonl，每行一条）

帧记录：`{"kind":"frame","conn":1,"seq":2,"start":5,"end":16,"payload_len":5,"type":1077,"crc":"eba8d9"}`

- `start`/`end` 是该连接原始字节流中的半开区间，`seq` 是本地接收顺序（连接内单调）。
- `type` 取载荷首十二位；载荷不足两字节时为 `null`（未知类型，仍保留）。

拒收记录：`{"kind":"reject",...,"reason":...}`，`reason` 取值：

- `noise`：扫描帧头时的非 0xD3 字节，按连续段记录；
- `bad-header`：保留位非零或长度超限，只拒收 1 字节，从下一字节重新扫描（`detail` 区分 `reserved-bits`/`length-over-limit`）；
- `bad-crc`：头部合法的候选帧校验不符，拒收整个候选区间，从候选末尾继续扫描（附 `crc_stored`/`crc_actual`）；
- `truncated`：字节流结束时候选帧未收完（`expected` 为应有总长度，头部不全时为 `null`）。

commit 行：`{"kind":"commit","conn":1,"parsed_upto":51,"records":6}`，每批落盘后追加，
表示索引已覆盖到原始段的第 `parsed_upto` 字节。

帧与拒收记录把每个连接的字节流从 0 开始连续平铺：每个字节恰好属于一条记录。
载荷中的 0xD3 不会被当成新帧；同一帧字节出现多次就保留多条记录。

## 一致性与崩溃恢复

写入顺序：先 fsync 原始段，再写索引，连接关闭日志最后写。因此：

- 崩溃于"字节落盘、索引更新"之间 → 索引过期（commit 的 `parsed_upto` 小于段大小），
  检查器报退出码 1，`rebuild` 从原始段重建后恢复；
- 崩溃于任何时候 → 日志停留 `open`，下次采集启动时标记为 `interrupted`；
  检查器报告"本地未完成"，退出码 2，不会把未完整写下的末尾报告成完整档案；
- 日志正常关闭（`peer-eof`）且结尾有半帧 → 来源断流；日志未关闭时的结尾半帧
  不能归因于来源。

连接未关闭时，结尾的半帧解释是临时的：更多字节到达（或从崩溃窗口恢复）后，
重建会把这段字节重新解释。只有关闭日志能冻结对结尾的解释。

## 连接编号与重连

每次连接单独编号，段文件互不追加。没有来源序号，因此不跨连接去重、不宣称
断线重连后无损衔接；检查器发现同一帧字节出现在多个连接时只提示。

## 上限与背压

- `--max-payload` / `--max-frame`：单帧上限，超限的候选按 `bad-header` 拒收；
- 解析器跨 `feed()` 留存的字节不超过 `max_frame`（统计在 `parser_retained_peak`）；
- `--queue-bytes`：读写线程间的缓存上限，写盘跟不上时读线程阻塞，TCP 接收窗口
  关闭后对端被自然限速，不会默默丢帧（统计在 `背压 N 次` 与队列峰值）。

## 检查器退出码

0=全部完整一致；1=一致性问题（索引过期/损坏、段与日志不符、文件缺失）；2=一致但存在未完成连接。

## 入口

```sh
python -m archiver.capture 127.0.0.1 8766 --archive out/archive --connections 2
python -m archiver.check --archive out/archive [--json]
python -m archiver.rebuild --archive out/archive
python -m archiver.demo            # 端到端演示（合成字节，含崩溃恢复）
```
