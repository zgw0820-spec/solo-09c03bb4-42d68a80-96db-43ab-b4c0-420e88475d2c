# MPI 计算集群回填调度模拟器（FastAPI）

模拟时钟驱动的 MPI 集群**回填（backfill）调度**服务。机架与节点为内存状态，
通过 HTTP API 提交作业、推进时钟、回报完工并查询预约与占用。

## 调度规则

1. **同机架分配**：提交作业时声明同一机架所需节点数与运行时限，作业不跨机架；
   超过最大机架容量的作业直接拒绝（422）。
2. **队首优先 + 最早开工时刻**：每次重排先看队首，跨所有机架求最早可凑齐节点的
   时刻（队首**可以换机架**）。需要等待时公布预约（机架、节点、最早开工时刻、
   预计释放时刻）。**已公布的开工时刻只会提前、不会推后**。
3. **回填**：队首等待期间，按提交顺序检查后续作业——当前机架有空位、跑满自己
   的时限（`now + duration <= 队首预约时刻`）且不占用预约节点，就放行；
   否则继续排队。
4. **释放与重排**：收到完工回报，或时钟推进到达运行时限，立即释放节点并重排；
   一次推进跨过多个释放时刻时，在**每个释放时刻**都执行一次重排。
5. **迟到回报安全**：节点释放带作业归属校验。作业已完工/已被杀掉后，迟到的
   完工回报返回 409，不会释放已经转配给其他作业的节点。
6. **并发安全**：所有写操作由一把异步锁串行化，并发提交不会把同一节点分给两个
   作业。

## 接续（MPI 后处理）规则

MPI 后处理需要读取前段留在节点本地的临时文件，因此后段复用前段的同一组节点：

1. **接续提交**：`POST /jobs/{id}/continuation` 指定**正在运行的普通作业**（前段）
   并填写后段时限；每个前段最多接一段，后段本身不能再挂接续。
2. **受理前评估**：按两段都跑满时限估算原节点的释放时刻
   （`前段开工 + 前段时限 + 后段时限`）；若队首**即使换机架**也赶不上已公布的
   预约时刻，拒绝接续（409），原作业与预约照旧。
3. **受理后**：前段仍在运行时，排程把这些节点的预计释放时刻计入后段时限
   （影响后续队首预约与回填判断）。
4. **交接**：前段的有效完工回报一经受理，后段在**当前模拟时刻**用这组节点开工
   （节点归属直接过户，前段记为完工）。
5. **前段超时**：达到时限被杀时，后段随之取消（`cancelled`），节点释放并重排。
6. 后段开工后就是普通运行作业：可正常回报完工，超时限同样被杀。

## 目录结构

```
app/
  config.py      # 机架拓扑配置（环境变量 CLUSTER_RACKS）
  models.py      # Pydantic 请求/响应模型
  scheduler.py   # 调度核心：预约、回填、超时堆、归属校验、接续交接
  main.py        # FastAPI 路由与异常映射
tests/           # pytest 单元 + API 测试（36 项）
scripts/
  smoke_test.py  # 仅用标准库的端到端 HTTP 冒烟脚本（36 项检查）
Dockerfile
docker-compose.yml
```

## 用 Docker Compose 启动

```bash
docker compose up --build
# 服务与 Swagger：
#   API 根路径    http://localhost:8000/
#   Swagger UI   http://localhost:8000/docs
#   OpenAPI      http://localhost:8000/openapi.json
```

机架拓扑通过环境变量 `CLUSTER_RACKS` 配置（compose 中默认 `4,4`，即两个机架、
每个 4 节点）。支持两种写法，可混用：

- `"4,4"`：机架 r1=4 节点、r2=4 节点；
- `"3x4,8"`：4 个各 3 节点的机架，再加一个 8 节点机架。

## 本地直接运行（无 Docker 时）

```bash
python3 -m venv .venv            # 如系统缺 ensurepip，可用 uv：uv venv .venv
.venv/bin/pip install -r requirements-dev.txt
.venv/bin/uvicorn app.main:app --reload --port 8000
.venv/bin/pytest -q              # 36 passed
.venv/bin/python scripts/smoke_test.py   # 需先启动服务，36 项检查
```

## API 一览（详见 Swagger）

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/jobs/submit` | 提交作业 `{nodes, duration}`，立即触发重排 |
| POST | `/jobs/{job_id}/continuation` | 提交接续后段 `{duration}`，复用前段同组节点；预约冲突则 409 |
| POST | `/jobs/{job_id}/completion` | 完工回报 `{at}`，`at` 不得晚于当前时钟、早于开工或超过时限；前段带后段时交接开工 |
| GET | `/jobs` / `/jobs/{id}` | 查询作业（预约、实际开工、完工时刻与原因、接续链） |
| GET | `/reservations` | 当前公布的预约（最早开工时刻与预计释放时刻） |
| POST | `/simulation/advance` | 推进模拟时钟 `{steps}`，跨多个释放时刻逐时刻重排 |
| GET | `/simulation/status` | 时钟、各机架节点占用、运行/排队/待开工后段、近期事件 |
| GET | `/racks/occupancy` | 各机架逐节点占用（节点号 → 作业 id） |
| GET | `/events` | 调度事件流（提交/预约/开工/完工/超时/接续受理与取消） |
| POST | `/simulation/reset` | 重置模拟环境 |

## 典型操作序列（Swagger 或 curl）

```bash
# 1) 占满两个机架（r1 4 节点、r2 先占 3 节点留 1 个空位）
curl -s -X POST localhost:8000/jobs/submit -H 'Content-Type: application/json' \
  -d '{"nodes":4,"duration":10}'
curl -s -X POST localhost:8000/jobs/submit -H 'Content-Type: application/json' \
  -d '{"nodes":3,"duration":10}'
# 2) 队首需要等待 -> 公布预约 t=10
curl -s -X POST localhost:8000/jobs/submit -H 'Content-Type: application/json' \
  -d '{"nodes":4,"duration":10}'
curl -s localhost:8000/reservations
# 3) 短作业回填空位（跑满 3 个时间单位，不耽误 t=10 的预约）
curl -s -X POST localhost:8000/jobs/submit -H 'Content-Type: application/json' \
  -d '{"nodes":1,"duration":3}'
# 4) 推进时钟；到达时限未回报完工的作业自动被杀并释放
curl -s -X POST localhost:8000/simulation/advance -H 'Content-Type: application/json' \
  -d '{"steps":6}'
# 5) 完工回报（at 必须 <= 当前时钟且未超时限）
curl -s -X POST localhost:8000/jobs/1/completion -H 'Content-Type: application/json' \
  -d '{"at":3}'
```

## 接续操作序列（Swagger 或 curl）

```bash
# 1) 前段占满 r1，r2 用长作业占满（让新队首只能等 r1）
curl -s -X POST localhost:8000/jobs/submit -H 'Content-Type: application/json' \
  -d '{"nodes":4,"duration":10}'        # 前段 job 1
curl -s -X POST localhost:8000/jobs/submit -H 'Content-Type: application/json' \
  -d '{"nodes":4,"duration":99}'
# 2) 给前段挂接续后段（时限 5）-> 201，后段锁定同组节点
curl -s -X POST localhost:8000/jobs/1/continuation -H 'Content-Type: application/json' \
  -d '{"duration":5}'
# 3) 新队首的预约按两段跑满估算：10 + 5 = 15
curl -s -X POST localhost:8000/jobs/submit -H 'Content-Type: application/json' \
  -d '{"nodes":4,"duration":4}'
curl -s localhost:8000/reservations
# 4) 推进到 t=3 并回报前段完工 -> 后段在 t=3 交接开工，节点占用过户
curl -s -X POST localhost:8000/simulation/advance -H 'Content-Type: application/json' \
  -d '{"steps":3}'
curl -s -X POST localhost:8000/jobs/1/completion -H 'Content-Type: application/json' \
  -d '{"at":3}'
# 5) 查询两段状态与节点占用
curl -s localhost:8000/jobs/1           # 前段 finished，continuation_id=3
curl -s localhost:8000/jobs/3           # 后段 running，actual_start=3，front_id=1
curl -s localhost:8000/racks/occupancy  # r1 节点归属已过户给后段
```

## 自测结果

- `pytest`：**36 passed**，覆盖回填准入/边界、预约节点保护、预约只提前与换机架、
  跨时刻逐点重排、时限杀作业、迟到/重复回报、错误码、并发不共享节点，
  以及接续的受理计入释放时刻、完工交接（含补报历史时刻在当前时刻开工）、
  前段超时取消后段、预约冲突拒绝、每前段一段、后段不可再接续、后段自身超时。
- `scripts/smoke_test.py`（真实 HTTP，uvicorn 进程）：**36 项检查全部通过**，
  含一次推进跨越 t=2/t=4/t=6 三个释放时刻的场景，以及接续的交接、超时取消、
  预约冲突拒绝三组端到端场景。
- 16 个并发提交压测：8 个作业运行、8 个排队，8 个节点归属互不重复。

## 已知限制 / 遗留事项

- 状态全部在内存，`/simulation/reset` 或进程重启即清空，未做持久化。
- 模拟时钟为整数单位；完工回报允许补报历史时刻（`at <= now`），不允许未来时刻。
- 回填判定为保守策略：严格不触碰队首预约节点，未做更激进的预约漂移类优化。
- 接续只支持"前段 + 一段后段"的两段链；后段开工时刻取受理完工回报的当前模拟
  时刻（补报历史完工时，后段仍从当前时刻起算）。
- 接续冲突评估只针对队首已公布的预约；更靠后的排队作业无预约保护，可能被
  接续推迟（与既有回填策略的语义一致）。
- 单进程实例；横向多实例部署需引入共享存储与分布式锁。
