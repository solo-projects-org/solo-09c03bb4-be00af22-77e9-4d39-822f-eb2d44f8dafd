# 联合目录补采采集服务

联合目录管理员对单馆书目进行补采的 OAI-PMH 采集服务。核心保证：

- **翻页失败绝不让半批记录入库**；
- **同一个 SQLite 库同一时刻只允许一笔补采**：重叠请求立即返回 409，不请求 OAI-PMH 源、也不新增运行记录。

## 需求与实现对照

| 需求 | 实现 |
| --- | --- |
| FastAPI 接收截止日，首次另填起始日 | `POST /api/v1/harvest`，请求体 `until` 必填、`from` 选填 |
| 之后从上次成功采集的截止日接着采，起止日都包含 | `from` 留空时取 `harvest_progress.last_until` 作为起始日（含当天） |
| 同一 SQLite 库从读截止日到整批提交/回滚只允许一笔补采（跨进程） | 采集开始即在专用连接上 `BEGIN IMMEDIATE` 取得库级写锁（`busy_timeout=0`），锁事务覆盖“读 `last_until` → 翻页 → 提交/回滚”全过程；进程内 `threading` 锁已移除，两实例/多线程同一把锁 |
| 重叠请求立即 409，不触 OAI 源、不留运行记录 | 取锁失败立即抛 `HarvestConflictError` → HTTP 409；取锁发生在读取截止日、`start_run`、任何 OAI 请求之前 |
| 补采未提交时目录与进度仍可查询，只返回已提交数据 | WAL 模式下读不阻塞写、写不阻塞读；运行记录也随锁事务最后才提交，未提交的 `running` 行对外不可见 |
| 采集进程异常退出后无需人工解锁即可续采 | 锁即 SQLite 事务，进程死亡后 SQLite 自动回滚未提交事务并释放锁；重启后从最后成功的 `last_until` 续采，不存在独立锁文件 |
| OAI-PMH ListRecords 按日期拉取 oai_dc | 首请求 `verb=ListRecords&metadataPrefix=oai_dc&from=...&until=...` |
| 续页用 resumptionToken 替代日期和格式参数 | 续页请求只带 `verb` + `resumptionToken`；样例源对混用参数返回 `badArgument` |
| 令牌失效丢弃已取页，按原区间重抓一次 | 捕获 `badResumptionToken` 后清空已取页，用原始 from/until 重新拉取；只重抓一次 |
| 按标识更新 SQLite 书目 | `bib_records.identifier` 主键 upsert，重复采集幂等 |
| 遇删除标记就撤下 | header `status=deleted` 的记录置为 `withdrawn`（库中不存在则写墓碑） |
| 整批采完才提交书目变更和截止日 | 全部页取完才在锁事务内写库并提交；异常整体回滚 |
| 无匹配也推进截止日 | `noRecordsMatch` 不写书目，同事务推进 `last_until` |
| Docker Compose 启动共用 SQLite 的两个采集实例与可放慢翻页的样例源 | `docker compose up --build` 启动 `harvester-api-a`(8000) + `harvester-api-b`(8001) + `oai-source`(8080)，两个实例挂载同一命名卷 |
| Swagger 发起采集并查询目录和进度 | http://localhost:8000/docs 与 http://localhost:8001/docs |

## 目录结构

```
app/                  FastAPI 采集服务
  main.py             路由（采集/目录/进度/运行历史/健康检查，409 冲突映射）
  harvester.py        采集编排：跨进程采集锁、翻页、令牌失效重抓、整批事务
  oai_client.py       OAI-PMH 2.0 客户端（httpx + xml.etree）
  db.py               SQLite 表结构、跨进程锁（BEGIN IMMEDIATE）与事务操作
  schemas.py          Pydantic 请求/响应模型
  Dockerfile
sample-oai/           本地分页样例 OAI-PMH 源（纯标准库）
  server.py           27 条样例记录（rec-013 为删除记录），PAGE_SIZE=10，3 页；
                      支持 PAGE_DELAY 与 /admin/slow 放慢翻页、请求计数
  Dockerfile
tests/                pytest 端到端测试（16 个用例，含跨进程锁/崩溃恢复）
  _harvest_worker.py  跨进程测试用的子进程工作脚本（hold/harvest/crash）
scripts/              演示与辅助脚本
  demo_overlap.py     两实例重叠请求收到 409 后续采的完整演示
  demo_crash_recovery.py  进程崩溃后免解锁续采演示（Docker / 本地两种模式）
  test_rollback.py    令牌持续失效时整批回滚演示
docker-compose.yml    两个共用 SQLite 的采集实例 + 可放慢翻页的样例源
```

## 快速开始（Docker Compose）

```bash
docker compose up --build
```

- 采集实例 A Swagger：http://localhost:8000/docs
- 采集实例 B Swagger：http://localhost:8001/docs（与 A 挂载同一个命名卷、同一个 SQLite 文件）
- 样例 OAI 源：http://localhost:8080/oai?verb=Identify
- SQLite 数据保存在命名卷 `harvester-data`（两个容器内均为 `/data/harvester.db`）
- 样例源默认 `PAGE_DELAY=2` 秒/页，全区间 3 页约持锁 6 秒，便于制造重叠窗口

### 重叠请求 409 与续采演示（Swagger 操作步骤）

1. 打开实例 A 的 Swagger（http://localhost:8000/docs），对 `POST /api/v1/harvest`
   填 `{"from":"2026-10-01","until":"2026-10-27"}` 并 Execute（慢翻页期间持锁）。
2. 立刻在实例 B 的 Swagger（http://localhost:8001/docs）对同一路由填
   `{"from":"2026-10-01","until":"2026-10-20"}` Execute —— **立即返回 409**：

   ```json
   {
     "detail": {
       "message": "另一个采集实例正在补采同一目录，请稍后再试",
       "hint": "请等待当前补采提交后，用相同的截止日重新发起即可续采"
     }
   }
   ```

   该请求不会出现在 `/api/v1/runs`，样例源 `/admin/state` 的
   `list_records_requests` 也不增加。
3. 等实例 A 返回 200 后，在实例 B 只填截止日 `{"until":"2026-10-27"}` 重新发起，
   即从 `last_until`（含当天）续采成功。

也可一键脚本化演示（标准库，无需额外依赖）：

```bash
python3 scripts/demo_overlap.py
```

### 崩溃后免人工解锁续采演示

采集锁是 SQLite 事务而非独立锁文件：持锁进程被 kill 或容器重启后，SQLite
在下次打开库时自动回滚未提交事务并释放锁。

```bash
# Docker Compose 环境：kill 实例 A 后由实例 B 续采
python3 scripts/demo_crash_recovery.py

# 无 Docker 的本地进程模式（用 tests/_harvest_worker.py 模拟 os._exit）
LOCAL=1 APP_DB_PATH=/tmp/crash-demo/harvester.db \
  OAI_ENDPOINT=http://127.0.0.1:8080/oai OAI_ADMIN=http://127.0.0.1:8080 \
  python3 scripts/demo_crash_recovery.py
```

## 操作流程（Swagger 或 curl）

### 1. 首次采集：另填起始日，起止日都包含

```bash
curl -X POST http://localhost:8000/api/v1/harvest \
  -H 'Content-Type: application/json' \
  -d '{"from": "2026-10-01", "until": "2026-10-15"}'
```

响应：

```json
{
  "status": "success",
  "from_date": "2026-10-01",
  "until_date": "2026-10-15",
  "pages": 2, "fetched": 15,
  "inserted": 14, "updated": 0, "withdrawn": 1,
  "retries": 0, "run_id": 1
}
```

### 2. 后续采集：只填截止日，自动从上次截止日（含）续采

```bash
curl -X POST http://localhost:8000/api/v1/harvest \
  -H 'Content-Type: application/json' \
  -d '{"until": "2026-10-27"}'
# 实际区间：2026-10-15 ~ 2026-10-27（last_until 含当天，避免边界漏采）
```

### 3. 查询目录与进度

```bash
curl 'http://localhost:8000/api/v1/catalog?status=active'   # 在架（默认）
curl 'http://localhost:8000/api/v1/catalog?status=all'      # 含已撤下
curl 'http://localhost:8000/api/v1/catalog/oai:sample.lib:rec-013'
curl 'http://localhost:8000/api/v1/progress'                # 上次成功截止日
curl 'http://localhost:8000/api/v1/runs'                    # 采集运行历史
```

### 4. 无匹配也推进截止日

对没有任何记录的区间发起采集，返回 `fetched=0` 且 `last_until` 照常推进。

## 令牌失效重抓的验证方法

样例源提供故障注入端点：

```bash
# 令下一次 resumptionToken 请求返回 badResumptionToken
curl -X POST http://localhost:8080/admin/fail-next
curl http://localhost:8080/admin/state

# 全区间 27 条共 3 页：第 1 页取到后续页令牌失效 -> 丢弃该页 -> 原区间重抓成功
curl -X POST http://localhost:8000/api/v1/harvest \
  -H 'Content-Type: application/json' \
  -d '{"from": "2026-10-01", "until": "2026-10-27"}'
# 响应中 retries=1；运行历史与服务日志可查
curl -X POST http://localhost:8080/admin/reset   # 清除故障开关
```

若重抓过程中令牌**再次**失效（不可恢复），接口返回 `502`，本批所有书目变更回滚、
截止日保持不变，`harvest_runs` 中记录一条 `failed` 历史——任何半批数据都不会进库。

## 事务、跨进程锁与一致性说明

- 采集启动即在专用连接上 `BEGIN IMMEDIATE`（`busy_timeout=0`）取得 SQLite 库级写锁：
  同一 SQLite 库从读取 `last_until` 到整批提交/回滚，只允许一笔补采；
- 重叠请求取锁立即失败 → HTTP 409，发生在读取截止日、写入运行记录、请求 OAI 源之前，
  因此不触源、不留痕；
- 采集过程先在内存中收集全部页面，**不向 SQLite 提交任何数据**；运行记录也随锁事务
  最后一起提交，未提交的 `running` 行对其他连接不可见；
- 全部页成功后，在锁事务内完成 upsert、撤下、截止日推进、运行历史更新并一次 commit；
  事务中任何一步失败都会 rollback，库状态回到采集前（失败历史在锁释放后的独立事务补记）；
- WAL 模式下读不阻塞写：补采持锁期间，目录与进度查询照常返回**已提交**的数据；
- 进程异常退出（含 `kill -9`、容器重启）时 SQLite 自动回滚未提交事务并释放锁，
  无需人工解锁，重启后从最后成功的 `last_until` 续采；
- 锁不区分请求来自哪个实例或线程：两个容器、同进程多线程共用同一把库级锁。

## 本地开发（无 Docker）

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt -r requirements-dev.txt

# 终端 1：样例 OAI 源
python3 sample-oai/server.py

# 终端 2：API 实例 A
APP_DB_PATH=./data/harvester.db OAI_ENDPOINT=http://127.0.0.1:8080/oai \
  uvicorn app.main:app --port 8000

# 终端 3：API 实例 B（同一个 SQLite 文件，模拟 Compose 的双实例）
APP_DB_PATH=./data/harvester.db OAI_ENDPOINT=http://127.0.0.1:8080/oai \
  uvicorn app.main:app --port 8001

# 测试（含跨进程锁与崩溃恢复用例）
pytest
```

## 环境变量

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `OAI_ENDPOINT` | `http://oai-source:8080/oai` | OAI-PMH 端点 |
| `APP_DB_PATH` | `/data/harvester.db` | SQLite 数据库路径（两个实例必须指向同一文件） |
| `PAGE_SIZE`（样例源） | `10` | 样例源每页记录数 |
| `PAGE_DELAY`（样例源） | `0` | 每次 ListRecords 响应前固定睡眠秒数；Compose 默认 2，可用 `POST /admin/slow?seconds=N` 动态调整，0 关闭 |
| `TOKEN_TTL`（样例源） | `3600` | 样例源 resumptionToken 有效期秒数 |
