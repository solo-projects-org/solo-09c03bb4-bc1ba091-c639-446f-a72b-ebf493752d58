# 配送数仓列依赖分析服务

两个接口：

- `POST /analyze`：待下线源列的跨模型列依赖分析（只解析、不执行 SQL）；
- `POST /locate-missing`：配送日报漏单定位（内存 DuckDB 只读试算，逐票追踪行数）。

## POST /analyze：列依赖分析

运单源表即将停供旧邮编列。本接口接收源表定义、各模型的 DuckDB SELECT 与待下线源列，
**只用 SQLGlot 解析、不执行任何 SQL**，沿别名、普通星号与非递归 CTE 跨模型追踪列引用，
按模型输出：

- 依赖该源列的**输出列**，每项附完整引用链；
- 依赖该源列的 **WHERE / JOIN ON（含 USING）/ QUALIFY 条件**，每项附引用链。
  条件依赖**不计入**输出列。

## 追踪规则

- 别名：`receiver_zip_old AS zip` 会把依赖从 `zip` 继续追到源列。
- 普通星号：`SELECT *`、`SELECT t.*` 按关系实际输出列展开；支持
  `* EXCLUDE (...)`（DuckDB 里 `EXCEPT` 同义）与 `* REPLACE (...)`。
- 非递归 CTE：CTE 可引用同模型内更早的 CTE，作用域链以 `模型.CTE.列` 呈现。
- 跨模型：模型可以引用其他模型的输出列，分析按依赖拓扑顺序进行，链上保留模型跳。
- 名称匹配大小写不敏感，源表可带 catalog/schema 前缀（按表名匹配）。
- 引用链自当前模型的输出列/条件列起，到 `源表.源列` 止。

## 明确不支持（提交即 400 并指出所在模型）

- 聚合函数、`GROUP BY`、`HAVING`；
- 集合运算 `UNION / INTERSECT / EXCEPT`；
- CTE 之外的子查询（FROM 子查询、标量子查询、`EXISTS` 等）；
- 递归 CTE（`WITH RECURSIVE`）、`LATERAL`、`UNNEST`/表函数、`NATURAL JOIN`。

## 找不到或有歧义的引用（400，指出模型）

- 列在当前查询所有关系中都不存在；
- 未限定的列名可由多个关系提供（歧义，要求加表别名）；
- 表别名不存在、表/CTE 未定义、输出列重名等。

## 启动（Docker Compose）

```bash
docker compose up --build
# 服务: http://localhost:8000
# Swagger: http://localhost:8000/docs
# OpenAPI: http://localhost:8000/openapi.json
```

Swagger 的 `POST /analyze` 请求体自带"邮编改名后仍用于筛选"的样例，
`POST /locate-missing` 自带"维表一对多、运单到下游才被筛掉"的样例，均可直接 Try it out。

## 请求示例

`examples/request_zip_rename.json` 是完整样例（也可用 Swagger 内置样例）：

```bash
curl -X POST http://localhost:8000/analyze \
  -H 'Content-Type: application/json' \
  --data @examples/request_zip_rename.json
```

请求结构：

```json
{
  "sources": [
    {"name": "waybills", "columns": ["waybill_id", "receiver_zip_old", "city_id"]}
  ],
  "models": [
    {"name": "stg_waybill", "sql": "WITH cleaned AS (SELECT waybill_id, receiver_zip_old AS zip FROM waybills WHERE receiver_zip_old IS NOT NULL) SELECT * FROM cleaned"}
  ],
  "targets": [
    {"table": "waybills", "column": "receiver_zip_old"}
  ]
}
```

响应结构（按模型分组）：

```json
{
  "targets": [{"table": "waybills", "column": "receiver_zip_old"}],
  "models": {
    "stg_waybill": {
      "dependent_output_columns": [
        {
          "output_column": "zip",
          "reference_chains": [
            ["stg_waybill.zip", "stg_waybill.cleaned.zip", "waybills.receiver_zip_old"]
          ]
        }
      ],
      "filter_conditions": [
        {
          "type": "WHERE",
          "scope": "stg_waybill.cleaned",
          "expression": "receiver_zip_old IS NOT NULL",
          "column": "receiver_zip_old",
          "reference_chain": ["stg_waybill.cleaned.zip", "waybills.receiver_zip_old"]
        }
      ]
    }
  }
}
```

错误响应统一为 `400`，`detail` 是数组，每项含 `model`、`location`、`message`，
一次返回收集到的全部问题。

# 漏单定位 API（POST /locate-missing）

配送日报漏了已签收的运单时，值班员提交**待查运单号**、**同批源表样本**和
**按执行顺序给出的模型 SQL**，服务把同批运单放进**一次内存 DuckDB 试算**
（只读提交的样本、不落盘、每请求独立连接），逐票返回源表及每次 JOIN、
WHERE 之后的行数，据此判断该补源数据还是查连接和筛选。

## 输入规则

- `waybill_table` / `waybill_column`：运单源表名与运单号列；`waybill_ids` 为待查运单号。
- `sources`：同批源表样本（运单源表 + 源维表）。每表附 `columns`（列名 + 类型，
  **只用 INTEGER 和 VARCHAR**）和按列顺序取值的 `rows`（`null` 表示 NULL）。
- `models`：按执行顺序排列。首个模型的 `FROM` 必须是运单源表，之后每个模型
  逐级读取前一模型。模型 SQL 只支持：
  - 直接选列及改名（`列` 或 `列 AS 别名`，**不去重**，不支持星号与表达式）；
  - 与**源维表**做等值 `INNER JOIN` / `LEFT JOIN`（`ON 列 = 列`，可用 `AND` 连接）；
  - `WHERE` 比较（`= != <> < <= > >=`，列与常量或列与列）或判空（`IS NULL` /
    `IS NOT NULL`），可用 `AND` / `OR` 组合。
- 条件列（`JOIN ON` 与 `WHERE`）必须带表名或别名；中间模型可以不输出运单号
  （内部用携带列逐票追踪）。

## 输出（逐票）

- `status`：`missing_in_source`（源表缺票，单独标记）/ `dropped_in_pipeline`
  （中途归零）/ `not_missing`（最后仍有行，未漏单）。
- `steps`：源表（`SOURCE`）及每次 `JOIN`、每个 `WHERE` 之后的行数，按执行顺序；
  维表一对多等**重复匹配照计**（行数可变多）。
- `first_zero`：仅 `dropped_in_pipeline` 时给出首次归零的模型与子句。
- `final_rows`：最后一个模型输出中该运单的行数。

样本或 SQL 有误时返回 `400` 并说明原因（`detail` 数组，可含多条），**不给部分报告**。
`steps[].detail` 为 sqlglot 归一化后的 SQL 片段（如 `IS NOT NULL` 会显示为
`NOT ... IS NULL`，语义相同）。

```bash
curl -X POST http://localhost:8000/locate-missing \
  -H 'Content-Type: application/json' \
  --data @examples/request_missing_waybills.json
```

`examples/request_missing_waybills.json`（同 Swagger 内置样例）覆盖：维表一对多、
运单 WB1001 到下游 `ads_delivery_daily` 才被筛掉、WB1003 在 INNER JOIN 处掉单、
WB9999 源表缺票、WB1002 未漏单且重复匹配照计（`final_rows = 2`）。

## 本地开发（不用 Docker）

```bash
python -m venv --without-pip .venv   # 若系统带 ensurepip 可直接 python -m venv .venv
.venv/bin/pip install -r requirements.txt
PYTHONPATH=. .venv/bin/uvicorn app.main:app --reload
.venv/bin/pytest                       # 68 个测试
```

## 目录

```
app/
  main.py        FastAPI 路由、请求/响应模型、Swagger 样例与入参校验
  lineage.py     SQLGlot 解析与列级血缘分析（不执行 SQL）
  missing.py     漏单定位：内存 DuckDB 共享试算与逐票行数追踪
  tests/         分析器、漏单定位与 API 测试
examples/        邮编改名场景与漏单定位场景的完整请求样例
Dockerfile, docker-compose.yml, requirements.txt
```
