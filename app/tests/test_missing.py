"""Tests for the /locate-missing missing-waybill localization API."""
import copy

from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app)

# 维表一对多（100000/100001 各对应两个区），WB1001 到下游 ads 才被筛掉，
# WB1002 全程有行且重复匹配照计，WB1003 在 INNER JOIN 处掉单，WB9999 源表缺票。
SAMPLE = {
    "waybill_table": "waybills",
    "waybill_column": "waybill_id",
    "waybill_ids": ["WB1001", "WB1002", "WB1003", "WB9999"],
    "sources": [
        {
            "name": "waybills",
            "columns": [
                {"name": "waybill_id", "type": "VARCHAR"},
                {"name": "receiver_zip", "type": "VARCHAR"},
                {"name": "city_id", "type": "INTEGER"},
                {"name": "signed_status", "type": "VARCHAR"},
            ],
            "rows": [
                ["WB1001", "100000", 1, "已签收"],
                ["WB1002", "100001", 2, "已签收"],
                ["WB1003", "100002", 3, "已签收"],
            ],
        },
        {
            "name": "dim_delivery_zone",
            "columns": [
                {"name": "zip_code", "type": "VARCHAR"},
                {"name": "zone_name", "type": "VARCHAR"},
            ],
            "rows": [
                ["100000", "远郊A区"],
                ["100000", "远郊B区"],
                ["100001", "核心区"],
                ["100001", "核心区备站"],
            ],
        },
    ],
    "models": [
        {
            "name": "stg_waybill",
            "sql": ("SELECT w.waybill_id, w.receiver_zip AS zip, w.city_id "
                    "FROM waybills w WHERE w.receiver_zip IS NOT NULL"),
        },
        {
            "name": "dwd_delivery_fact",
            "sql": ("SELECT s.zip, z.zone_name, s.city_id "
                    "FROM stg_waybill s "
                    "JOIN dim_delivery_zone z ON s.zip = z.zip_code"),
        },
        {
            "name": "ads_delivery_daily",
            "sql": ("SELECT f.zone_name, f.city_id "
                    "FROM dwd_delivery_fact f WHERE f.city_id >= 2"),
        },
    ],
}


def locate(payload):
    return client.post("/locate-missing", json=payload)


def sample(**overrides):
    payload = copy.deepcopy(SAMPLE)
    payload.update(overrides)
    return payload


def reports_by_id(body):
    return {r["waybill_id"]: r for r in body["results"]}


def step_rows(report):
    return [(s["model"], s["clause"], s["rows"]) for s in report["steps"]]


# --------------------------------------------------------------- happy path

def test_end_to_end_all_statuses():
    resp = locate(SAMPLE)
    assert resp.status_code == 200, resp.text
    reports = reports_by_id(resp.json())

    wb1 = reports["WB1001"]
    assert wb1["status"] == "dropped_in_pipeline"
    assert wb1["first_zero"] == {
        "model": "ads_delivery_daily", "clause": "WHERE",
        "detail": "WHERE f.city_id >= 2",
    }
    assert wb1["final_rows"] == 0
    # 源表 1 行 -> stg WHERE 1 行 -> 维表一对多 JOIN 后 2 行 -> ads WHERE 归零
    assert step_rows(wb1) == [
        ("stg_waybill", "SOURCE", 1),
        ("stg_waybill", "WHERE", 1),
        ("dwd_delivery_fact", "JOIN", 2),
        ("ads_delivery_daily", "WHERE", 0),
    ]

    wb2 = reports["WB1002"]
    assert wb2["status"] == "not_missing"
    assert wb2["first_zero"] is None
    assert wb2["final_rows"] == 2  # 重复匹配照计

    wb3 = reports["WB1003"]
    assert wb3["status"] == "dropped_in_pipeline"
    assert wb3["first_zero"]["model"] == "dwd_delivery_fact"
    assert wb3["first_zero"]["clause"] == "JOIN"

    wb9 = reports["WB9999"]
    assert wb9["status"] == "missing_in_source"
    assert wb9["first_zero"] is None
    assert wb9["steps"][0]["clause"] == "SOURCE"
    assert wb9["steps"][0]["rows"] == 0


def test_intermediate_model_may_drop_waybill_column():
    # dwd_delivery_fact 与 ads_delivery_daily 都不输出运单号，仍能逐票追踪
    resp = locate(SAMPLE)
    assert resp.status_code == 200, resp.text
    assert {r["status"] for r in resp.json()["results"]} == {
        "dropped_in_pipeline", "not_missing", "missing_in_source"}


def test_left_join_no_match_keeps_row_then_where_drops():
    payload = sample(
        models=[
            {"name": "stg_waybill",
             "sql": "SELECT w.waybill_id, w.receiver_zip AS zip FROM waybills w"},
            {"name": "dwd_fact",
             "sql": ("SELECT s.waybill_id, z.zone_name FROM stg_waybill s "
                     "LEFT JOIN dim_delivery_zone z ON s.zip = z.zip_code")},
            {"name": "ads_daily",
             "sql": ("SELECT d.waybill_id FROM dwd_fact d "
                     "WHERE d.zone_name = '核心区'")},
        ],
        waybill_ids=["WB1003"],  # 邮编 100002 在维表中不存在
    )
    resp = locate(payload)
    assert resp.status_code == 200, resp.text
    (report,) = resp.json()["results"]
    assert report["status"] == "dropped_in_pipeline"
    # LEFT JOIN 未命中不掉行（仍为 1），WHERE 判空未过才归零
    assert step_rows(report) == [
        ("stg_waybill", "SOURCE", 1),
        ("dwd_fact", "JOIN", 1),
        ("ads_daily", "WHERE", 0),
    ]
    assert report["first_zero"]["clause"] == "WHERE"


def test_left_join_fanout_counts_duplicates():
    payload = sample(
        models=[
            {"name": "stg_waybill",
             "sql": "SELECT w.waybill_id, w.receiver_zip AS zip FROM waybills w"},
            {"name": "dwd_fact",
             "sql": ("SELECT s.waybill_id, z.zone_name FROM stg_waybill s "
                     "LEFT JOIN dim_delivery_zone z ON s.zip = z.zip_code")},
        ],
        waybill_ids=["WB1001"],
    )
    resp = locate(payload)
    (report,) = resp.json()["results"]
    assert report["status"] == "not_missing"
    assert report["final_rows"] == 2  # 100000 一对多命中两个区


def test_integer_waybill_ids_and_coercion():
    payload = {
        "waybill_table": "waybills",
        "waybill_column": "waybill_id",
        "waybill_ids": [101, "102", 999],  # 字符串形式的数字也可校正
        "sources": [
            {"name": "waybills",
             "columns": [{"name": "waybill_id", "type": "INTEGER"},
                         {"name": "city_id", "type": "INTEGER"}],
             "rows": [[101, 1], [102, 2]]},
        ],
        "models": [
            {"name": "stg",
             "sql": "SELECT w.waybill_id FROM waybills w WHERE w.city_id > 1"},
        ],
    }
    resp = locate(payload)
    assert resp.status_code == 200, resp.text
    reports = reports_by_id(resp.json())
    assert reports[101]["status"] == "dropped_in_pipeline"
    assert reports[101]["first_zero"]["clause"] == "WHERE"
    assert reports[102]["status"] == "not_missing"
    assert reports[999]["status"] == "missing_in_source"


def test_duplicate_source_rows_counted():
    payload = sample()
    payload["sources"][0]["rows"].append(["WB1001", "100000", 1, "已签收"])
    payload["waybill_ids"] = ["WB1001"]
    resp = locate(payload)
    (report,) = resp.json()["results"]
    assert report["steps"][0]["rows"] == 2  # 源表重复行照计
    # 2 行源表 -> JOIN 一对多 -> 4 行 -> WHERE city_id>=2 归零
    assert [s["rows"] for s in report["steps"]] == [2, 2, 4, 0]


def test_pure_projection_model_passes_counts_through():
    payload = sample(
        models=[
            {"name": "stg_waybill",
             "sql": "SELECT w.waybill_id, w.city_id FROM waybills w"},
            {"name": "proj_only",
             "sql": "SELECT s.waybill_id AS wb, s.city_id FROM stg_waybill s"},
            {"name": "ads_daily",
             "sql": "SELECT p.wb FROM proj_only p WHERE p.city_id = 2"},
        ],
        waybill_ids=["WB1001", "WB1002"],
    )
    resp = locate(payload)
    reports = reports_by_id(resp.json())
    assert reports["WB1001"]["status"] == "dropped_in_pipeline"
    assert reports["WB1001"]["first_zero"]["model"] == "ads_daily"
    assert reports["WB1002"]["status"] == "not_missing"
    # 纯投影模型不产生环节，但计数贯穿
    assert [s["model"] for s in reports["WB1002"]["steps"]] == [
        "stg_waybill", "ads_daily"]


def test_multiple_joins_steps_in_order():
    payload = sample(
        sources=SAMPLE["sources"] + [
            {"name": "dim_city",
             "columns": [{"name": "city_id", "type": "INTEGER"},
                         {"name": "zip_code", "type": "VARCHAR"},
                         {"name": "city_name", "type": "VARCHAR"}],
             "rows": [[1, "100000", "甲城"], [2, "100001", "乙城"]]},
        ],
        models=[
            {"name": "stg_waybill",
             "sql": ("SELECT w.waybill_id, w.receiver_zip AS zip, w.city_id "
                     "FROM waybills w")},
            {"name": "dwd_fact",
             "sql": ("SELECT s.waybill_id, z.zone_name, c.city_name "
                     "FROM stg_waybill s "
                     "JOIN dim_delivery_zone z ON s.zip = z.zip_code "
                     "LEFT JOIN dim_city c ON s.city_id = c.city_id "
                     "AND s.zip = c.zip_code")},
        ],
        waybill_ids=["WB1001", "WB1003"],
    )
    resp = locate(payload)
    assert resp.status_code == 200, resp.text
    reports = reports_by_id(resp.json())
    assert step_rows(reports["WB1001"]) == [
        ("stg_waybill", "SOURCE", 1),
        ("dwd_fact", "JOIN", 2),   # 一对多
        ("dwd_fact", "JOIN", 2),   # LEFT JOIN 命中，行数不变
    ]
    assert step_rows(reports["WB1003"])[1] == ("dwd_fact", "JOIN", 0)
    assert reports["WB1003"]["first_zero"]["clause"] == "JOIN"


def test_where_column_to_column_and_or():
    payload = sample(
        models=[
            {"name": "stg_waybill",
             "sql": ("SELECT w.waybill_id, w.receiver_zip AS zip, w.city_id "
                     "FROM waybills w "
                     "WHERE w.city_id = 1 OR w.receiver_zip = '100001'")},
            {"name": "dwd_fact",
             "sql": ("SELECT s.waybill_id FROM stg_waybill s "
                     "JOIN dim_delivery_zone z ON s.zip = z.zip_code "
                     "WHERE s.city_id = 1 AND s.zip = z.zip_code")},
        ],
        waybill_ids=["WB1001", "WB1002"],
    )
    resp = locate(payload)
    assert resp.status_code == 200, resp.text
    reports = reports_by_id(resp.json())
    assert reports["WB1001"]["status"] == "not_missing"
    assert reports["WB1002"]["status"] == "dropped_in_pipeline"


def test_nullable_sample_values():
    payload = sample()
    payload["sources"][0]["rows"][2][1] = None  # WB1003 邮编为 NULL
    payload["waybill_ids"] = ["WB1003"]
    resp = locate(payload)
    (report,) = resp.json()["results"]
    # IS NOT NULL 在首模型就把 NULL 邮编筛掉
    assert report["status"] == "dropped_in_pipeline"
    assert report["first_zero"] == {
        "model": "stg_waybill", "clause": "WHERE",
        "detail": "WHERE NOT w.receiver_zip IS NULL",
    }


# ------------------------------------------------------------- sample errors

def test_unsupported_column_type():
    payload = sample()
    payload["sources"][0]["columns"][1]["type"] = "TEXT"
    resp = locate(payload)
    assert resp.status_code == 400
    assert "INTEGER / VARCHAR" in resp.json()["detail"][0]["message"]
    assert "results" not in resp.json()


def test_row_length_mismatch():
    payload = sample()
    payload["sources"][0]["rows"][0] = ["WB1001", "100000"]
    resp = locate(payload)
    assert resp.status_code == 400
    assert "列数" in resp.json()["detail"][0]["message"]


def test_value_type_mismatch():
    payload = sample()
    payload["sources"][0]["rows"][0][2] = "一城"
    resp = locate(payload)
    assert resp.status_code == 400
    assert "INTEGER" in resp.json()["detail"][0]["message"]


def test_unknown_waybill_table_or_column():
    resp = locate(sample(waybill_table="nope"))
    assert resp.status_code == 400
    assert "运单源表" in resp.json()["detail"][0]["message"]
    resp = locate(sample(waybill_column="nope"))
    assert resp.status_code == 400
    assert "运单号列" in resp.json()["detail"][0]["message"]


def test_empty_waybill_ids_and_models():
    assert locate(sample(waybill_ids=[])).status_code == 400
    assert locate(sample(models=[])).status_code == 400


def test_waybill_id_type_mismatch():
    payload = sample(waybill_ids=["WB1001", 123])
    # 运单号列是 VARCHAR，数字会校正为字符串；再换成 INTEGER 列场景
    resp = locate(payload)
    assert resp.status_code == 200
    int_payload = {
        "waybill_table": "waybills", "waybill_column": "waybill_id",
        "waybill_ids": ["abc"],
        "sources": [{"name": "waybills",
                     "columns": [{"name": "waybill_id", "type": "INTEGER"}],
                     "rows": [[1]]}],
        "models": [{"name": "m", "sql": "SELECT w.waybill_id FROM waybills w"}],
    }
    resp = locate(int_payload)
    assert resp.status_code == 400
    assert "INTEGER" in resp.json()["detail"][0]["message"]


# ---------------------------------------------------------------- SQL errors

def _bad_model_payload(sql, name="m1", models_tail=None):
    models = [{"name": name, "sql": sql}] + (models_tail or [])
    return sample(models=models)


def test_first_model_must_read_waybill_source():
    payload = _bad_model_payload("SELECT z.zip_code FROM dim_delivery_zone z")
    resp = locate(payload)
    assert resp.status_code == 400
    assert "运单源表" in resp.json()["detail"][0]["message"]


def test_model_must_read_previous_model():
    payload = sample(models=[
        {"name": "m1", "sql": "SELECT w.waybill_id FROM waybills w"},
        {"name": "m2", "sql": "SELECT w.waybill_id FROM waybills w"},
    ])
    resp = locate(payload)
    assert resp.status_code == 400
    detail = resp.json()["detail"]
    assert detail[0]["model"] == "m2"
    assert "前一模型" in detail[0]["message"]


def test_join_target_must_be_dimension_source():
    payload = sample(models=[
        {"name": "m1", "sql": "SELECT w.waybill_id FROM waybills w"},
        {"name": "m2",
         "sql": ("SELECT a.waybill_id FROM m1 a "
                 "JOIN waybills b ON a.waybill_id = b.waybill_id")},
    ])
    resp = locate(payload)
    assert resp.status_code == 400
    assert "运单源表" in resp.json()["detail"][0]["message"]

    payload["models"][1]["sql"] = (
        "SELECT a.waybill_id FROM m1 a JOIN m1 b ON a.waybill_id = b.waybill_id")
    resp = locate(payload)
    assert resp.status_code == 400
    assert "模型" in resp.json()["detail"][0]["message"]

    payload["models"][1]["sql"] = (
        "SELECT a.waybill_id FROM m1 a JOIN nope b ON a.waybill_id = b.waybill_id")
    resp = locate(payload)
    assert resp.status_code == 400
    assert "源维表" in resp.json()["detail"][0]["message"]


def test_non_equi_and_unsupported_joins():
    base = "SELECT w.waybill_id FROM waybills w {join}"
    for join, keyword in [
        ("JOIN dim_delivery_zone z ON w.city_id > z.city_id", "等值"),
        ("CROSS JOIN dim_delivery_zone z", "CROSS"),
        ("NATURAL JOIN dim_delivery_zone z", "NATURAL"),
        ("RIGHT JOIN dim_delivery_zone z ON w.receiver_zip = z.zip_code", "RIGHT"),
        ("JOIN dim_delivery_zone z USING (zip_code)", "USING"),
        ("JOIN dim_delivery_zone z", "ON"),
        (", dim_delivery_zone z", "ON"),
    ]:
        resp = locate(_bad_model_payload(base.format(join=join)))
        assert resp.status_code == 400, join
        assert keyword in resp.json()["detail"][0]["message"], join


def test_join_on_must_reference_joined_table():
    payload = _bad_model_payload(
        "SELECT w.waybill_id FROM waybills w "
        "JOIN dim_delivery_zone z ON w.city_id = w.city_id")
    resp = locate(payload)
    assert resp.status_code == 400
    assert "同一关系" in resp.json()["detail"][0]["message"]


def test_unqualified_condition_column_rejected():
    payload = _bad_model_payload(
        "SELECT w.waybill_id FROM waybills w WHERE city_id = 1")
    resp = locate(payload)
    assert resp.status_code == 400
    assert "表名或别名" in resp.json()["detail"][0]["message"]

    payload = _bad_model_payload(
        "SELECT w.waybill_id FROM waybills w "
        "JOIN dim_delivery_zone z ON receiver_zip = z.zip_code")
    resp = locate(payload)
    assert resp.status_code == 400
    assert "表名或别名" in resp.json()["detail"][0]["message"]


def test_where_unsupported_constructs():
    base = "SELECT w.waybill_id FROM waybills w WHERE {cond}"
    for cond in [
        "w.receiver_zip LIKE '10%'",
        "w.city_id IN (1, 2)",
        "w.city_id BETWEEN 1 AND 5",
        "length(w.receiver_zip) > 3",
        "NOT (w.city_id = 1)",
        "w.city_id + 1 > 2",
        "w.receiver_zip = NULL",
        "1 = 1",
    ]:
        resp = locate(_bad_model_payload(base.format(cond=cond)))
        assert resp.status_code == 400, cond


def test_where_supported_constructs_pass():
    payload = _bad_model_payload(
        "SELECT w.waybill_id FROM waybills w "
        "WHERE (w.city_id >= 1 AND w.receiver_zip <> '100002') "
        "OR w.signed_status IS NULL OR w.receiver_zip IS NOT NULL")
    resp = locate(payload)
    assert resp.status_code == 200, resp.text


def test_select_shape_restrictions():
    for sql, keyword in [
        ("SELECT * FROM waybills w", "星号"),
        ("SELECT w.* FROM waybills w", "星号"),
        ("SELECT w.city_id + 1 AS c FROM waybills w", "直接选列"),
        ("SELECT DISTINCT w.waybill_id FROM waybills w", "DISTINCT"),
        ("SELECT w.waybill_id AS x, w.city_id AS x FROM waybills w", "重复"),
        ("SELECT w.waybill_id FROM waybills w ORDER BY w.waybill_id", "ORDER BY"),
        ("SELECT w.waybill_id FROM waybills w LIMIT 3", "LIMIT"),
        ("SELECT w.waybill_id, count(*) FROM waybills w GROUP BY w.waybill_id",
         "GROUP BY"),
        ("WITH c AS (SELECT w.waybill_id FROM waybills w) SELECT * FROM c", "CTE"),
        ("SELECT w.waybill_id FROM waybills w UNION SELECT w.waybill_id FROM waybills w",
         "SELECT"),
        ("SELECT w.waybill_id FROM (SELECT * FROM waybills) w", "FROM"),
    ]:
        resp = locate(_bad_model_payload(sql))
        assert resp.status_code == 400, sql
        assert keyword in resp.json()["detail"][0]["message"], sql


def test_unknown_column_and_ambiguity():
    resp = locate(_bad_model_payload(
        "SELECT w.nope FROM waybills w"))
    assert resp.status_code == 400
    assert "找不到列" in resp.json()["detail"][0]["message"]

    resp = locate(_bad_model_payload(
        "SELECT w.waybill_id FROM waybills w WHERE nope.col = 1"))
    assert resp.status_code == 400
    assert "不在当前查询" in resp.json()["detail"][0]["message"]

    # 维表与源表同名列，SELECT 未限定 -> 歧义
    payload = sample(
        sources=SAMPLE["sources"] + [
            {"name": "dim_city",
             "columns": [{"name": "city_id", "type": "INTEGER"}],
             "rows": [[1]]},
        ],
        models=[
            {"name": "m1",
             "sql": ("SELECT city_id FROM waybills w "
                     "JOIN dim_city c ON w.city_id = c.city_id")},
        ],
    )
    resp = locate(payload)
    assert resp.status_code == 400
    assert "歧义" in resp.json()["detail"][0]["message"]


def test_duplicate_model_name_and_source_clash():
    payload = sample(models=[
        {"name": "m1", "sql": "SELECT w.waybill_id FROM waybills w"},
        {"name": "m1", "sql": "SELECT a.waybill_id FROM m1 a"},
    ])
    resp = locate(payload)
    assert resp.status_code == 400
    assert "重复" in resp.json()["detail"][0]["message"]

    payload = sample(models=[
        {"name": "waybills", "sql": "SELECT w.waybill_id FROM waybills w"},
    ])
    resp = locate(payload)
    assert resp.status_code == 400
    assert "重名" in resp.json()["detail"][0]["message"]


def test_parse_error_and_multiple_statements():
    resp = locate(_bad_model_payload("SELECT FROM WHERE"))
    assert resp.status_code == 400
    assert "解析失败" in resp.json()["detail"][0]["message"]

    resp = locate(_bad_model_payload(
        "SELECT w.waybill_id FROM waybills w; SELECT 1"))
    assert resp.status_code == 400
    assert "一条 SELECT" in resp.json()["detail"][0]["message"]


def test_no_partial_report_on_error():
    # 第一个模型合法、第二个模型非法：整体 400，不给部分报告
    payload = sample(models=[
        {"name": "m1", "sql": "SELECT w.waybill_id FROM waybills w"},
        {"name": "m2", "sql": "SELECT a.waybill_id FROM m1 a WHERE city_id = 1"},
    ])
    resp = locate(payload)
    assert resp.status_code == 400
    body = resp.json()
    assert "results" not in body
    assert body["detail"][0]["model"] == "m2"


def test_errors_across_models_aggregated():
    payload = sample(models=[
        {"name": "m1", "sql": "SELECT w.nope FROM waybills w"},
        {"name": "m2", "sql": "SELECT a.waybill_id FROM m1 a WHERE city_id = 1"},
    ])
    resp = locate(payload)
    assert resp.status_code == 400
    models = {d["model"] for d in resp.json()["detail"]}
    assert models == {"m1", "m2"}


def test_swagger_example_and_analyze_kept():
    schema = client.get("/openapi.json").json()
    assert "/locate-missing" in schema["paths"]
    assert "/analyze" in schema["paths"]
    example = schema["components"]["schemas"]["LocateMissingRequest"]["examples"][0]
    resp = locate(example)
    assert resp.status_code == 200, resp.text
    reports = reports_by_id(resp.json())
    assert reports["WB1001"]["status"] == "dropped_in_pipeline"
    assert reports["WB1002"]["status"] == "not_missing"
    assert reports["WB9999"]["status"] == "missing_in_source"
