"""FastAPI service: column-level dependency analysis for decommissioned source columns."""
from __future__ import annotations

from typing import Any, Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from .lineage import AggregatedAnalysisError, AnalysisError, Analyzer
from .missing import AggregatedMissingTraceError, Locator, MissingTraceError

app = FastAPI(
    title="配送数仓列依赖分析服务",
    description=(
        "POST /analyze：基于 SQLGlot 解析（不执行 SQL），沿别名、普通星号与非递归 CTE "
        "跨模型追踪待下线源列的引用，按模型输出依赖该列的输出列以及 WHERE / JOIN ON "
        "条件，每项附引用链。不支持聚合、集合运算与 CTE 之外的子查询。\n\n"
        "POST /locate-missing：漏单定位。接收待查运单号、同批源表样本（列名 + 类型，"
        "仅 INTEGER / VARCHAR）和按执行顺序给出的 DuckDB 模型 SQL，同批运单共用一次"
        "内存 DuckDB 只读试算，逐票返回源表及每次 JOIN、WHERE 之后的行数（重复匹配"
        "照计）：源表缺票单独标记，中途归零指出首次归零的模型与子句，最后仍有行则"
        "标为未漏单。模型只支持直接选列及改名（不去重）、与源维表的等值 INNER / "
        "LEFT JOIN、以及 WHERE 比较或判空；条件列须带表名或别名，中间模型可以不"
        "输出运单号。样本或 SQL 有误时说明原因，不给部分报告。"
    ),
    version="1.1.0",
)


class SourceTable(BaseModel):
    name: str = Field(..., description="源表名（大小写不敏感）")
    columns: list[str] = Field(..., description="源表当前全部列名")


class ModelSpec(BaseModel):
    name: str = Field(..., description="模型名称")
    sql: str = Field(..., description="该模型的 DuckDB SELECT 语句（可含非递归 CTE）")


class TargetColumn(BaseModel):
    table: str = Field(..., description="待下线列所在源表")
    column: str = Field(..., description="待下线的源列名（旧邮编列）")


class AnalyzeRequest(BaseModel):
    sources: list[SourceTable] = Field(..., description="源表及其列名")
    models: list[ModelSpec] = Field(..., description="各模型的名称和 DuckDB SELECT")
    target_columns: list[TargetColumn] = Field(
        ..., alias="targets", description="待下线的源列列表")

    model_config = {
        "populate_by_name": True,
        "json_schema_extra": {
            "examples": [
                {
                    "sources": [
                        {
                            "name": "waybills",
                            "columns": [
                                "waybill_id", "receiver_zip_old", "city_id", "weight_kg",
                            ],
                        },
                        {
                            "name": "dim_delivery_zone",
                            "columns": ["zone_id", "zip_code", "zone_name"],
                        },
                    ],
                    "models": [
                        {
                            "name": "stg_waybill",
                            "sql": (
                                "WITH cleaned AS (\n"
                                "  SELECT waybill_id, receiver_zip_old AS zip, city_id\n"
                                "  FROM waybills\n"
                                "  WHERE receiver_zip_old IS NOT NULL\n"
                                ")\n"
                                "SELECT waybill_id, zip, city_id\n"
                                "FROM cleaned"
                            ),
                        },
                        {
                            "name": "dwd_delivery_fact",
                            "sql": (
                                "SELECT s.waybill_id,\n"
                                "       s.zip AS receiver_zip,\n"
                                "       z.zone_name,\n"
                                "       s.weight_kg * 1000 AS weight_g\n"
                                "FROM stg_waybill s\n"
                                "JOIN dim_delivery_zone z ON s.zip = z.zip_code\n"
                                "WHERE s.zip LIKE '10%'"
                            ),
                        },
                    ],
                    "targets": [
                        {"table": "waybills", "column": "receiver_zip_old"}
                    ],
                }
            ]
        },
    }


class OutputDependency(BaseModel):
    output_column: str
    reference_chains: list[list[str]] = Field(
        ..., description="每条链自当前模型输出列起，到源表列止")


class ConditionDependency(BaseModel):
    type: str = Field(..., description="WHERE / JOIN ON / JOIN USING / QUALIFY")
    scope: str = Field(..., description="条件所在的模型或 CTE 作用域")
    expression: str
    column: str
    reference_chain: list[str]


class ModelResult(BaseModel):
    dependent_output_columns: list[OutputDependency]
    filter_conditions: list[ConditionDependency]


class AnalyzeResponse(BaseModel):
    targets: list[TargetColumn]
    models: dict[str, ModelResult]


class ErrorItem(BaseModel):
    model: Optional[str] = None
    location: Optional[str] = None
    message: str


class ErrorResponse(BaseModel):
    detail: list[ErrorItem]


@app.post(
    "/analyze",
    response_model=AnalyzeResponse,
    responses={400: {"model": ErrorResponse, "description": "解析、引用或不支持构造错误"}},
    summary="分析待下线源列在各模型中的依赖",
)
def analyze(request: AnalyzeRequest) -> AnalyzeResponse:
    source_names = [s.name for s in request.sources]
    dup_sources = {n for n in source_names if source_names.count(n) > 1}
    if dup_sources:
        raise HTTPException(400, [{"message": f"源表名重复: {sorted(dup_sources)}"}])
    model_names = [m.name for m in request.models]
    dup_models = {n for n in model_names if model_names.count(n) > 1}
    if dup_models:
        raise HTTPException(400, [{"message": f"模型名重复: {sorted(dup_models)}"}])
    if not request.target_columns:
        raise HTTPException(400, [{"message": "targets 不能为空"}])

    sources = {s.name: s.columns for s in request.sources}
    source_lookup = {
        name.lower(): {col.lower(): col for col in columns}
        for name, columns in sources.items()
    }
    target_tuples = [(t.table, t.column) for t in request.target_columns]
    unknown = [
        {"message": f"待下线列 {t.table}.{t.column} 在源表定义中不存在"}
        for t in request.target_columns
        if t.column.lower() not in source_lookup.get(t.table.lower(), {})
    ]
    if unknown:
        raise HTTPException(400, unknown)

    analyzer = Analyzer(
        sources=sources,
        models={m.name: m.sql for m in request.models},
        targets=target_tuples,
    )
    try:
        per_model = analyzer.run()
    except AggregatedAnalysisError as exc:
        raise HTTPException(400, [
            {"model": e.model, "location": e.location, "message": e.message}
            for e in exc.errors
        ])
    except AnalysisError as exc:
        raise HTTPException(400, [{
            "model": exc.model, "location": exc.location, "message": exc.message}])

    return AnalyzeResponse(
        targets=request.target_columns,
        models=per_model,
    )


# ---------------------------------------------------------------- 漏单定位

class SampleColumn(BaseModel):
    name: str = Field(..., description="列名（大小写不敏感）")
    type: str = Field(..., description="列类型，仅支持 INTEGER / VARCHAR（大小写不敏感）")


class SourceSample(BaseModel):
    name: str = Field(..., description="源表名（运单源表或源维表）")
    columns: list[SampleColumn] = Field(..., description="样本列名与类型")
    rows: list[list[Any]] = Field(
        default_factory=list,
        description="样本行，按 columns 顺序取值，null 表示 NULL")


class LocateMissingRequest(BaseModel):
    waybill_table: str = Field(..., description="运单源表名（首个模型的 FROM 必须是它）")
    waybill_column: str = Field(..., description="运单源表中的运单号列")
    waybill_ids: list[str | int] = Field(
        ..., description="待查运单号，同批共用一次内存 DuckDB 试算")
    sources: list[SourceSample] = Field(..., description="同批源表样本（运单源表 + 源维表）")
    models: list[ModelSpec] = Field(
        ..., description="按执行顺序给出的模型 SQL：从运单源表起，逐级读取前一模型")

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
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
                            "sql": (
                                "SELECT w.waybill_id,\n"
                                "       w.receiver_zip AS zip,\n"
                                "       w.city_id\n"
                                "FROM waybills w\n"
                                "WHERE w.receiver_zip IS NOT NULL"
                            ),
                        },
                        {
                            "name": "dwd_delivery_fact",
                            "sql": (
                                "SELECT s.zip,\n"
                                "       z.zone_name,\n"
                                "       s.city_id\n"
                                "FROM stg_waybill s\n"
                                "JOIN dim_delivery_zone z ON s.zip = z.zip_code"
                            ),
                        },
                        {
                            "name": "ads_delivery_daily",
                            "sql": (
                                "SELECT f.zone_name,\n"
                                "       f.city_id\n"
                                "FROM dwd_delivery_fact f\n"
                                "WHERE f.city_id >= 2"
                            ),
                        },
                    ],
                }
            ]
        },
    }


class MissingStep(BaseModel):
    model: str = Field(..., description="环节所在模型")
    clause: str = Field(..., description="环节类型：SOURCE（源表读取）/ JOIN / WHERE")
    detail: str = Field(..., description="归一化后的子句 SQL 片段")
    rows: int = Field(..., description="该环节之后该运单对应的行数（重复匹配照计）")


class FirstZero(BaseModel):
    model: str = Field(..., description="首次归零的模型")
    clause: str = Field(..., description="首次归零的子句类型：JOIN / WHERE")
    detail: str = Field(..., description="首次归零的子句（归一化 SQL）")


class WaybillReport(BaseModel):
    waybill_id: str | int = Field(..., description="待查运单号（按运单号列类型返回）")
    status: str = Field(
        ...,
        description="missing_in_source（源表缺票）/ dropped_in_pipeline（中途归零）"
                    "/ not_missing（未漏单）")
    final_rows: int = Field(..., description="最后一个模型输出中该运单的行数")
    first_zero: Optional[FirstZero] = Field(
        None, description="首次归零位置，仅 dropped_in_pipeline 时给出")
    steps: list[MissingStep] = Field(
        ..., description="源表及每次 JOIN、WHERE 之后的行数，按执行顺序")


class LocateMissingResponse(BaseModel):
    waybill_table: str
    waybill_column: str
    results: list[WaybillReport]


@app.post(
    "/locate-missing",
    response_model=LocateMissingResponse,
    responses={400: {"model": ErrorResponse, "description": "样本或 SQL 有误（说明原因，不给部分报告）"}},
    summary="漏单定位：逐票追踪运单在模型链各环节的行数",
)
def locate_missing(request: LocateMissingRequest) -> LocateMissingResponse:
    locator = Locator(
        waybill_table=request.waybill_table,
        waybill_column=request.waybill_column,
        waybill_ids=request.waybill_ids,
        sources=request.sources,
        models=request.models,
    )
    try:
        results = locator.run()
    except AggregatedMissingTraceError as exc:
        raise HTTPException(400, [
            {"model": e.model, "location": e.location, "message": e.message}
            for e in exc.errors
        ])
    except MissingTraceError as exc:
        raise HTTPException(400, [{
            "model": exc.model, "location": exc.location, "message": exc.message}])
    return LocateMissingResponse(
        waybill_table=request.waybill_table,
        waybill_column=request.waybill_column,
        results=results,
    )


@app.get("/healthz", include_in_schema=False)
def healthz() -> dict:
    return {"status": "ok"}
