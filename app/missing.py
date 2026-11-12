"""漏单定位：同批运单共用一次内存 DuckDB 试算，逐票追踪行数变化。

配送日报漏掉已签收运单后，值班员提交待查运单号、同批源表样本（列名 +
类型，仅 INTEGER / VARCHAR）和按执行顺序排列的模型 SQL。本模块在内存
DuckDB 中只读试算该模型链：模型从运单源表起、逐级读取前一模型，只支持
直接选列及改名（不去重）、与源维表的等值 INNER / LEFT JOIN、以及 WHERE
比较或判空（条件列必须带表名或别名）。

逐票返回源表及每次 JOIN、WHERE 之后的行数（维表一对多等重复匹配照计）：
源表缺票单独标记为 missing_in_source；中途归零标记为 dropped_in_pipeline
并指出首次归零的模型与子句；最后仍有行标记为 not_missing。样本或 SQL 有
误时汇总报错，不给部分报告。

实现要点：每个模型的 SQL 会被追加一个内部携带列 ``__missing_trace_carry__``
（首模型取运单号列，后续模型取自前一模型的同名携带列），据此在源表读取、
每次 JOIN、每个 WHERE 之后用一条 GROUP BY 查询为同批全部运单计数，因此
中间模型即使不输出运单号也能逐票追踪，且整批只共享一次内存试算。
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Optional

import duckdb
import sqlglot
from sqlglot import exp
from sqlglot.errors import ParseError, TokenError

DIALECT = "duckdb"
CARRY_COL = "__missing_trace_carry__"
SUPPORTED_TYPES = ("INTEGER", "VARCHAR")
_INT_RE = re.compile(r"[+-]?\d+")
COMPARE_OPS = (exp.EQ, exp.NEQ, exp.LT, exp.LTE, exp.GT, exp.GTE)

# 语句级不允许出现的子句 -> 中文说明
_FORBIDDEN_CLAUSES = (
    ("with_", "CTE（WITH）"),
    ("distinct", "DISTINCT（要求不去重）"),
    ("group", "GROUP BY"),
    ("having", "HAVING"),
    ("qualify", "QUALIFY"),
    ("order", "ORDER BY"),
    ("limit", "LIMIT"),
    ("offset", "OFFSET"),
)


class MissingTraceError(Exception):
    """单条漏单定位问题（样本或 SQL 有误）。"""

    def __init__(self, message: str, model: Optional[str] = None,
                 location: Optional[str] = None):
        self.message = message
        self.model = model
        self.location = location
        prefix = f"[{model}] " if model else ""
        loc = f"{location}: " if location else ""
        super().__init__(f"{prefix}{loc}{message}")


class AggregatedMissingTraceError(Exception):
    """一次返回收集到的全部问题。"""

    def __init__(self, errors: list[MissingTraceError]):
        self.errors = errors
        super().__init__("; ".join(str(e) for e in errors))


def _norm(value: Optional[str]) -> str:
    return (value or "").lower()


def _qi(name: str) -> str:
    """Quote an identifier for DuckDB DDL."""
    return '"' + name.replace('"', '""') + '"'


def _unwrap(node: exp.Expression) -> exp.Expression:
    while isinstance(node, exp.Paren):
        node = node.this
    return node


def _flatten_and(node: exp.Expression):
    node = _unwrap(node)
    if isinstance(node, exp.And):
        yield from _flatten_and(node.left)
        yield from _flatten_and(node.right)
    else:
        yield node


def _coerce_value(value: Any, ctype: str) -> tuple[Any, Optional[str]]:
    """样本值按列类型校正；返回 (校正后的值, 错误消息或 None)。"""
    if value is None:
        return None, None
    if ctype == "INTEGER":
        if isinstance(value, bool):
            return None, "应为 INTEGER，得到布尔值"
        if isinstance(value, int):
            return value, None
        if isinstance(value, str) and _INT_RE.fullmatch(value.strip()):
            return int(value.strip()), None
        return None, f"应为 INTEGER，得到 {value!r}"
    # VARCHAR
    if isinstance(value, str):
        return value, None
    if isinstance(value, bool):
        return None, "应为 VARCHAR，得到布尔值"
    if isinstance(value, (int, float)):
        return str(value), None
    return None, f"应为 VARCHAR，得到 {value!r}"


@dataclass
class SampleTable:
    name: str                            # 原始表名
    columns: list[tuple[str, str]]       # (原始列名, 规范化类型)
    rows: list[list[Any]]                # 已按类型校正的样本行
    col_keys: dict[str, str]             # 小写列名 -> 原始列名


@dataclass
class Relation:
    binding: str                         # 小写别名或表名
    display: str                         # 原始写法（报错用）
    columns: dict[str, str]              # 小写列名 -> 原始列名


@dataclass
class Step:
    model: str
    clause: str                          # SOURCE / JOIN / WHERE
    detail: str                          # 归一化后的 SQL 片段
    sql: str                             # 按运单分组计数的查询


@dataclass
class ModelPlan:
    name: str
    augmented_sql: str                   # 追加携带列后的模型 SQL
    steps: list[Step] = field(default_factory=list)


class Locator:
    """校验样本与模型链，在内存 DuckDB 中试算并生成逐票报告。"""

    def __init__(self, waybill_table: str, waybill_column: str,
                 waybill_ids: list, sources: list, models: list):
        self.waybill_table = waybill_table
        self.waybill_column = waybill_column
        self.raw_ids = list(waybill_ids)
        self.raw_sources = list(sources)
        self.raw_models = list(models)
        self.errors: list[MissingTraceError] = []
        self.tables: dict[str, SampleTable] = {}
        self.model_keys: set[str] = set()
        self.id_column: Optional[str] = None
        self.id_type: Optional[str] = None
        self.ids: list = []                       # 校正后的运单号，与 raw_ids 对齐
        self.plans: list[ModelPlan] = []

    # ------------------------------------------------------------- 样本校验
    def _validate_samples(self) -> None:
        if not self.raw_sources:
            self.errors.append(MissingTraceError("sources 不能为空"))
            return
        for src in self.raw_sources:
            key = _norm(src.name)
            if not key:
                self.errors.append(MissingTraceError("源表名不能为空"))
                continue
            if key in self.tables:
                self.errors.append(MissingTraceError(f"源表名重复: {src.name}"))
                continue
            if not src.columns:
                self.errors.append(MissingTraceError(f"源表 {src.name} 至少需要一列"))
                continue
            col_keys: dict[str, str] = {}
            columns: list[tuple[str, str]] = []
            bad = False
            for col in src.columns:
                ck = _norm(col.name)
                if not ck:
                    self.errors.append(
                        MissingTraceError(f"源表 {src.name} 存在空列名"))
                    bad = True
                    continue
                if ck in col_keys:
                    self.errors.append(
                        MissingTraceError(f"源表 {src.name} 列名重复: {col.name}"))
                    bad = True
                    continue
                ctype = (col.type or "").strip().upper()
                if ctype not in SUPPORTED_TYPES:
                    self.errors.append(MissingTraceError(
                        f"源表 {src.name} 列 {col.name} 类型 {col.type!r} 不支持，"
                        "仅支持 INTEGER / VARCHAR"))
                    bad = True
                    continue
                if ck == CARRY_COL:
                    self.errors.append(MissingTraceError(
                        f"源表 {src.name} 列 {col.name} 与内部保留列名冲突"))
                    bad = True
                    continue
                col_keys[ck] = col.name
                columns.append((col.name, ctype))
            if bad:
                continue
            rows: list[list[Any]] = []
            for ridx, row in enumerate(src.rows or []):
                if len(row) != len(columns):
                    self.errors.append(MissingTraceError(
                        f"源表 {src.name} 第 {ridx + 1} 行列数为 {len(row)}，"
                        f"应为 {len(columns)}"))
                    bad = True
                    continue
                coerced: list[Any] = []
                for (cname, ctype), value in zip(columns, row):
                    v, msg = _coerce_value(value, ctype)
                    if msg:
                        self.errors.append(MissingTraceError(
                            f"源表 {src.name} 第 {ridx + 1} 行 列 {cname}: {msg}"))
                        bad = True
                    coerced.append(v)
                rows.append(coerced)
            if bad:
                continue
            self.tables[key] = SampleTable(
                name=src.name, columns=columns, rows=rows, col_keys=col_keys)
        wkey = _norm(self.waybill_table)
        table = self.tables.get(wkey)
        if table is None:
            self.errors.append(
                MissingTraceError(f"运单源表 {self.waybill_table} 不在 sources 中"))
            return
        id_original = table.col_keys.get(_norm(self.waybill_column))
        if id_original is None:
            self.errors.append(MissingTraceError(
                f"运单号列 {self.waybill_column} 不在运单源表 {table.name} 的列中"))
            return
        self.id_column = id_original
        self.id_type = next(t for n, t in table.columns if n == id_original)

    # ----------------------------------------------------------- 运单号校验
    def _validate_ids(self) -> None:
        if not self.raw_ids:
            self.errors.append(MissingTraceError("waybill_ids 不能为空"))
            return
        if self.id_type is None:
            return  # 运单源表校验已报错，不再级联
        for raw in self.raw_ids:
            v, msg = _coerce_value(raw, self.id_type)
            if msg or v is None:
                self.errors.append(MissingTraceError(
                    f"待查运单号 {raw!r} 与运单号列类型 {self.id_type} 不符"))
                self.ids.append(None)
            else:
                self.ids.append(v)

    # ------------------------------------------------------------- 模型校验
    def _validate_models(self) -> None:
        if not self.raw_models:
            self.errors.append(MissingTraceError("models 不能为空"))
            return
        seen: dict[str, str] = {}
        for spec in self.raw_models:
            key = _norm(spec.name)
            if not key:
                self.errors.append(MissingTraceError("模型名不能为空"))
            elif key in seen:
                self.errors.append(
                    MissingTraceError(f"模型名重复: {spec.name}", model=spec.name))
            elif key in self.tables:
                self.errors.append(MissingTraceError(
                    f"模型名 {spec.name} 与源表重名", model=spec.name))
            else:
                seen[key] = spec.name
        self.model_keys = set(seen)

        waybill_key = _norm(self.waybill_table)
        waybill_table = self.tables.get(waybill_key)
        prev_name: Optional[str] = None
        prev_outputs: Optional[list[str]] = None
        for idx, spec in enumerate(self.raw_models):
            if seen.get(_norm(spec.name)) != spec.name:
                # 模型名本身有问题（已记录错误），后续模型只能做名字级校验
                prev_name, prev_outputs = spec.name, None
                continue
            if idx == 0:
                expected_key = waybill_key
                expected_desc = f"运单源表 {self.waybill_table}"
                from_columns = ([n for n, _ in waybill_table.columns]
                                if waybill_table is not None else None)
            else:
                expected_key = _norm(prev_name)
                expected_desc = f"前一模型 {prev_name}"
                from_columns = prev_outputs
            outputs = self._validate_one_model(
                spec, idx, expected_key, expected_desc, from_columns)
            prev_name, prev_outputs = spec.name, outputs

    def _validate_one_model(self, spec, idx: int, expected_from_key: str,
                            expected_from_desc: str,
                            from_columns: Optional[list[str]]) -> Optional[list[str]]:
        """校验单个模型；返回其输出列（原名），无法确定时返回 None。"""
        name = spec.name
        before = len(self.errors)

        def err(msg: str, loc: Optional[str] = None) -> None:
            self.errors.append(MissingTraceError(msg, model=name, location=loc))

        try:
            statements = sqlglot.parse(spec.sql or "", read=DIALECT)
        except (ParseError, TokenError) as exc:
            err(f"SQL 解析失败: {exc}")
            return None
        real = [s for s in statements if s is not None]
        if len(real) != 1:
            err("每个模型必须是且只能是一条 SELECT 语句")
            return None
        stmt = real[0]
        if not isinstance(stmt, exp.Select):
            err("只支持单条 SELECT（不支持 UNION / INTERSECT / EXCEPT 等）")
            return None
        for key, label in _FORBIDDEN_CLAUSES:
            if stmt.args.get(key) is not None:
                err(f"不支持{label}，只支持直接选列及改名、等值 JOIN、WHERE 比较或判空")

        # FROM：必须是预期前驱（首模型为运单源表，其余为前一模型）
        from_node = stmt.args.get("from_")
        from_table = from_node.this if from_node is not None else None
        from_ok = False
        from_binding_written = ""
        if not isinstance(from_table, exp.Table):
            err("FROM 缺失或不是普通表（不支持子查询 / 表函数）")
        else:
            if from_table.db or from_table.catalog:
                err("表名不要带 catalog / schema 前缀", from_table.sql(DIALECT))
            if _norm(from_table.name) != expected_from_key:
                err(f"FROM 必须读取{expected_from_desc}，"
                    f"实际读取 {from_table.name}")
            else:
                from_ok = True
            from_binding_written = from_table.alias or from_table.name

        bindings: set[str] = set()
        if from_binding_written:
            bindings.add(_norm(from_binding_written))

        # 列解析只在 FROM 正确且前驱输出已知时进行，避免级联误报
        resolution = from_ok and from_columns is not None
        relations: list[Relation] = []
        if resolution:
            relations.append(Relation(
                binding=_norm(from_binding_written),
                display=from_table.sql(DIALECT),
                columns={_norm(c): c for c in from_columns}))

        # JOIN：只允许与源维表做等值 INNER / LEFT JOIN
        joins = stmt.args.get("joins") or []
        for join in joins:
            self._validate_join(join, relations, bindings, resolution, err)

        # WHERE：比较或判空
        where = stmt.args.get("where")
        if where is not None:
            self._check_where(where.this, relations if resolution else None, err)

        # SELECT：直接选列及改名
        outputs: list[str] = []
        out_keys: set[str] = set()
        for proj in stmt.expressions:
            ploc = proj.sql(DIALECT)[:60]
            if isinstance(proj, exp.Star) or (
                    isinstance(proj, exp.Column) and proj.is_star):
                err("不支持星号，请显式列出列名", ploc)
                continue
            col: Optional[exp.Column] = None
            out_name: Optional[str] = None
            if isinstance(proj, exp.Alias):
                inner = _unwrap(proj.this)
                if isinstance(inner, exp.Column) and not inner.is_star:
                    col = inner
                    out_name = proj.alias
            elif isinstance(proj, exp.Column):
                col = proj
                out_name = proj.name
            if col is None or out_name is None:
                err("SELECT 项只支持直接选列或 列 AS 别名", ploc)
                continue
            if resolution:
                self._resolve_select_column(col, relations, err)
            key = _norm(out_name)
            if key == CARRY_COL:
                err(f"输出列名 {out_name} 与内部保留列名冲突", ploc)
                continue
            if key in out_keys:
                err(f"输出列 {out_name} 在同一 SELECT 中重复定义", ploc)
                continue
            out_keys.add(key)
            outputs.append(out_name)
        if not stmt.expressions:
            err("SELECT 至少需要输出一列")

        if len(self.errors) != before:
            # 有错误：不生成执行计划，但仍把输出列名传给下一模型做名字级校验
            return outputs or None

        carry_source = self.id_column if idx == 0 else CARRY_COL
        carry_ref = exp.column(carry_source, table=from_binding_written)
        augmented = stmt.copy()
        augmented.set("expressions", list(augmented.args["expressions"]) + [
            exp.alias_(carry_ref.copy(), CARRY_COL)])
        steps: list[Step] = []
        if idx == 0:
            steps.append(Step(
                model=name, clause="SOURCE",
                detail=f"FROM {from_table.sql(DIALECT)}",
                sql=self._count_sql(stmt, 0, False, carry_ref)))
        for jdx, join in enumerate(joins, start=1):
            steps.append(Step(
                model=name, clause="JOIN",
                detail=join.sql(DIALECT),
                sql=self._count_sql(stmt, jdx, False, carry_ref)))
        if where is not None:
            steps.append(Step(
                model=name, clause="WHERE",
                detail=f"WHERE {where.this.sql(DIALECT)}",
                sql=self._count_sql(stmt, len(joins), True, carry_ref)))
        self.plans.append(ModelPlan(
            name=name, augmented_sql=augmented.sql(DIALECT), steps=steps))
        return outputs

    # --------------------------------------------------------------- JOIN
    def _validate_join(self, join: exp.Join, relations: list[Relation],
                       bindings: set[str], resolution: bool, err) -> None:
        jloc = join.sql(DIALECT)[:80]
        method = (join.args.get("method") or "").upper()
        kind = (join.args.get("kind") or "").upper()
        side = (join.args.get("side") or "").upper()
        ok = True
        if method == "NATURAL":
            err("不支持 NATURAL JOIN，请改写为显式 JOIN ON", jloc)
            ok = False
        if kind == "CROSS":
            err("不支持 CROSS JOIN，只支持等值 INNER JOIN / LEFT JOIN", jloc)
            ok = False
        elif kind not in ("", "INNER", "OUTER"):
            err(f"不支持 {kind} JOIN，只支持 INNER JOIN / LEFT JOIN", jloc)
            ok = False
        if side not in ("", "LEFT"):
            err(f"不支持 {side} JOIN，只支持 INNER JOIN / LEFT JOIN", jloc)
            ok = False

        target = join.this
        tgt_table: Optional[SampleTable] = None
        if not isinstance(target, exp.Table):
            err("JOIN 目标必须是源维表（不支持子查询 / 表函数）", jloc)
            ok = False
        else:
            if target.db or target.catalog:
                err("表名不要带 catalog / schema 前缀", jloc)
                ok = False
            tkey = _norm(target.name)
            if tkey == _norm(self.waybill_table):
                err(f"JOIN 目标 {target.name} 是运单源表，JOIN 只能接源维表", jloc)
                ok = False
            elif tkey in self.model_keys:
                err(f"JOIN 目标 {target.name} 是模型，JOIN 只能接源维表", jloc)
                ok = False
            elif tkey not in self.tables:
                err(f"JOIN 目标 {target.name} 不是已提交的源维表", jloc)
                ok = False
            else:
                tgt_table = self.tables[tkey]

        if join.args.get("using"):
            err("不支持 JOIN USING，请改写为 ON 等值条件", jloc)
            ok = False
        on = join.args.get("on")
        if on is None and not join.args.get("using"):
            err("JOIN 必须带 ON 等值条件", jloc)
            ok = False

        binding_written = ""
        if isinstance(target, exp.Table):
            binding_written = target.alias or target.name
            if _norm(binding_written) in bindings:
                err(f"关系别名 {binding_written} 在同一查询中重复", jloc)
                ok = False
            else:
                bindings.add(_norm(binding_written))
        if not ok:
            return

        if resolution and tgt_table is not None:
            relation = Relation(
                binding=_norm(binding_written),
                display=target.sql(DIALECT),
                columns=dict(tgt_table.col_keys))
            earlier = {r.binding for r in relations}
            relations.append(relation)
            self._check_join_on(on, relation, earlier, relations, err)

    def _check_join_on(self, on_expr: exp.Expression, relation: Relation,
                       earlier: set[str], relations: list[Relation], err) -> None:
        for cond in _flatten_and(on_expr):
            cloc = cond.sql(DIALECT)[:80]
            if not isinstance(cond, exp.EQ):
                err("JOIN ON 只支持等值条件（列 = 列，可用 AND 连接）", cloc)
                continue
            left, right = _unwrap(cond.left), _unwrap(cond.right)
            if not (isinstance(left, exp.Column) and isinstance(right, exp.Column)):
                err("JOIN ON 等值条件两侧都必须是列", cloc)
                continue
            side_bindings: list[str] = []
            for col in (left, right):
                binding = self._check_condition_column(col, relations, err)
                if binding:
                    side_bindings.append(binding)
            if len(side_bindings) != 2:
                continue
            if side_bindings[0] == side_bindings[1]:
                err("JOIN ON 等值条件两侧不能来自同一关系", cloc)
            elif relation.binding not in side_bindings:
                err(f"JOIN ON 等值条件必须关联本 JOIN 的维表 {relation.display}", cloc)
            elif not ({*side_bindings} - {relation.binding}) <= earlier:
                err("JOIN ON 等值条件另一侧必须来自 FROM 或更早的 JOIN", cloc)

    # --------------------------------------------------------------- WHERE
    def _check_where(self, node: exp.Expression,
                     relations: Optional[list[Relation]], err) -> None:
        node = _unwrap(node)
        if isinstance(node, (exp.And, exp.Or)):
            self._check_where(node.left, relations, err)
            self._check_where(node.right, relations, err)
            return
        if isinstance(node, exp.Not):
            inner = _unwrap(node.this)
            if isinstance(inner, exp.Is) and isinstance(
                    _unwrap(inner.expression), exp.Null):
                self._require_column(inner.this, relations, err, "IS NOT NULL")
            else:
                err("WHERE 中 NOT 仅支持 IS NOT NULL 判空形式",
                    node.sql(DIALECT)[:60])
            return
        if isinstance(node, exp.Is):
            if isinstance(_unwrap(node.expression), exp.Null):
                self._require_column(node.this, relations, err, "IS NULL")
            else:
                err("判空只支持 IS NULL / IS NOT NULL", node.sql(DIALECT)[:60])
            return
        if isinstance(node, COMPARE_OPS):
            ncols = 0
            for operand in (_unwrap(node.left), _unwrap(node.right)):
                if isinstance(operand, exp.Column):
                    ncols += 1
                    self._check_condition_column(operand, relations, err)
                elif isinstance(operand, exp.Literal):
                    pass
                elif isinstance(operand, exp.Null):
                    err("与 NULL 比较请改用 IS NULL / IS NOT NULL",
                        node.sql(DIALECT)[:60])
                elif (isinstance(operand, exp.Neg)
                      and isinstance(_unwrap(operand.this), exp.Literal)
                      and not _unwrap(operand.this).is_string):
                    pass  # 负数字面量
                else:
                    err("WHERE 只支持列与常量、列与列的比较或判空，"
                        f"不支持 {operand.sql(DIALECT)[:40]}",
                        node.sql(DIALECT)[:60])
            if ncols == 0:
                err("比较两侧都是常量，无意义", node.sql(DIALECT)[:60])
            return
        err(f"WHERE 只支持比较或判空，不支持 {node.sql(DIALECT)[:60]}")

    def _require_column(self, node: exp.Expression,
                        relations: Optional[list[Relation]], err, label: str) -> None:
        node = _unwrap(node)
        if not isinstance(node, exp.Column):
            err(f"{label} 判空对象必须是列", node.sql(DIALECT)[:60])
            return
        self._check_condition_column(node, relations, err)

    def _check_condition_column(self, col: exp.Column,
                                relations: Optional[list[Relation]], err) -> Optional[str]:
        """校验条件列（必须带表名或别名）；返回解析到的绑定（小写）。"""
        cloc = col.sql(DIALECT)[:60]
        if not col.table:
            err(f"条件列 {cloc} 必须带表名或别名", cloc)
            return None
        if col.args.get("db") or col.args.get("catalog"):
            err(f"条件列 {cloc} 只支持 表.列 两节形式", cloc)
            return None
        if relations is None:
            return None
        binding = _norm(col.table)
        relation = next((r for r in relations if r.binding == binding), None)
        if relation is None:
            err(f"条件列 {cloc} 引用的表/别名 {col.table} 不在当前查询中", cloc)
            return None
        if _norm(col.name) not in relation.columns:
            available = ", ".join(sorted(relation.columns.values()))
            err(f"关系 {relation.display} 中找不到列 {col.name}，可用列: {available}",
                cloc)
            return None
        return binding

    # -------------------------------------------------------------- SELECT
    def _resolve_select_column(self, col: exp.Column, relations: list[Relation],
                               err) -> None:
        cloc = col.sql(DIALECT)[:60]
        if col.args.get("db") or col.args.get("catalog"):
            err(f"列 {cloc} 只支持 表.列 两节形式", cloc)
            return
        if col.table:
            binding = _norm(col.table)
            relation = next((r for r in relations if r.binding == binding), None)
            if relation is None:
                err(f"列 {cloc} 引用的表/别名 {col.table} 不在当前查询中", cloc)
                return
            if _norm(col.name) not in relation.columns:
                available = ", ".join(sorted(relation.columns.values()))
                err(f"关系 {relation.display} 中找不到列 {col.name}，"
                    f"可用列: {available}", cloc)
            return
        candidates = [r for r in relations if _norm(col.name) in r.columns]
        if not candidates:
            err(f"找不到列 {col.name}", cloc)
        elif len(candidates) > 1:
            where = ", ".join(r.display for r in candidates)
            err(f"列 {col.name} 存在歧义，可来自: {where}，请加表别名限定", cloc)

    # ----------------------------------------------------------- 计数 SQL
    @staticmethod
    def _count_sql(select: exp.Select, n_joins: int, include_where: bool,
                   carry_ref: exp.Expression) -> str:
        """生成某环节的按运单分组计数查询（重复匹配照计）。"""
        q = select.copy()
        q.set("expressions", [
            exp.alias_(carry_ref.copy(), "wb"),
            exp.alias_(exp.Count(this=exp.Star()), "n"),
        ])
        q.set("joins", (q.args.get("joins") or [])[:n_joins])
        if not include_where:
            q.args.pop("where", None)
        q.set("group", exp.Group(expressions=[carry_ref.copy()]))
        return q.sql(DIALECT)

    # --------------------------------------------------------------- 执行
    def _execute(self) -> list[tuple[Step, dict[str, int]]]:
        """同批运单共用一次内存 DuckDB 试算；只读提交的样本，不落盘。"""
        con = duckdb.connect(":memory:")
        try:
            for table in self.tables.values():
                cols = ", ".join(f"{_qi(n)} {t}" for n, t in table.columns)
                con.execute(f"CREATE TABLE {_qi(table.name)} ({cols})")
                if table.rows:
                    placeholders = ", ".join("?" for _ in table.columns)
                    con.executemany(
                        f"INSERT INTO {_qi(table.name)} VALUES ({placeholders})",
                        table.rows)
            results: list[tuple[Step, dict[str, int]]] = []
            for plan in self.plans:
                try:
                    con.execute(
                        f"CREATE VIEW {_qi(plan.name)} AS {plan.augmented_sql}")
                    for step in plan.steps:
                        rows = con.execute(step.sql).fetchall()
                        results.append((step, {str(wb): n for wb, n in rows}))
                except duckdb.Error as exc:
                    raise MissingTraceError(
                        f"DuckDB 试算失败: {exc}", model=plan.name) from exc
            return results
        finally:
            con.close()

    # --------------------------------------------------------------- 报告
    def run(self) -> list[dict]:
        self._validate_samples()
        self._validate_ids()
        self._validate_models()
        if self.errors:
            raise AggregatedMissingTraceError(self.errors)
        step_results = self._execute()
        reports: list[dict] = []
        for norm_id in self.ids:
            key = str(norm_id)
            steps: list[dict] = []
            for step, counts in step_results:
                steps.append({"model": step.model, "clause": step.clause,
                              "detail": step.detail,
                              "rows": counts.get(key, 0)})
            source_rows = steps[0]["rows"] if steps else 0
            first_zero: Optional[dict] = None
            if source_rows > 0:
                for entry, (step, _) in zip(steps, step_results):
                    if step.clause != "SOURCE" and entry["rows"] == 0:
                        first_zero = {"model": step.model, "clause": step.clause,
                                      "detail": step.detail}
                        break
            current = steps[-1]["rows"] if steps else 0
            if source_rows == 0:
                status = "missing_in_source"      # 源表缺票，单独标记
            elif first_zero is not None:
                status = "dropped_in_pipeline"    # 中途归零，见 first_zero
            else:
                status = "not_missing"            # 最后仍有行，未漏单
            reports.append({
                "waybill_id": norm_id,
                "status": status,
                "final_rows": current,
                "first_zero": first_zero,
                "steps": steps,
            })
        return reports
