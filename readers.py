"""
Readers: how to get metadata, counts and rows from one side.

    OracleReader   Oracle over JDBC. Oracle only serves rows (and a COUNT / metadata);
                   every comparison happens in Spark.
    TableReader    Databricks / Spark catalog tables (e.g. it_bronze_dev.test_only.fact_sales_src).

Both expose the same methods, so the rest of the framework does not care which one it gets:
    columns(table)                                   -> [ColumnInfo]
    count(table, where)                              -> int
    read(table, cols, where, n_rows)                 -> DataFrame   (whole table / filtered)
    sample(table, cols, where, n_rows, fraction, seed) -> DataFrame (random rows)
    lookup(table, cols, where, keys, keys_df, n_rows)  -> (DataFrame, note)  (rows with these keys)
Column names are always returned in UPPER case.
"""
from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from decimal import Decimal
from functools import reduce
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .config import Connection

IN_LIST_MAX = 1000            # Oracle allows at most 1000 values in one IN (...) list
LISTS_PER_LOOKUP_QUERY = 5    # indexed lookups: 5 lists = 5000 keys per query (one Spark task each)


@dataclass
class ColumnInfo:
    name: str          # upper case
    type: str          # display type, e.g. NUMBER(10,2), VARCHAR2(50), decimal(10,2), string
    nullable: bool


def _and(*conds: Optional[str]) -> str:
    conds = [f"({c})" for c in conds if c]
    return " AND ".join(conds) if conds else "1=1"


# =========================================================================== Databricks / Spark tables
class TableReader:
    def __init__(self, spark: SparkSession, conn: Connection):
        self.spark, self.conn = spark, conn

    def fq(self, table: str) -> str:
        return self.conn.name.format(table=table.lower())

    def _df(self, table: str) -> DataFrame:
        df = self.spark.table(self.fq(table))
        return df.toDF(*[c.upper() for c in df.columns])

    def _base(self, table: str, where: Optional[str]) -> DataFrame:
        df = self._df(table)
        return df.where(where) if where else df

    def columns(self, table: str) -> list[ColumnInfo]:
        parts = self.fq(table).split(".")
        if len(parts) == 3:                     # Unity Catalog: exact types + nullability
            cat, sch, tbl = parts
            try:
                rows = self.spark.sql(
                    f"SELECT column_name, full_data_type, is_nullable FROM `{cat}`.information_schema.columns "
                    f"WHERE lower(table_schema) = '{sch.lower()}' AND lower(table_name) = '{tbl.lower()}' "
                    f"ORDER BY ordinal_position").collect()
                if rows:
                    return [ColumnInfo(r[0].upper(), r[1].lower(), str(r[2]).upper() in ("YES", "TRUE"))
                            for r in rows]
            except Exception:
                pass                            # no information_schema access -> fall back to the DataFrame schema
        return [ColumnInfo(f.name, f.dataType.simpleString(), f.nullable) for f in self._df(table).schema.fields]

    def count(self, table: str, where: Optional[str]) -> int:
        return self._base(table, where).count()

    def read(self, table, cols, where, n_rows=None) -> DataFrame:
        return self._base(table, where).select(*cols)

    def sample(self, table, cols, where, n_rows, fraction, seed) -> DataFrame:
        return self._base(table, where).sample(False, fraction, seed).select(*cols)

    def lookup(self, table, cols, where, keys, keys_df, n_rows=None):
        wanted = keys_df.select(*keys).dropna().distinct()
        rows = self._base(table, where).join(F.broadcast(wanted), keys, "left_semi").select(*cols)
        return rows, "key join"

    def describe_lookup(self, table, keys) -> str:
        return "key join"


# =========================================================================== Oracle
class OracleReader:
    def __init__(self, spark: SparkSession, conn: Connection, read_partitions: int = 16):
        self.spark, self.conn, self.parts = spark, conn, read_partitions
        self._types: dict[str, dict[str, str]] = {}
        self._index: dict[str, Optional[str]] = {}

    # ---- plumbing
    def _props(self) -> dict:
        p = {"driver": "oracle.jdbc.OracleDriver", "fetchsize": str(self.conn.fetchsize), "user": self.conn.user}
        pwd = self.conn.secret(self.spark)
        if pwd:
            p["password"] = pwd
        p.update({k: str(v) for k, v in self.conn.properties.items()})
        return p

    def fq(self, table: str) -> str:
        return f"{self.conn.schema}.{table}".upper()

    def _sql(self, sql: str) -> DataFrame:
        """One JDBC query = one Spark partition."""
        return self.spark.read.jdbc(self.conn.url, f"({sql}) q", properties=self._props())

    def _union(self, sqls: list[str]) -> DataFrame:
        """Several JDBC queries read in parallel (one Spark task each)."""
        return reduce(DataFrame.unionByName, [self._sql(s) for s in sqls])

    # ---- metadata
    def columns(self, table: str) -> list[ColumnInfo]:
        rows = self._sql(f"""
            SELECT COLUMN_NAME, DATA_TYPE, DATA_LENGTH, CHAR_LENGTH, DATA_PRECISION, DATA_SCALE, NULLABLE
            FROM ALL_TAB_COLUMNS
            WHERE OWNER = '{self.conn.schema.upper()}' AND TABLE_NAME = '{table.upper()}'
            ORDER BY COLUMN_ID""").collect()
        if not rows:
            raise ValueError(f"{self.fq(table)} not found or no access")
        out = []
        for r in rows:
            t = r.DATA_TYPE
            if t in ("VARCHAR2", "NVARCHAR2", "CHAR", "NCHAR"):
                t = f"{t}({int(r.CHAR_LENGTH or r.DATA_LENGTH)})"
            elif t == "NUMBER" and r.DATA_PRECISION is not None:
                t = f"NUMBER({int(r.DATA_PRECISION)},{int(r.DATA_SCALE or 0)})"
            out.append(ColumnInfo(r.COLUMN_NAME.upper(), t, r.NULLABLE == "Y"))
        self._types[table.upper()] = {c.name: c.type for c in out}
        return out

    def count(self, table: str, where: Optional[str]) -> int:
        return int(self._sql(f"SELECT COUNT(*) AS CNT FROM {self.fq(table)} WHERE {_and(where)}").first()["CNT"])

    def key_index(self, table: str, keys: list[str]) -> Optional[str]:
        """Name of a usable index whose leading column is one of the key columns, else None."""
        t = table.upper()
        if t not in self._index:
            try:
                rows = self._sql(f"""
                    SELECT ic.INDEX_NAME, ic.COLUMN_NAME
                    FROM ALL_IND_COLUMNS ic
                    JOIN ALL_INDEXES i ON i.OWNER = ic.INDEX_OWNER AND i.INDEX_NAME = ic.INDEX_NAME
                    WHERE ic.TABLE_OWNER = '{self.conn.schema.upper()}' AND ic.TABLE_NAME = '{t}'
                      AND ic.COLUMN_POSITION = 1 AND i.STATUS IN ('VALID', 'N/A')""").collect()
                self._index[t] = next((r.INDEX_NAME for r in rows if r.COLUMN_NAME.upper() in keys), None)
            except Exception:
                self._index[t] = None            # no access to the dictionary views -> treat as not indexed
        return self._index[t]

    def describe_lookup(self, table, keys) -> str:
        idx = self.key_index(table, keys)
        return f"index lookup ({idx})" if idx else "single pass (no index on the key)"

    # ---- parallel full reads: ROWID ranges, so the slices together read the table once
    def _rowid_slices(self, table: str, n_rows: Optional[int]) -> list[Optional[str]]:
        k = self.parts
        if k <= 1 or not n_rows or n_rows < 50_000:
            return [None]
        pct = min(99.0, max(0.000001, 100.0 * k * 200 / n_rows))     # ~200 sampled ROWIDs per slice
        try:
            bounds = [r[0] for r in self._sql(f"""
                SELECT ROWIDTOCHAR(MIN(R)) AS LO FROM (
                    SELECT R, NTILE({k}) OVER (ORDER BY R) AS NT
                    FROM (SELECT ROWID AS R FROM {self.fq(table)} SAMPLE BLOCK ({pct:.6f})))
                GROUP BY NT ORDER BY NT""").collect()]
        except Exception:
            return [None]                         # e.g. index-organized table: read in one piece
        if len(bounds) < 2:
            return [None]
        rid = lambda b: f"CHARTOROWID('{b}')"
        conds = [f"ROWID < {rid(bounds[1])}"]
        conds += [f"ROWID >= {rid(bounds[i])} AND ROWID < {rid(bounds[i + 1])}" for i in range(1, len(bounds) - 1)]
        conds += [f"ROWID >= {rid(bounds[-1])}"]
        return conds

    def read(self, table, cols, where, n_rows=None) -> DataFrame:
        sel = ", ".join(cols)
        return self._union([f"SELECT {sel} FROM {self.fq(table)} WHERE {_and(where, s)}"
                            for s in self._rowid_slices(table, n_rows)])

    # ---- sampling: Oracle reads only the sampled blocks
    def sample(self, table, cols, where, n_rows, fraction, seed) -> DataFrame:
        pct = fraction * 100
        if pct >= 99.999999:
            return self.read(table, cols, where, n_rows)
        return self._sql(f"SELECT {', '.join(cols)} FROM {self.fq(table)} "
                         f"SAMPLE BLOCK ({max(pct, 0.000001):.6f}) SEED ({int(seed)}) WHERE {_and(where)}")

    # ---- lookup: fetch exactly the given keys
    def _literal(self, v, oracle_type: str) -> str:
        t = (oracle_type or "").upper()
        if isinstance(v, (dt.datetime, dt.date)) and t == "DATE":
            v = v if isinstance(v, dt.datetime) else dt.datetime.combine(v, dt.time())
            return f"TO_DATE('{v:%Y-%m-%d %H:%M:%S}', 'YYYY-MM-DD HH24:MI:SS')"
        if isinstance(v, dt.datetime):
            return f"TO_TIMESTAMP('{v:%Y-%m-%d %H:%M:%S.%f}', 'YYYY-MM-DD HH24:MI:SS.FF6')"
        if isinstance(v, dt.date):
            return f"DATE '{v:%Y-%m-%d}'"
        if isinstance(v, bool):
            return "1" if v else "0"
        if isinstance(v, Decimal):
            return format(v, "f")
        if isinstance(v, (int, float)):
            return repr(v)
        return "'" + str(v).replace("'", "''") + "'"

    def key_in_lists(self, table: str, keys: list[str], rows) -> list[str]:
        """IN conditions of at most 1000 keys each, e.g. (ORDER_ID, LINE) IN ((1,1),(1,2),...)."""
        types = self._types.get(table.upper()) or {c.name: c.type for c in self.columns(table)}
        lits = []
        for r in rows:
            vals = [self._literal(r[k], types.get(k, "")) for k in keys]
            lits.append(vals[0] if len(keys) == 1 else "(" + ", ".join(vals) + ")")
        col = keys[0] if len(keys) == 1 else "(" + ", ".join(keys) + ")"
        return [f"{col} IN ({', '.join(lits[i:i + IN_LIST_MAX])})" for i in range(0, len(lits), IN_LIST_MAX)]

    def lookup(self, table, cols, where, keys, keys_df, n_rows=None):
        rows = keys_df.select(*keys).dropna().distinct().collect()
        sel, src = ", ".join(cols), self.fq(table)
        if not rows:
            return self._sql(f"SELECT {sel} FROM {src} WHERE 1 = 0"), "no keys"
        lists = self.key_in_lists(table, keys, rows)
        idx = self.key_index(table, keys)
        if idx:     # index: many small queries, each touching only its keys
            groups = [lists[i:i + LISTS_PER_LOOKUP_QUERY] for i in range(0, len(lists), LISTS_PER_LOOKUP_QUERY)]
            sqls = [f"SELECT {sel} FROM {src} WHERE {_and(where, ' OR '.join(g))}" for g in groups]
            return self._union(sqls), f"index lookup ({idx}), {len(rows):,} keys in {len(sqls)} queries"
        # no index: one pass over the table (ROWID slices), every slice filters on the full key list
        cond = " OR ".join(lists)
        sqls = [f"SELECT {sel} FROM {src} WHERE {_and(where, s, cond)}" for s in self._rowid_slices(table, n_rows)]
        return self._union(sqls), f"single pass (no index on the key), {len(rows):,} keys"


def make_reader(spark: SparkSession, conn: Connection, read_partitions: int = 16):
    return OracleReader(spark, conn, read_partitions) if conn.type == "oracle" else TableReader(spark, conn)
