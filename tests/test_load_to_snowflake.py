from datetime import datetime
from pathlib import Path

import pytest

from loading.load_to_snowflake import filter_pending, parse_transform_statements

REAL_TRANSFORM_SQL = Path(__file__).resolve().parents[1] / "src" / "transformations" / "scd_type_2_customers.sql"


def test_filter_pending_skips_already_loaded():
    objects = [
        {"Key": "raw/a.csv", "LastModified": datetime(2026, 1, 2)},
        {"Key": "raw/b.csv", "LastModified": datetime(2026, 1, 1)},
    ]
    assert filter_pending(objects, already_loaded={"raw/a.csv"}) == ["raw/b.csv"]


def test_filter_pending_orders_oldest_first():
    objects = [
        {"Key": "raw/newer.csv", "LastModified": datetime(2026, 1, 2)},
        {"Key": "raw/older.csv", "LastModified": datetime(2026, 1, 1)},
    ]
    assert filter_pending(objects, already_loaded=set()) == ["raw/older.csv", "raw/newer.csv"]


def test_filter_pending_empty_when_all_loaded():
    objects = [{"Key": "raw/a.csv", "LastModified": datetime(2026, 1, 1)}]
    assert filter_pending(objects, already_loaded={"raw/a.csv"}) == []


def test_parse_transform_statements_splits_ddl_and_merge():
    sql_text = (
        "CREATE TABLE a (x INT); CREATE TABLE b (y INT); "
        "MERGE INTO b USING a ON 1=1 WHEN MATCHED THEN DELETE; "
        "INSERT INTO b SELECT * FROM a;"
    )
    ddl, merge = parse_transform_statements(sql_text)
    assert len(ddl) == 2
    assert len(merge) == 2
    assert ddl[0].startswith("CREATE TABLE a")
    assert merge[0].startswith("MERGE INTO b")


def test_parse_transform_statements_rejects_wrong_count():
    with pytest.raises(ValueError):
        parse_transform_statements("SELECT 1;")


def test_real_transform_sql_has_four_statements():
    """Guards against someone editing the .sql file and breaking the 4-statement
    assumption the loader relies on."""
    ddl, merge = parse_transform_statements(REAL_TRANSFORM_SQL.read_text())
    assert len(ddl) == 2
    assert len(merge) == 2
