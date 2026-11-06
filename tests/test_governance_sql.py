import pytest

from lakehouse.cli import governance_statements


def test_statements_split_cleanly_and_name_the_catalog() -> None:
    statements = governance_statements("payments_dev")
    assert len(statements) == 8
    assert all("{catalog}" not in s and "--" not in s for s in statements)
    assert sum("SET MASK" in s for s in statements) == 3
    assert statements[-1].startswith(
        "ALTER TABLE payments_dev.gold.fct_transactions SET ROW FILTER"
    )


def test_unsafe_catalog_names_are_refused() -> None:
    with pytest.raises(SystemExit):
        governance_statements("payments; DROP CATALOG payments")
