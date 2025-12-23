import pandas as pd


def require_columns(df: pd.DataFrame, columns: list[str]) -> None:
    for col in columns:
        assert col in df.columns, f"Required column missing: {col}"


def assert_non_empty(df: pd.DataFrame, name: str = "DataFrame") -> None:
    assert len(df) > 0, f"{name} is empty"


def assert_unique_key(
    df: pd.DataFrame,
    key: str,
    *,
    allow_na: bool = False,
) -> None:
    series = df[key]

    if not allow_na:
        assert series.notna().all(), f"{key} contains missing values"

    duplicates = series[series.notna()].duplicated()
    assert not duplicates.any(), f"{key} contains duplicate values"


def assert_in_range(
    series: pd.Series,
    lo=None,
    hi=None,
    name: str = "value",
) -> None:
    values = series.dropna()

    if lo is not None:
        assert (values >= lo).all(), f"{name} has values below {lo}"

    if hi is not None:
        assert (values <= hi).all(), f"{name} has values above {hi}"
