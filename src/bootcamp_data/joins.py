import pandas as pd


def safe_left_join(
    left: pd.DataFrame,
    right: pd.DataFrame,
    on: str | list[str],
    *,
    validate: str,
    suffixes: tuple[str, str] = ("", "_right"),
) -> pd.DataFrame:
    joined = left.merge(
        right,
        how="left",
        on=on,
        validate=validate,
        suffixes=suffixes,
    )
    return joined
