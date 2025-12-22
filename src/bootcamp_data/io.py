from pathlib import Path
import pandas as pd


NA_VALUES = ["", "NA", "N/A", "null", "None"]


def read_orders_csv(path: Path) -> pd.DataFrame:
    df = pd.read_csv(
        path,
        dtype={"order_id": "string", "user_id": "string"},
        na_values=NA_VALUES,
        keep_default_na=True,
    )
    return df


def read_users_csv(path: Path) -> pd.DataFrame:
    df = pd.read_csv(
        path,
        dtype={"user_id": "string"},
        na_values=NA_VALUES,
        keep_default_na=True,
    )
    return df


def write_parquet(df: pd.DataFrame, path: Path) -> None:
    
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)


def read_parquet(path: Path) -> pd.DataFrame:
    return pd.read_parquet(path)

