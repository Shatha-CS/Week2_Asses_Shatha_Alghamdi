import pandas as pd

def enforce_schema(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(
        order_id=df["order_id"].astype("string"),
        user_id=df["user_id"].astype("string"),
        amount=pd.to_numeric(df["amount"], errors="coerce").astype("Float64"),
        quantity=pd.to_numeric(df["quantity"], errors="coerce").astype("Int64"),
    )



def missingness_report(df: pd.DataFrame) -> pd.DataFrame:
    n_rows = len(df)

    missing_count = df.isna().sum()
    missing_percent = missing_count / n_rows

    report = pd.DataFrame(
        {
            "n_missing": missing_count,
            "p_missing": missing_percent,
        }
    ).sort_values("p_missing", ascending=False)

    return report


def add_missing_flags(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    out = df.copy()

    for col in cols:
        flag_col = f"{col}__isna"
        out[flag_col] = out[col].isna()

    return out



def normalize_text(series: pd.Series) -> pd.Series:
    
    s = series.astype("string")

    
    s = s.str.strip().str.lower()

    s = s.str.replace("  ", " ", regex=False)
    s = s.str.replace("  ", " ", regex=False)
    s = s.str.replace("  ", " ", regex=False)

    return s


def apply_mapping(series: pd.Series, mapping: dict[str, str]) -> pd.Series:
    
    return series.map(lambda x: mapping.get(x, x))




def dedupe_keep_latest(
    df: pd.DataFrame,
    key_cols: list[str],
    ts_col: str,
) -> pd.DataFrame:
    
    
    sorted_df = df.sort_values(by=ts_col)
    
   
    latest_rows_df = sorted_df.drop_duplicates(
        subset=key_cols,
        keep="last"
    )
    
    
    latest_rows_df = latest_rows_df.reset_index(drop=True)
    
    return latest_rows_df









