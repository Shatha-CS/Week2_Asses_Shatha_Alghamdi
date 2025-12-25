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




def parse_datetime(df: pd.DataFrame, col: str, utc: bool = True) -> pd.DataFrame:
    parsed = pd.to_datetime(df[col], errors="coerce", utc=utc)
    out = df.copy()
    out[col] = parsed
    return out


def add_time_parts(df: pd.DataFrame, ts_col: str) -> pd.DataFrame:
    ts = df[ts_col]
    return df.assign(
        date=ts.dt.date,
        year=ts.dt.year,
        month=ts.dt.to_period("M").astype("string"),
        dow=ts.dt.day_name(),
        hour=ts.dt.hour,
    )





def iqr_bounds(s: pd.Series, k: float = 1.5) -> tuple[float, float]:
    clean = pd.to_numeric(s, errors="coerce").dropna()

    if clean.empty:
        return (float("nan"), float("nan"))

    q1 = clean.quantile(0.25)
    q3 = clean.quantile(0.75)
    iqr = q3 - q1

    low = q1 - k * iqr
    high = q3 + k * iqr
    return float(low), float(high)


def winsorize(s: pd.Series, lo: float = 0.01, hi: float = 0.99) -> pd.Series:
    x = pd.to_numeric(s, errors="coerce")
    non_missing = x.dropna()

    if non_missing.empty:
        return x

    lo_val = non_missing.quantile(lo)
    hi_val = non_missing.quantile(hi)

    return x.clip(lower=lo_val, upper=hi_val)







def add_outlier_flag(df: pd.DataFrame, col: str, k: float = 1.5, out_col: str | None = None) -> pd.DataFrame:
   
    s = pd.to_numeric(df[col], errors="coerce")

    q1 = s.quantile(0.25)
    q3 = s.quantile(0.75)
    iqr = q3 - q1

    low = q1 - k * iqr
    high = q3 + k * iqr

    flag_name = out_col or f"{col}__outlier"
    return df.assign(**{flag_name: (s < low) | (s > high)})













