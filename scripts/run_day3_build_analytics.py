import sys
import logging
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
sys.path.insert(0, str(SRC))

from bootcamp_data.config import make_paths
from bootcamp_data.quality import require_columns, assert_non_empty, assert_unique_key
from bootcamp_data.transforms import parse_datetime, add_time_parts, winsorize
from bootcamp_data.joins import safe_left_join


logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
log = logging.getLogger("day3")


def main() -> None:
    p = make_paths(ROOT)

    log.info("Loading processed inputs...")
    orders = pd.read_parquet(p.processed / "orders_clean.parquet")
    users = pd.read_parquet(p.processed / "users.parquet")
    log.info("Rows: orders=%s users=%s", len(orders), len(users))

    require_columns(orders, ["order_id", "user_id", "amount", "quantity", "created_at", "status_clean"])
    require_columns(users, ["user_id", "country", "signup_date"])
    assert_non_empty(orders, "orders_clean")
    assert_non_empty(users, "users")

    assert_unique_key(users, "user_id")

    orders_time = parse_datetime(orders, col="created_at", utc=True)
    orders_time = add_time_parts(orders_time, ts_col="created_at")

    missing_ts = int(orders_time["created_at"].isna().sum())
    log.info("Missing created_at after parse: %s / %s", missing_ts, len(orders_time))

    before_rows = len(orders_time)

    analytics = safe_left_join(
        left=orders_time,
        right=users,
        on="user_id",
        validate="many_to_one",
        suffixes=("", "_user"),
    )

    after_rows = len(analytics)
    log.info("Rows before join=%s after join=%s", before_rows, after_rows)
    assert after_rows == before_rows, "Join changed row count"

    country_match_rate = 1.0 - float(analytics["country"].isna().mean())
    log.info("country match rate: %.3f", country_match_rate)

    analytics["amount_winsor"] = winsorize(analytics["amount"], lo=0.01, hi=0.99)

    summary = (
        analytics
        .groupby("country", dropna=False)
        .agg(
            orders_count=("order_id", "count"),
            total_revenue=("amount", "sum"),
        )
        .reset_index()
        .sort_values("total_revenue", ascending=False)
    )

    print(summary)

    reports_dir = ROOT / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    summary.to_csv(reports_dir / "revenue_by_country.csv", index=False)

    out_path = p.processed / "analytics_table.parquet"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    analytics.to_parquet(out_path, index=False)
    log.info("Wrote analytics table: %s", out_path)


if __name__ == "__main__":
    main()

