import sys
import logging
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
sys.path.insert(0, str(SRC))

from bootcamp_data.config import make_paths
from bootcamp_data.io import read_orders_csv, read_users_csv, write_parquet
from bootcamp_data.transforms import (
    enforce_schema,
    missingness_report,
    add_missing_flags,
    normalize_text,
    apply_mapping,
)
from bootcamp_data.quality import (
    require_columns,
    assert_non_empty,
    assert_in_range,
)

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
log = logging.getLogger("day2")


def main() -> None:
    p = make_paths(ROOT)

    log.info("Loading raw CSVs...")
    orders_raw = read_orders_csv(p.raw / "orders.csv")
    users_raw = read_users_csv(p.raw / "users.csv")
    log.info("Rows loaded: orders=%s users=%s", len(orders_raw), len(users_raw))

    require_columns(
        orders_raw,
        ["order_id", "user_id", "amount", "quantity", "created_at", "status"],
    )
    require_columns(users_raw, ["user_id", "country", "signup_date"])
    assert_non_empty(orders_raw, "orders_raw")
    assert_non_empty(users_raw, "users_raw")

    orders = enforce_schema(orders_raw)

    report_df = missingness_report(orders)
    reports_dir = ROOT / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    report_path = reports_dir / "missingness_orders.csv"
    report_df.to_csv(report_path)
    log.info("Wrote missingness report: %s", report_path)

    status_norm = normalize_text(orders["status"])
    status_map = {
        "paid": "paid",
        "refund": "refund",
        "refunded": "refund",
        "payment complete": "paid",
    }
    status_clean = apply_mapping(status_norm, status_map)

    orders_clean = orders.copy()
    orders_clean["status_clean"] = status_clean
    orders_clean = add_missing_flags(orders_clean, ["amount", "quantity"])


    # علشان نحدد Range معين للقيم 
    assert_in_range(orders_clean["amount"], lo=0, name="amount")
    assert_in_range(orders_clean["quantity"], lo=1, hi=100, name="quantity")

    write_parquet(orders_clean, p.processed / "orders_clean.parquet")
    write_parquet(users_raw, p.processed / "users.parquet")
    log.info("Done. Outputs in: %s", p.processed)


if __name__ == "__main__":
    main()

