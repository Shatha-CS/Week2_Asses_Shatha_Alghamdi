from __future__ import annotations

import json
import logging
from dataclasses import asdict, dataclass
from pathlib import Path

import pandas as pd

from bootcamp_data.io import read_orders_csv, read_users_csv, read_parquet, write_parquet
from bootcamp_data.quality import require_columns, assert_non_empty, assert_unique_key
from bootcamp_data.transforms import (
    enforce_schema,
    add_missing_flags,
    normalize_text,
    apply_mapping,
    parse_datetime,
    add_time_parts,
    winsorize,
    add_outlier_flag,
)
from bootcamp_data.joins import safe_left_join

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class ETLConfig:
    root: Path
    raw_orders: Path
    raw_users: Path
    out_orders_clean: Path
    out_users: Path
    out_analytics: Path
    run_meta: Path

    @classmethod
    def from_root(cls, root: Path) -> "ETLConfig":
        data = root / "data"
        raw = data / "raw"
        processed = data / "processed"

        def pick(*candidates: Path) -> Path:
            for p in candidates:
                if p.exists():
                    return p
            return candidates[0]

        raw_orders = pick(
            raw / "orders.csv",
            raw / "orders.parquet",
            root / "orders.csv",
            root / "orders.parquet",
        )
        raw_users = pick(
            raw / "users.csv",
            raw / "users.parquet",
            root / "users.csv",
            root / "users.parquet",
        )

        return cls(
            root=root,
            raw_orders=raw_orders,
            raw_users=raw_users,
            out_orders_clean=processed / "orders_clean.parquet",
            out_users=processed / "users.parquet",
            out_analytics=processed / "analytics_table.parquet",
            run_meta=processed / "_run_meta.json",
        )


def load_inputs(cfg: ETLConfig) -> tuple[pd.DataFrame, pd.DataFrame]:
    if cfg.raw_orders.suffix.lower() == ".csv":
        orders_raw = read_orders_csv(cfg.raw_orders)
    else:
        orders_raw = read_parquet(cfg.raw_orders)

    if cfg.raw_users.suffix.lower() == ".csv":
        users_raw = read_users_csv(cfg.raw_users)
    else:
        users_raw = read_parquet(cfg.raw_users)

    return orders_raw, users_raw


def transform(orders_raw: pd.DataFrame, users: pd.DataFrame) -> pd.DataFrame:
    require_columns(orders_raw, ["order_id", "user_id", "amount", "quantity", "created_at", "status"])
    require_columns(users, ["user_id", "country", "signup_date"])
    assert_non_empty(orders_raw, "orders_raw")
    assert_non_empty(users, "users")

    assert_unique_key(users, "user_id")

    orders = enforce_schema(orders_raw)

    status_norm = normalize_text(orders["status"])
    mapping = {"paid": "paid", "refund": "refund", "refunded": "refund"}
    orders = orders.assign(status_clean=apply_mapping(status_norm, mapping))

    orders = add_missing_flags(orders, ["amount", "quantity"])

    orders = parse_datetime(orders, col="created_at", utc=True)
    orders = add_time_parts(orders, ts_col="created_at")

    joined = safe_left_join(
        orders,
        users,
        on="user_id",
        validate="many_to_one",
        suffixes=("", "_user"),
    )

    assert len(joined) == len(orders), "Row count changed (join explosion?)"

    joined = joined.assign(amount_winsor=winsorize(joined["amount"]))
    joined = add_outlier_flag(joined, "amount", k=1.5)

    return joined


def load_outputs(analytics: pd.DataFrame, users: pd.DataFrame, cfg: ETLConfig) -> None:
    cfg.out_analytics.parent.mkdir(parents=True, exist_ok=True)

    
    write_parquet(users, cfg.out_users)
    write_parquet(analytics, cfg.out_analytics)

    
    write_parquet(analytics, cfg.out_orders_clean)


def write_run_meta(cfg: ETLConfig, *, analytics: pd.DataFrame) -> None:
    missing_created_at = int(analytics["created_at"].isna().sum()) if "created_at" in analytics.columns else 0
    country_match_rate = 1.0 - float(analytics["country"].isna().mean()) if "country" in analytics.columns else 0.0

    meta = {
        "rows_out": int(len(analytics)),
        "missing_created_at": missing_created_at,
        "country_match_rate": country_match_rate,
        "config": {k: str(v) for k, v in asdict(cfg).items()},
    }

    cfg.run_meta.parent.mkdir(parents=True, exist_ok=True)
    cfg.run_meta.write_text(json.dumps(meta, indent=2), encoding="utf-8")


def run_etl(cfg: ETLConfig) -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s %(name)s: %(message)s",
    )

    log.info("Loading inputs")
    orders_raw, users = load_inputs(cfg)

    log.info("Transforming (orders=%s, users=%s)", len(orders_raw), len(users))
    analytics = transform(orders_raw, users)

    log.info("Writing outputs to %s", cfg.out_analytics.parent)
    load_outputs(analytics, users, cfg)

    log.info("Writing run metadata: %s", cfg.run_meta)
    write_run_meta(cfg, analytics=analytics)





