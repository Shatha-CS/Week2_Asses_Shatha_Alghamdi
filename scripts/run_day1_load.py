from pathlib import Path
import sys
import logging

# allow importing from src/
ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from bootcamp_data.config import make_paths
from bootcamp_data.io import read_orders_csv, read_users_csv, write_parquet
from bootcamp_data.transforms import enforce_schema


logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
log = logging.getLogger(__name__)


def main() -> None:
    paths = make_paths(ROOT)

    orders = read_orders_csv(paths.raw / "orders.csv")
    users = read_users_csv(paths.raw / "users.csv")

    orders = enforce_schema(orders)

    log.info("Rows loaded: orders=%s users=%s", len(orders), len(users))
    log.info("Orders dtypes:\n%s", orders.dtypes)

    orders_out = paths.processed / "orders.parquet"
    users_out = paths.processed / "users.parquet"

    write_parquet(orders, orders_out)
    write_parquet(users, users_out)

    log.info("Wrote files:")
    log.info(" - %s", orders_out)
    log.info(" - %s", users_out)


if __name__ == "__main__":
    main()
