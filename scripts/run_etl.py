
from pathlib import Path

from bootcamp_data.etl import ETLConfig, run_etl


def main() -> None:
   
    ROOT = Path(__file__).resolve().parents[1]

    cfg = ETLConfig.from_root(ROOT)
    run_etl(cfg)


if __name__ == "__main__":
    main()
