from pathlib import Path

from ingestion.common.logger import get_logger
from ingestion.elia_load.load import load_ods001
from ingestion.elia_load.validate import validate_csv

PROJECT_ROOT = Path(__file__).resolve().parents[2]
ODS_PATH = PROJECT_ROOT / "data" / "raw" / "ods001.csv"
logger = get_logger(__name__)


def run_initial_load(path: Path):
    logger.info("Starting initial load")

    rows = validate_csv(path)
    load_ods001(rows)

    logger.info("Initial load completed")


if __name__ == "__main__":
    run_initial_load(ODS_PATH)
