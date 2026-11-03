from pathlib import Path

from prefect import flow, get_run_logger, task

PROJECT_ROOT = Path(__file__).resolve().parents[1]
INBOX = PROJECT_ROOT / "data" / "inbox"


@task
def find_csv_files() -> list[Path]:
    return sorted(INBOX.glob("*.csv"))


@flow(name="energy-inbox-check")
def check_inbox() -> None:
    logger = get_run_logger()
    files = find_csv_files()

    if not files:
        logger.info("Aucun fichier CSV en attente.")
    else:
        for file in files:
            logger.info("Fichier détecté : %s", file.name)


if __name__ == "__main__":
    check_inbox()
