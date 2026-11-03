"""Poll and classify one local CSV per run; all file lifecycle stays in Prefect."""

import json
import msvcrt
import os
import time
from contextlib import contextmanager
from pathlib import Path
from uuid import uuid4

from prefect import flow
from src.prefect.pipeline import PROJECT_ROOT, PipelineExecutionError, energy_pipeline

DIRECTORIES = ("inbox", "processing", "archive", "pending_approval", "failed")
DESTINATIONS = {
    "SUCCESS": "archive",
    "NO_CHANGE": "archive",
    "AWAITING_APPROVAL": "pending_approval",
    "DBT_FAILED": "failed",
}
MIN_AGE_SECONDS = 60
STABILITY_SECONDS = 1


@contextmanager
def inbox_lock(path: Path):
    """One local inbox run at a time; Windows releases the lock on process exit."""
    with path.open("a+b") as stream:
        if stream.tell() == 0:
            stream.write(b"\0")
            stream.flush()
        stream.seek(0)
        try:
            msvcrt.locking(stream.fileno(), msvcrt.LK_NBLCK, 1)
        except OSError as error:
            if error.errno != 13:
                raise
            yield False
            return
        try:
            yield True
        finally:
            stream.seek(0)
            msvcrt.locking(stream.fileno(), msvcrt.LK_UNLCK, 1)


def move_csv(source: Path, directory: Path) -> Path:
    """Rename on the same Windows volume, atomically and without replacement.

    Windows rename refuses an existing destination, including one created after
    our existence check. A losing claimant sees FileNotFoundError. Do not replace
    this with shutil.move/os.replace (which can overwrite). POSIX rename has
    different semantics; this local Windows implementation deliberately refuses it.
    """
    if os.name != "nt":
        raise RuntimeError("La prise en charge atomique nécessite Windows")
    target = directory / source.name
    while True:
        if target.exists():
            target = directory / f"{source.stem}__{uuid4().hex}{source.suffix}"
        try:
            return source.rename(target)
        except FileExistsError:
            target = directory / f"{source.stem}__{uuid4().hex}{source.suffix}"


def claim_csv(data_root: Path) -> Path | None:
    """Take the first ready CSV in filename order; competing runs may claim once."""
    candidates = sorted(
        (data_root / "inbox").glob("*.csv"),
        key=lambda path: (path.name.casefold(), path.name),
    )
    for source in candidates:
        if source.name.startswith((".", "~")) or any(
            part in {"tmp", "temp", "part", "partial", "crdownload"}
            for part in source.name.lower().split(".")[:-1]
        ):
            continue
        try:
            if not source.is_file() or source.is_symlink():
                continue
            before = source.stat()
            if before.st_size == 0 or time.time() - before.st_mtime < MIN_AGE_SECONDS:
                continue
            time.sleep(STABILITY_SECONDS)
            after = source.stat()
            if (before.st_size, before.st_mtime_ns) != (
                after.st_size,
                after.st_mtime_ns,
            ):
                continue
            return move_csv(source, data_root / "processing")
        except FileNotFoundError:
            # Another invocation has already moved this candidate.
            continue
        except PermissionError:
            # Windows may deny rename while a producer still holds the file open.
            print(f"Fichier occupé ou inaccessible, laissé dans inbox : {source.name}")
    return None


def classify_csv(source: Path, data_root: Path, result: dict) -> dict:
    destination = DESTINATIONS.get(result["status"], "failed")
    target = move_csv(source, data_root / destination)
    report = {**result, "classified_path": str(target)}
    # Exclusive creation also protects diagnostics left by an earlier manual move.
    diagnostic = target.with_name(target.name + ".result.json")
    while True:
        try:
            with diagnostic.open("x", encoding="utf-8") as stream:
                json.dump(report, stream, ensure_ascii=False, indent=2)
                stream.write("\n")
            break
        except FileExistsError:
            diagnostic = target.with_name(target.name + f".{uuid4().hex}.result.json")
    print(f"CSV classé : {target} ; diagnostic : {diagnostic}")
    return report


@flow(name="energy-inbox", log_prints=True, retries=0, persist_result=False)
def energy_inbox() -> dict:
    data_root = PROJECT_ROOT / "data"
    for name in DIRECTORIES:
        (data_root / name).mkdir(parents=True, exist_ok=True)
    with inbox_lock(data_root / "processing" / ".inbox.lock") as acquired:
        if not acquired:
            print("Une autre exécution locale surveille déjà cette inbox.")
            return {"status": "BUSY"}
        source = claim_csv(data_root)
        if source is None:
            print("Aucun CSV prêt dans data/inbox/.")
            return {"status": "EMPTY"}
        print(f"CSV pris en charge : {source}")
        try:
            # A direct flow call creates a subflow and waits for its terminal result.
            result = energy_pipeline(str(source))
        except Exception as error:
            result = (
                error.result
                if isinstance(error, PipelineExecutionError)
                else {"status": "TECHNICAL_ERROR", "error_type": type(error).__name__}
            )
            classify_csv(source, data_root, result)
            raise
        # Interruptions deliberately leave the CSV in processing for manual recovery.
        return classify_csv(source, data_root, result)


if __name__ == "__main__":
    energy_inbox()
