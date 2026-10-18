from pathlib import Path

import pandas as pd

DATA_PATH = Path("data/raw/ods001.csv")


def profile_timestamps(df: pd.DataFrame) -> None:
    timestamps = pd.to_datetime(
        df["Datetime"],
        utc=True,
        errors="coerce",
    )

    print("\n=== Temporal profiling ===")

    print(f"Invalid timestamps: {timestamps.isna().sum()}")
    print(f"Start (UTC): {timestamps.min()}")
    print(f"End (UTC): {timestamps.max()}")

    print(f"Duplicate timestamps: {timestamps.duplicated().sum()}")

    sorted_timestamps = timestamps.dropna().sort_values()
    intervals = sorted_timestamps.diff().dropna()

    print("\nInterval distribution:")
    print(intervals.value_counts().head(10))

    expected_interval = pd.Timedelta(minutes=15)

    unexpected = (intervals != expected_interval).sum()
    print(f"\nUnexpected intervals: {unexpected}")


def main() -> None:
    df = pd.read_csv(DATA_PATH, sep=";")

    print("=== ODS001 Data Profiling ===")

    print(f"\nRows: {len(df):,}")
    print(f"Columns: {len(df.columns)}")

    print("\nColumn names:")
    for column in df.columns:
        print(f"  - {column}")

    print("\nData types:")
    print(df.dtypes)

    print("\nMissing values:")
    print(df.isna().sum())

    print("\nSample:")
    print(df.head())

    profile_timestamps(df)


if __name__ == "__main__":
    main()
