import pandas as pd

from config import CSV_PATH


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


def profile_quality(df: pd.DataFrame) -> None:
    print("\n=== Data quality profiling ===")

    numeric_columns = df.select_dtypes(include="number").columns

    print("\nNumeric statistics:")
    print(df[numeric_columns].describe().T)

    print("\nMissing values by column:")
    print(df.isna().sum())

    print("\nNegative values:")
    print((df[numeric_columns] < 0).sum())

    forecast_groups = [
        (
            "Most recent",
            "Most recent P10",
            "Most recent forecast",
            "Most recent P90",
        ),
        (
            "Day-ahead",
            "Day-ahead 6PM P10",
            "Day-ahead 6PM forecast",
            "Day-ahead 6PM P90",
        ),
    ]

    print("\nForecast interval consistency:")

    for name, p10, forecast, p90 in forecast_groups:
        invalid = (df[p10] > df[forecast]) | (df[forecast] > df[p90])

        print(f"{name}: {invalid.sum()} anomalies")

        if invalid.any():
            print(
                df.loc[
                    invalid,
                    ["Datetime", p10, forecast, p90],
                ]
                .head(10)
                .to_string(index=False)
            )


def profile_missing_periods(df: pd.DataFrame) -> None:
    timestamps = pd.to_datetime(df["Datetime"], utc=True)

    print("\n=== Missing values by year ===")

    for column in ["Total Load", "Week-ahead forecast"]:
        missing = df[column].isna()

        counts = (
            pd.DataFrame(
                {
                    "year": timestamps.dt.year,
                    "missing": missing,
                }
            )
            .groupby("year")["missing"]
            .sum()
        )

        print(f"\n{column}:")
        print(counts[counts > 0])

        missing_dates = timestamps[missing].sort_values()

        if not missing_dates.empty:
            print(f"First missing: {missing_dates.iloc[0]}")
            print(f"Last missing:  {missing_dates.iloc[-1]}")


def main() -> None:
    df = pd.read_csv(CSV_PATH, sep=";")

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
    profile_quality(df)
    profile_missing_periods(df)


if __name__ == "__main__":
    main()
