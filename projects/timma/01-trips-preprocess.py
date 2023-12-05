"""For CHAMP trip summaries for TIMMA

Parses output from CHAMP version: CHAMP 5.2.0-Toll_Quintile
"""
from pathlib import Path

import pandas as pd
import polars as pl

from timma import load_population, read_income_quintiles, group_od_tazs


def group_purpose(trips_df):
    purpose_conditional = (
        (
            pl.when(pl.col("purpose") == 1)
            .then(pl.lit("work"))
            .when((1 < pl.col("purpose")) & (pl.col("purpose") < 5))
            .then(pl.lit("school"))
            .when(pl.col("purpose") == 6)
            .then(pl.lit("workbased"))
            .otherwise(pl.lit("other"))
        )
        .cast(pl.Categorical)
        .alias("purpose")
    )
    return trips_df.with_columns(purpose_conditional)


def group_mode(trips_df):
    # grouping the trip mode, not tour mode
    trip_mode_col = pl.col("mChosenmode")
    purpose_conditional = (
        (
            pl.when(trip_mode_col.is_in([1, 4, 7]))
            .then(pl.lit("auto-DA"))
            .when(trip_mode_col.is_in([2, 5, 8, 22]))  # includes 22 (TAXI)
            .then(pl.lit("auto-SR2"))
            .when(trip_mode_col.is_in([3, 6, 9]))
            .then(pl.lit("auto-SR3"))
            .when(trip_mode_col == 10)
            .then(pl.lit("walk"))
            .when(trip_mode_col == 11)
            .then(pl.lit("bike"))
            .when((11 < trip_mode_col) & (trip_mode_col < 22))
            .then(pl.lit("transit"))
        )
        .cast(pl.Categorical)
        .alias("trip_mode")
    )
    return trips_df.with_columns(purpose_conditional)


if __name__ == "__main__":
    dir = r"X:\Projects\TIMMA\Round7\Round7_2040_weekday_ubi_currentandlowincomeresidents"
    output_dir = Path(dir) / "summaries"
    trips_filename = "TRIPMC.H51"
    trips_simplified_filename = "TRIPMC1-simplified.parquet"  # output

    trips_filepath = Path(dir, trips_filename)
    output_dir.mkdir(exist_ok=True)
    trips_simplified_filepath = Path(
        output_dir, trips_simplified_filename
    )  # output

    persons = load_population(dir)
    income_quintiles = read_income_quintiles()
    trips = pl.from_pandas(
        pd.read_hdf(trips_filepath, "records"), include_index=False
    ).join(persons, on=["hhid", "persid"], how="left")
    trips = group_mode(
        group_purpose(
            group_od_tazs(trips))
        )

    # trips.select((pl.col("homestaz") == pl.col("sfzone")).all())

    trips_subset = trips.select(
        "origin",
        "destination",
        "residency",
        "income_quintile",
        "purpose",
        "trip_mode",
        "mOdt"
    )
    trips_subset.write_parquet(trips_simplified_filepath)
