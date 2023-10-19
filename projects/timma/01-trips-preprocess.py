"""For CHAMP trip summaries for TIMMA

Parses output from CHAMP version: CHAMP 5.2.0-Toll_Quintile
"""
from pathlib import Path

import pandas as pd
import polars as pl


def calc_income_quintiles(trips_df, income_quintiles):
    """calculate household income quintiles (TI project definitions)"""
    income_quintile_labels = list(map(str, range(1, 6)))
    cut_expression = lambda hhsize: (pl.col("hhinc") * 1000).cut(
        income_quintiles.loc[hhsize].to_numpy(), labels=income_quintile_labels
    )
    return trips_df.with_columns(
        pl.when(pl.col("hhsize") == 1)
        .then(cut_expression(1))
        .when(pl.col("hhsize") == 2)
        .then(cut_expression(2))
        .when(pl.col("hhsize") == 3)
        .then(cut_expression(3))
        .when(pl.col("hhsize") == 4)
        .then(cut_expression(4))
        .when(pl.col("hhsize") == 5)
        .then(cut_expression(5))
        .when(pl.col("hhsize") == 6)
        .then(cut_expression(6))
        .when(pl.col("hhsize") == 7)
        .then(cut_expression(7))
        .when(pl.col("hhsize") == 8)
        .then(cut_expression(8))
        .when(pl.col("hhsize") == 9)
        .then(cut_expression(9))
        .when(pl.col("hhsize") == 10)
        .then(cut_expression(10))
        .when(pl.col("hhsize") >= 11)
        .then(cut_expression(11))
        .alias("income_quintile")
        .cast(pl.Categorical)
    )


def group_tazs(trips_df):
    """Use TAZs & hhidbase to determine origin/destination/residency

    output columns: origin, destination, (homezone,) residency
    column values: TI[-legacy/new], SF, exSF
    """
    taz_in_ti_predicate = lambda col: (
        (865 <= pl.col(col)) & (pl.col(col) <= 872)
    )
    taz_in_sf_predicate = lambda col: pl.col(col) <= 981
    taz_conditional = lambda col: (
        pl.when(taz_in_ti_predicate(col))
        .then(pl.lit("TI"))
        .when(taz_in_sf_predicate(col))
        .then(pl.lit("SF-exTI"))
        .otherwise(pl.lit("exSF"))
    ).cast(pl.Categorical)
    ti_residency_conditional = lambda col: (
        pl.when(pl.col(col) == "TI")
        .then(
            # legacy residents: hhidbase != 0
            pl.when(pl.col("hhidbase").cast(bool))
            .then(pl.lit("TI-legacy"))
            .otherwise(pl.lit("TI-new"))
        )
        .otherwise(pl.col(col))
    ).cast(pl.Categorical)

    return trips_df.with_columns(
        taz_conditional("mOtaz").alias("origin"),
        taz_conditional("mDtaz").alias("destination"),
        taz_conditional("homestaz").alias("home_zone"),
    ).with_columns(ti_residency_conditional("home_zone").alias("residency"))


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
    output_dir = Path(dir) / "summaries"
    trips_filename = "TRIPMC.H51"
    persons_filename = "sfsamp.txt"
    trips_simplified_filename = "TRIPMC1-simplified.parquet"  # output

    trips_filepath = Path(dir, trips_filename)
    persons_filepath = Path(dir, persons_filename)
    output_dir.mkdir(exist_ok=True)
    trips_simplified_filepath = Path(
        output_dir, trips_simplified_filename
    )  # output

    # to save memory, we don't actually need all of these columns
    sfsamp_headers = [
        "hhid",
        "persid",
        "sfzone",
        "hhsize",
        "hhadlt",
        "hh65up",
        "hh5064",
        "hh3549",
        "hh2534",
        "hh1824",
        "hh1217",
        "hhc511",
        "hhchu5",
        "hhfull",
        "hhpart",
        "hhvehs",
        "hhinc",
        "gender",
        "age",
        "relat",
        "race",
        "employ",
        "educn",
        "hhidbase",
        "persidbase",
        "ubieligible",
    ]
    tripmc_headers = [
        "hhid",
        "persid",
        "homestaz",
        "hhsize",
        "hhadlt",
        "nage65up",
        "nage5064",
        "nage3549",
        "nage2534",
        "nage1824",
        "nage1217",
        "nage511",
        "nageund5",
        "nfulltim",
        "nparttim",
        "autos",
        "hhinc",
        "gender",
        "age",
        "relat",
        "race",
        "employ",
        "educn",
        "exflag",
        "worksTwoJobs",
        "worksOutOfArea",
        "mVOT",
        "oVOT",
        "randseed",
        "workstaz",
        "paysToPark",
        "mcLogSumW0",
        "mcLogSumW1",
        "mcLogSumW2",
        "mcLogSumW3",
        "mcLogSumW",
        "dcLogSumPk",
        "dcLogSumOp",
        "dcLogSumAtWk",
        "pseg",
        "tour",
        "daypattern",
        "purpose",
        "ctprim",
        "StopBefTime1",
        "StopBefTime2",
        "StopBefTime3",
        "StopBefTime4",
        "StopAftTime1",
        "StopAftTime2",
        "StopAftTime3",
        "StopAftTime4",
        "tourmode",
        "autoExpUlitity",
        "walkTransitAvailable",
        "walkTransitProb",
        "driveTransitOnly",
        "driveTransitProb",
        "stopb1",
        "stopb2",
        "stopb3",
        "stopb4",
        "stopa1",
        "stopa2",
        "stopa3",
        "stopa4",
        "mcurrseg",
        "mOtaz",
        "mDtaz",
        "odt",
        "mChosenmode",
        "mNonMot",
        "mExpAuto",
        "mWlkAvail",
        "mWlkTrnProb",
        "mDriveOnly",
        "mDriveTrnProb",
        "mSegDir",
        "currsegdur",
        "trippkcst",
        "prefTripTOF",
        "tripTOD",
    ]
    persons_dtypes = {"hhid": pl.Int32, "persid": pl.Int32}

    persons = pl.read_csv(
        persons_filepath,
        separator=" ",
        has_header=False,
        new_columns=sfsamp_headers,
        dtypes=persons_dtypes,
    )
    income_quintiles = pd.read_csv("income_quintiles.csv", index_col="hhsize")
    trips = pl.from_pandas(
        pd.read_hdf(trips_filepath, "records"), include_index=False
    ).join(persons, on=["hhid", "persid"], how="left")
    trips = group_mode(
        group_purpose(
            group_tazs(calc_income_quintiles(trips, income_quintiles))
        )
    )

    # trips.select((pl.col("homestaz") == pl.col("sfzone")).all())

    trips_subset = trips.select(
        "origin",
        "destination",
        "residency",
        "income_quintile",
        "purpose",
        "trip_mode",
    )
    trips_subset.write_parquet(trips_simplified_filepath)
