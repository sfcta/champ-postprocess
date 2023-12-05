"""Shared functions for TIMMA CHAMP summaries"""
from pathlib import Path

import pandas as pd
import polars as pl


def read_sfsamp(dir):
    persons_filename = "sfsamp.txt"
    persons_filepath = Path(dir, persons_filename)

    # to save memory: we don't actually need all of these columns
    # just load the ones that we need
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
    persons_dtypes = {"hhid": pl.Int32, "persid": pl.Int32}

    return pl.read_csv(
        persons_filepath,
        separator=" ",
        has_header=False,
        new_columns=sfsamp_headers,
        dtypes=persons_dtypes,
    )


def read_income_quintiles():
    return pd.read_csv("income_quintiles.csv", index_col="hhsize")


def calc_income_quintiles(df, income_quintiles):
    """calculate household income quintiles (TI project definitions)"""
    income_quintile_labels = list(map(str, range(1, 6)))
    cut_expression = lambda hhsize: (pl.col("hhinc") * 1000).cut(
        income_quintiles.loc[hhsize].to_numpy(), labels=income_quintile_labels
    )
    return df.with_columns(
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


taz_in_ti_predicate = lambda col: ((865 <= pl.col(col)) & (pl.col(col) <= 872))
taz_in_sf_predicate = lambda col: pl.col(col) <= 981
taz_conditional = lambda col: (
    pl.when(taz_in_ti_predicate(col))
    .then(pl.lit("TI"))
    .when(taz_in_sf_predicate(col))
    .then(pl.lit("SF-exTI"))
    .otherwise(pl.lit("exSF"))
).cast(pl.Categorical)


def group_od_tazs(trips_df):
    """Use TAZs & hhidbase to determine origin/destination

    output columns: (homezone,) residency
    column values: TI, SF, exSF
    """
    return trips_df.with_columns(
        taz_conditional("mOtaz").alias("origin"),
        taz_conditional("mDtaz").alias("destination"),
    )


def group_residency(df, col="sfzone"):
    """Use TAZs & hhidbase to determine residency

    output columns: origin, destination
    column values: TI-legacy/new, SF, exSF
    """
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
    return df.with_columns(
        taz_conditional(col).alias("home_zone")
    ).with_columns(ti_residency_conditional("home_zone").alias("residency"))


def load_population(dir):
    """read sfsamp (synthetic persons) then
    calculate income quintiles and residency status"""
    return group_residency(
        calc_income_quintiles(read_sfsamp(dir), read_income_quintiles())
    )


def crosstab_income_residency(df):
    return (
        # since we're doing a crosstab with agg func `count`,
        # the values field doesn't matter
        df.pivot(
            index="income_quintile",
            columns="residency",
            values="residency",
            aggregate_function="count",
        )
        .select(
            ["income_quintile", "TI-legacy", "TI-new", "SF-exTI", "exSF"]
        )  # reorder columns
        .sort("income_quintile")
    )
