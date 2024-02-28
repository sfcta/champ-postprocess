import argparse
import tomllib
from pathlib import Path

import pandas as pd
import polars as pl

time_periods = ["EA", "AM", "MD", "PM", "EV"]


def load_config():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "config_filename",
        help=(
            "config filename in "
            "champ-postprocess/mtc_model_consistency/configs/"
        ),
    )
    args = parser.parse_args()
    with open(
        Path(__file__).parent.resolve() / "configs" / args.config_filename,
        "rb",
    ) as f:
        return tomllib.load(f)


def read_taz(taz_filepath, usecols=None):
    if usecols:
        usecols = {"SFTAZ", "COUNTY", "SUPERDST"} | set(usecols)
    else:
        usecols = {"SFTAZ", "COUNTY", "SUPERDST"}
    return pd.read_csv(taz_filepath, usecols=usecols)


def _read_model_output_dat(model_run_dir, filename, usecols=None):
    return pd.read_csv(
        Path(model_run_dir) / "daysim" / "abm_output1" / filename,
        sep=r"\s+",
        usecols=usecols,
    )


def read_hh(model_run_dir, usecols=None):
    return _read_model_output_dat(
        model_run_dir, "_household_2.dat", usecols=usecols
    )


def read_pers(model_run_dir, usecols=None):
    return _read_model_output_dat(
        model_run_dir, "_person_2.dat", usecols=usecols
    )


def read_tours(model_run_dir, usecols=None):
    return _read_model_output_dat(
        model_run_dir, "_tour_2.dat", usecols=usecols
    )


def read_trips(model_run_dir, usecols=None):
    return _read_model_output_dat(
        model_run_dir, "_trip_2.dat", usecols=usecols
    )


def read_hh_pers(model_run_dir, hh_usecols=None, pers_usecols=None):
    if hh_usecols:
        hh_usecols = {"hhno"} | set(hh_usecols)
    else:
        hh_usecols = {"hhno"}
    if pers_usecols:
        pers_usecols = {"hhno", "pno"} | set(pers_usecols)
    else:
        pers_usecols = {"hhno", "pno"}
    hh = read_hh(model_run_dir, usecols=hh_usecols)
    pers = read_pers(model_run_dir, usecols=pers_usecols)
    return pd.merge(hh, pers, on=["hhno"])


def read_tours_pers(
    model_run_dir,
    tours_usecols=None,
    pers_usecols=None,
):
    if tours_usecols:
        tours_usecols = {"pno"} | set(tours_usecols)
    else:
        tours_usecols = {"pno"}
    if pers_usecols:
        pers_usecols = {"pno"} | set(pers_usecols)
    else:
        pers_usecols = {"pno"}
    tours = read_tours(model_run_dir, usecols=tours_usecols)
    pers = read_pers(model_run_dir, usecols=pers_usecols)
    return pd.merge(pers, tours, on=["pno"])


def read_tours_hh_pers(
    model_run_dir,
    tours_usecols=None,
    hh_usecols=None,
    pers_usecols=None,
):
    if tours_usecols:
        tours_usecols = {"hhno", "pno"} | set(tours_usecols)
    else:
        tours_usecols = {"hhno", "pno"}
    tours = read_tours(model_run_dir, usecols=tours_usecols)
    hh_pers = read_hh_pers(
        model_run_dir, hh_usecols=hh_usecols, pers_usecols=pers_usecols
    )
    return pd.merge(hh_pers, tours, on=["hhno", "pno"])


def merge_home_geog(hh, taz):
    """
    hh: can be any DataFrame with the hhtaz column from the household file
    taz: output of read_taz()
    """
    return pd.merge(
        hh.rename(columns={"hhtaz": "home_taz"}),
        taz.rename(
            columns={
                "SFTAZ": "home_taz",
                "COUNTY": "home_county",
                "SUPERDST": "home_superdst",
            }
        ),
        on="home_taz",
    )


def merge_work_geog(pers, taz):
    """
    pers: can be any DataFrame with the pwtaz column from the household file
    taz: output of read_taz()
    """
    return pd.merge(
        pers.rename(columns={"pwtaz": "work_taz"}),
        taz.rename(
            columns={
                "SFTAZ": "work_taz",
                "COUNTY": "work_county",
                "SUPERDST": "work_superdst",
            }
        ),
        on="work_taz",
    )


def merge_home_and_work_geog(hh_pers, taz):
    return merge_work_geog(merge_home_geog(hh_pers, taz), taz)


def read_hh_with_home_geog(model_run_dir, taz_filepath, hh_usecols=None):
    if hh_usecols:
        hh_usecols = {"hhno", "hhtaz"} | set(hh_usecols)
    else:
        hh_usecols = {"hhno", "hhtaz"}
    hh = read_hh(model_run_dir, usecols=hh_usecols)
    taz = read_taz(taz_filepath)
    return merge_home_geog(hh, taz)


def read_pers_with_home_geog(
    model_run_dir, taz_filepath, hh_usecols=None, pers_usecols=None
):
    if pers_usecols:
        pers_usecols = {"hhno", "pno"} | set(pers_usecols)
    else:
        pers_usecols = {"hhno", "pno"}
    hh = read_hh_with_home_geog(
        model_run_dir, taz_filepath, hh_usecols=hh_usecols
    )
    pers = read_pers(model_run_dir, usecols=pers_usecols)
    return pd.merge(hh, pers, on=["hhno"])


def read_tours_with_home_geog(
    model_run_dir,
    taz_filepath,
    hh_usecols=None,
    pers_usecols=None,
    tours_usecols=None,
):
    if tours_usecols:
        tours_usecols = {"hhno", "pno"} | set(tours_usecols)
    else:
        tours_usecols = {"hhno", "pno"}
    pers = read_pers_with_home_geog(
        model_run_dir,
        taz_filepath,
        hh_usecols=hh_usecols,
        pers_usecols=pers_usecols,
    )
    tours = read_tours(model_run_dir, usecols=tours_usecols)
    return pd.merge(pers, tours, on=["hhno", "pno"])


def time_period_conversion_champ_to_mtc(df):
    """
    Convert from 3hr (CHAMP) to 4hr (MTC) peaks while maintaining totals.
    The conversion factors have been used since 2019, and are derived from PeMS
    See Q:\MTC\Model\ConsistencyReports\2021\Analysis\12.HighwayAssignment.xlsx
    """
    return df.with_columns(
        (pl.col("CHAMP-EA") * 0.75178).alias("MTC-EA"),  # 0300-0600
        (
            pl.col("CHAMP-EA") * 0.24822  # 0600-0630
            + pl.col("CHAMP-AM")  # 0630-0930
            + pl.col("CHAMP-MD") * 0.09532  # 0930-1000
        ).alias("MTC-AM"),
        (pl.col("CHAMP-MD") * 0.81584).alias("MTC-MD"),  # 1000-1500
        (
            pl.col("CHAMP-MD") * 0.08884  # 1500-1530
            + pl.col("CHAMP-PM")  # 1530-1830
            + pl.col("CHAMP-EV") * 0.11220  # 1830-1900
        ).alias("MTC-PM"),
        (pl.col("CHAMP-EV") * 0.88780).alias("MTC-EV"),  # 1900-0300
    )
