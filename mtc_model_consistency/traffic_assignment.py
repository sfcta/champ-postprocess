"""
First create the LOAD{XX}_FINAL.csv files with Y:/champ/util/Validation/NETtoCSV_simple.s
(see champ-postprocess/cube/csv/README.md for instructions)
"""

from pathlib import Path

import pandas as pd
from core import load_config, time_periods

facility_type_champ = {
    1: "Ramp",
    2: "Freeways",
    3: "Expressways",
    4: "Collectors",
    5: "Ramp",
    7: "Major arterials",
    11: "Local",
    12: "Minor arterials",
    15: "Major arterials",
}

facility_type_champ_to_mtc = {
    1: "Others",
    2: "Freeways",
    3: "Expressways",
    4: "Collectors",
    5: "Others",
    7: "Major arterials",
    11: "Others",
    12: "Others",
    15: "Major arterials",
}

facility_types_mtc = [
    # "Managed Freeways",  # no easy way to get VHT & VMT of all toll/express lanes in CHAMP
    "Freeways",
    "Expressways",
    "Major arterials",
    "Collectors",
    "Others",
    "All Facilities",
]


def load_VMT_and_VHT(model_run_dir, time_period):
    loaded_network = pd.read_csv(
        model_run_dir / f"LOAD{time_period}_FINAL.csv",
        usecols=["FT", "VDT_1", "VHT_1"],
    )
    loaded_network["FT"] = loaded_network["FT"].map(facility_type_champ_to_mtc)
    VMT_and_VHT_by_FT = (
        loaded_network.groupby("FT")
        .sum()
        .rename(columns={"VDT_1": "VMT", "VHT_1": "VHT"})
    )
    VMT_and_VHT_by_FT.loc["All Facilities"] = VMT_and_VHT_by_FT.sum()
    return VMT_and_VHT_by_FT


def load_VMT_and_VHT_all_time_periods(model_run_dir):
    return {t: load_VMT_and_VHT(model_run_dir, t) for t in time_periods}


def time_period_conversion_champ_to_mtc(champ_timeperiods_dict):
    """
    Convert from 3hr (CHAMP) to 4hr (MTC) peaks while maintaining totals.
    The conversion factors have been used since 2019, and are derived from PeMS
    See Q:\MTC\Model\ConsistencyReports\2021\Analysis\12.HighwayAssignment.xlsx
    """
    mtc_timeperiods_dict = {
        "EA": champ_timeperiods_dict["EA"] * 0.75178,  # 0300-0600
        "AM": (
            champ_timeperiods_dict["EA"] * 0.24822  # 0600-0630
            + champ_timeperiods_dict["AM"]  # 0630-0930
            + champ_timeperiods_dict["MD"] * 0.09532  # 0930-1000
        ),
        "MD": champ_timeperiods_dict["MD"] * 0.81584,  # 1000-1500
        "PM": (
            champ_timeperiods_dict["MD"] * 0.08884  # 1500-1530
            + champ_timeperiods_dict["PM"]  # 1530-1830
            + champ_timeperiods_dict["EV"] * 0.11220  # 1830-1900
        ),
        "EV": champ_timeperiods_dict["EV"] * 0.88780,  # 1900-0300
    }
    return mtc_timeperiods_dict


def traffic_assignment(model_run_dir, out_dir):
    mtc_timeperiods_dict = time_period_conversion_champ_to_mtc(
        load_VMT_and_VHT_all_time_periods(model_run_dir)
    )
    dfs = []
    for t, df in mtc_timeperiods_dict.items():
        df["timeperiod"] = t
        dfs.append(df)
    df = pd.concat(dfs)
    alltime_df = df.groupby("FT").sum()
    alltime_df["timeperiod"] = "All Time"
    df = pd.concat((df, alltime_df))
    df["avg_speed"] = df["VMT"] / df["VHT"]
    df = df.reset_index().melt(
        id_vars=["FT", "timeperiod"], value_vars=["VMT", "VHT", "avg_speed"]
    )
    df.pivot_table(
        columns="FT", index=["variable", "timeperiod"], values="value"
    ).reindex(
        pd.MultiIndex.from_product(
            (["VMT", "VHT", "avg_speed"], time_periods + ["All Time"])
        )
    )[
        facility_types_mtc
    ].to_csv(
        out_dir / "J-Traffic&TransitAssignment-VMTVHTSpeed-2050.csv"
    )


if __name__ == "__main__":
    config = load_config()
    traffic_assignment(
        config["champ"]["forecast"]["model_run_dir"],
        config["out_dir"],
    )
