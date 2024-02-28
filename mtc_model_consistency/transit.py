from pathlib import Path

import pandas as pd
import polars as pl
from core import load_config, time_period_conversion_champ_to_mtc, time_periods

# these 2 lists are for sorting via pandas reindex later
operators_mtc = [
    "AC Transit",
    "BART",
    "Caltrain",
    "Golden Gate Transit",
    "Golden Gate Transit",
    "SamTrans",
    "SF Muni",
    "SF Muni",
    "VTA",
    "VTA",
    "Other",
    "Other",
    "Other",
    "Other",
    "All",
]

techs_mtc = [
    "Bus",
    "Heavy Rail",
    "Commuter Rail",
    "Bus",
    "Ferry",
    "Bus",
    "Bus",
    "Light Rail",
    "Bus",
    "Light Rail",
    "Bus",
    "Ferry",
    "Light Rail",
    "Commuter Rail",
    "All",
]


def transit(out_dir):
    transit_data_dir = Path(out_dir) / "transit_assignment"
    out_csv_filepath = (
        transit_data_dir / "J-Traffic&TransitAssignment-Transit-2050.csv"
    )

    boardings = pl.from_pandas(
        # cast from pandas to polars df as join not working in pandas
        pd.read_csv(  # read as pandas df to use thousands arg
            transit_data_dir / "quickboards-transit_line_boardings.csv",
            thousands=",",
        ),
        # no need to cast as int as time period conversion later
        # would cast back these numbers back as float
        # schema_overrides={
        #     "Daily": int,
        #     "AM": int,
        #     "MD": int,
        #     "PM": int,
        #     "EV": int,
        #     "EA": int,
        # },
    ).join(
        pl.read_csv(
            transit_data_dir / "transit_lines-operator-tech.csv",
            columns=["Line Name", "Operator", "Technology"],
        ),
        on="Line Name",
    )
    boardings = (
        time_period_conversion_champ_to_mtc(
            boardings.fill_null(strategy="zero")
            .group_by(["Operator", "Technology"])
            .sum()
            .rename({t: f"CHAMP-{t}" for t in time_periods})
        )
        .select(
            ["Operator", "Technology"] + [f"MTC-{t}" for t in time_periods]
        )
        .sort("Operator", "Technology")
    )
    boardings = (
        pl.concat(
            (
                boardings,
                boardings.sum().with_columns(
                    pl.lit("All").alias("Operator"),
                    pl.lit("All").alias("Technology"),
                ),
            )
            # cast back to pandas to do manual sorting via reindex
        )
        .to_pandas()
        .set_index(["Operator", "Technology"])
        .reindex(pd.MultiIndex.from_arrays((operators_mtc, techs_mtc)))
        .to_csv(out_csv_filepath)
    )


if __name__ == "__main__":
    config = load_config()
    transit(config["out_dir"])
