import argparse
import tomllib
from pathlib import Path

import pandas as pd


def auto_ownership(model_run_dir, taz_filepath, out_dir, forecast_year):
    hh = pd.read_csv(
        Path(model_run_dir) / "daysim" / "abm_output1" / "_household_2.dat",
        sep=r"\s+",
    )
    taz = pd.read_csv(taz_filepath)
    hh = pd.merge(
        hh,
        taz.loc[:, ["SFTAZ", "COUNTY", "SUPERDST"]],
        left_on="hhtaz",
        right_on="SFTAZ",
    )

    out_dir = Path(out_dir)
    out_bycounty_filepath = (
        out_dir / f"F-ForecastAutoOwnership-county-{forecast_year}.csv"
    )
    out_bysuperdst_filepath = (
        out_dir / f"F-ForecastAutoOwnership-superdst-{forecast_year}.csv"
    )

    auto_ownership = hh.pivot_table(
        index=["COUNTY"], columns="hhvehs", aggfunc="size"
    ).to_csv(out_bycounty_filepath)
    auto_ownership = hh.pivot_table(
        index=["SUPERDST"], columns="hhvehs", aggfunc="size"
    ).to_csv(out_bysuperdst_filepath)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("config_filename")
    args = parser.parse_args()
    with open(
        Path(__file__).parent.resolve() / "../configs" / args.config_filename,
        "rb",
    ) as f:
        config = tomllib.load(f)
    auto_ownership(
        config["model_run_dir"],
        config["taz_filepath"],
        config["out_dir"],
        config["forecast_year"],
    )
