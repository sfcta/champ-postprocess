from pathlib import Path

from core import load_config, read_hh_with_home_geog


def auto_ownership(model_run_dir, taz_filepath, out_dir, forecast_year):
    hh = read_hh_with_home_geog(model_run_dir, taz_filepath, {"hhvehs"})

    out_dir = Path(out_dir)
    out_bycounty_filepath = (
        out_dir / f"F-ForecastAutoOwnership-county-{forecast_year}.csv"
    )
    out_bysuperdst_filepath = (
        out_dir / f"F-ForecastAutoOwnership-superdst-{forecast_year}.csv"
    )

    hh.pivot_table(index=["COUNTY"], columns="hhvehs", aggfunc="size").to_csv(
        out_bycounty_filepath
    )
    hh.pivot_table(
        index=["SUPERDST"], columns="hhvehs", aggfunc="size"
    ).to_csv(out_bysuperdst_filepath)


if __name__ == "__main__":
    config = load_config()
    auto_ownership(
        config["model_run_dir"],
        config["taz_filepath"],
        config["out_dir"],
        config["forecast_year"],
    )
