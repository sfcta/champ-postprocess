from pathlib import Path

import geopandas as gpd
import matplotlib.pyplot as plt
import pandas as pd
from core import load_config, read_taz


def plot_map(gdf, var, out_filepath_stem):
    assert var in {"hh", "emp"}
    ax = gdf.plot(f"{var}-diff-CHAMP_-_MTC", legend=True)
    ax.set_xlim([5978000, 6026000])
    ax.set_ylim([2085000, 2132000])
    plt.savefig(f"{out_filepath_stem}-{var}.png")


def write_map_output(gdf, out_filepath_stem):
    gdf.to_file(f"{out_filepath_stem}.gpkg")
    plot_map(gdf, "hh", out_filepath_stem)
    plot_map(gdf, "emp", out_filepath_stem)


def taz_maps(
    champ_forecast_taz_filepath,
    mtc_taz_excel_filepath,
    mtc_taz_gis_filepath,
    out_dir,
    forecast_year,
):
    champ_forecast_taz = read_taz(
        champ_forecast_taz_filepath, usecols={"MTCTAZ", "HHLDS", "TOTALEMP"}
    )
    mtc_forecast_taz = pd.read_excel(
        mtc_taz_excel_filepath,
        sheet_name=str(forecast_year),
        usecols=["ZONE", "COUNTY", "TOTHH", "TOTEMP"],
    )
    out_filepath_stem = (
        Path(out_dir) / f"C-ForecastYearDemographics-taz-diff-{forecast_year}"
    )

    champ_forecast_taz = (
        champ_forecast_taz[champ_forecast_taz["COUNTY"] == 1]  # SF only
        .rename(columns={"HHLDS": "HH-CHAMP", "TOTALEMP": "EMP-CHAMP"})
        .drop(columns=["SFTAZ", "COUNTY", "SUPERDST"])
        .groupby("MTCTAZ")
        .sum()  # sum households and employment
    )

    mtc_forecast_taz = (
        mtc_forecast_taz[mtc_forecast_taz["COUNTY"] == 1]  # SF only
        .rename(
            columns={"ZONE": "MTCTAZ", "TOTHH": "HH-MTC", "TOTEMP": "EMP-MTC"}
        )
        .drop(columns=["COUNTY"])
        .set_index("MTCTAZ")
    )

    taz_comparison = champ_forecast_taz.join(mtc_forecast_taz, on="MTCTAZ")
    taz_comparison["hh-diff-CHAMP_-_MTC"] = (
        taz_comparison["HH-CHAMP"] - taz_comparison["HH-MTC"]
    )
    taz_comparison["emp-diff-CHAMP_-_MTC"] = (
        taz_comparison["EMP-CHAMP"] - taz_comparison["EMP-MTC"]
    )

    gdf = gpd.read_file(mtc_taz_gis_filepath)
    gdf = gdf[gdf["COUNTY"] == 1]  # SF only
    gdf = gdf.merge(taz_comparison, left_on="TAZ1454", right_on="MTCTAZ")

    write_map_output(gdf, out_filepath_stem)


if __name__ == "__main__":
    config = load_config()
    taz_maps(
        config["champ"]["forecast"]["taz_filepath"],
        config["mtc"]["taz_excel_filepath"],
        config["mtc"]["taz_gis_filepath"],
        config["out_dir"],
        config["forecast_year"],
    )
