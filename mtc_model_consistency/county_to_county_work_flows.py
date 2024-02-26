from pathlib import Path

from core import (
    load_config,
    merge_home_and_work_geog,
    read_taz,
    read_tours_hh_pers,
)


def county_to_county_work_flows(
    model_run_dir, taz_filepath, out_dir, forecast_year
):
    out_dir = Path(out_dir)
    out_filepath = out_dir / (
        "H-ForecastActivityLocation-"
        f"journey_to_work_flows-{forecast_year}.csv"
    )

    taz = read_taz(taz_filepath)
    tours_hh_pers = read_tours_hh_pers(
        model_run_dir,
        tours_usecols=["tdtaz", "pdpurp"],
        hh_usecols=["hhtaz"],
        pers_usecols=["pwtaz"],
    )
    journey_to_work_tours = (
        tours_hh_pers[
            # tour purpose == work
            (tours_hh_pers["pdpurp"] == 1)
            # Disabled: tour destination TAZ == work location TAZ (of the person)
            # disabled because work tours do NOT necessarily have to end at the work TAZ in Daysim
            # & (tours_hh_pers["pwtaz"] == tours_hh_pers["tdtaz"])
        ]
        # only count once for people who makes multiple work tours to the work-TAZ a day
        .drop_duplicates()
    )
    merge_home_and_work_geog(journey_to_work_tours, taz).pivot_table(
        index="home_county", columns="work_county", aggfunc="size"
    ).to_csv(out_filepath)


if __name__ == "__main__":
    config = load_config()
    county_to_county_work_flows(
        config["champ"]["forecast"]["model_run_dir"],
        config["champ"]["forecast"]["taz_filepath"],
        config["out_dir"],
        config["forecast_year"],
    )
