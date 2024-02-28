from pathlib import Path

import pandas as pd
from core import load_config, read_tours_with_home_geog, read_trips

# source: Daysim2.1 Users Guide.xlsx
modes_champ = {
    0: "none",
    1: "walk",
    2: "bike",
    3: "sov",
    4: "hov 2",
    5: "hov 3",
    6: "walk to transit",
    7: "park and ride",
    8: "school bus",
    9: "other",
}

modes_champ_to_mtc = {
    1: "Walk",
    2: "Bicycle",
    3: "Drive Alone",
    4: "Shared Ride 2",
    5: "Shared Ride 3+",
    6: "Transit",
    # 7: # ignoring park and ride
    8: "Transit",
    9: "Taxi & Ride-Hailing",
}

modes_mtc = [
    "Drive Alone",
    "Shared Ride 2",
    "Shared Ride 3+",
    "Taxi & Ride-Hailing",
    "All Auto",
    "Transit",
    "Walk",
    "Bicycle",
    "All Modes",
]

# source: Daysim2.1 Users Guide.xlsx
purposes_champ = {
    0: "none/home",
    1: "work",
    2: "school",
    3: "escort",
    4: "pers.bus",
    5: "shop",
    6: "meal",
    7: "social",
    8: "recreational",  # (currently combined with social)
    9: "medical",  # (currently combined with pers.bus.)
    10: "change mode inserted purpose",
    # extra:
    11: "work-based",
    12: "college/university",
    13: "other school",
}

purposes_champ_to_mtc = {
    1: "Work",
    2: "School",
    3: "Escort",
    4: "Other",
    5: "Shopping",
    6: "Eat Out",
    7: "Social",
    11: "At-Work",
    12: "University",
    13: "School",
}

purposes_mtc = [
    "Work",
    "University",
    "School",
    "At-Work",
    "Eat Out",
    "Escort",
    "Shopping",
    "Social",
    "Other",
]

purposes_mtc_with_total = purposes_mtc + ["Total"]

# source: Daysim2.1 Users Guide.xlsx
person_type = {
    1: "Full time worker",
    2: "Part time worker",
    3: 'Non working adult age 6"5+',
    4: "Non working adult age<65",
    5: "University student",
    6: "High school student age 16+",
    7: "Child age 5-15",
    8: "Child age 0-4",
}


def parse_tours(tours):
    parents = tours.loc[tours["subtrs"] > 0]
    parents.set_index(["hhno", "pno", "tour"], inplace=True)
    tours.set_index(["hhno", "pno", "parent"], inplace=True)
    tours["parent_purp"] = parents["pdpurp"]
    tours.reset_index(inplace=True)

    # work-based subtours
    tours.loc[tours["parent_purp"] == 1, "pdpurp"] = 11
    # school purpose by person type
    tours.loc[(tours["pdpurp"] == 2) & (tours["pptyp"] == 5), "pdpurp"] = 12
    tours.loc[
        (tours["pdpurp"] == 2) & tours["pptyp"].isin([1, 2, 3, 4]), "pdpurp"
    ] = 13

    return tours


def calc_trips_and_dist_by_purp(trips, geog=None):
    """
    calculate, by purpose and by home county/superdistrict / full-Bay-Area,
    the number of trips and average trip distance
    """
    if geog:
        assert geog in {"home_county", "home_superdst"}
        grouped = trips.groupby([geog, "purpose"])
    else:
        grouped = trips.groupby("purpose")
    return (
        grouped.agg({"tour": "size", "travdist": "mean"})
        .rename(columns={"tour": "trips", "travdist": "average_trip_distance"})
        .reset_index()
    )


def trip_freq_pivot(trips_by_purpose, geog):
    assert geog in {"home_county", "home_superdst"}
    return trips_by_purpose.pivot(
        index=geog, columns="purpose", values="trips"
    )[purposes_mtc]


def calculate_trip_freq(trips, out_dir):
    # trips_by_purpose_bayarea = calc_trips_and_dist_by_purp(trips)
    trips_by_purpose_county = calc_trips_and_dist_by_purp(trips, "home_county")
    trips_by_purpose_superdst = calc_trips_and_dist_by_purp(
        trips, "home_superdst"
    )

    tripfreq_county = trip_freq_pivot(trips_by_purpose_county, "home_county")
    tripfreq_county.to_csv(
        out_dir / "G-ForecastActivityPattern-tripfreq_county.csv"
    )
    tripfreq_superdst = trip_freq_pivot(
        trips_by_purpose_superdst, "home_superdst"
    )
    tripfreq_superdst.to_csv(
        out_dir / "G-ForecastActivityPattern-tripfreq_superdst.csv"
    )


def calculate_trip_len(trips, out_dir):
    triplen = (
        trips.groupby("purpose")
        .agg({"travdist": "mean"})
        .rename(columns={"travdist": "avg_trip_dist"})
        .reindex(purposes_mtc)
    )
    triplen.loc["All Purposes", "avg_trip_dist"] = trips["travdist"].mean()
    triplen.to_csv(out_dir / "H-ForecastActivityLocation-triplen.csv")


def calculate_mode_choice(trips, out_dir):
    tripmc = trips.pivot_table(
        index=["home_county", "purpose"],
        columns=["trip_mode"],
        aggfunc="size",
        fill_value=0,
    )
    tripmc["All Modes"] = tripmc.sum(axis=1)
    tripmc["All Auto"] = (
        tripmc["Drive Alone"]
        + tripmc["Shared Ride 2"]
        + tripmc["Shared Ride 3+"]
        + tripmc["Taxi & Ride-Hailing"]
    )

    tripmc_countytotals = tripmc.groupby("home_county").sum()
    tripmc_countytotals["purpose"] = "Total"
    tripmc_countytotals.set_index(
        [tripmc_countytotals.index, "purpose"], inplace=True
    )
    tripmode_bayarea = tripmc_countytotals.sum()
    tripmode_bayarea["home_county"] = "Bay Area"
    tripmode_bayarea["purpose"] = "Total"
    tripmode_bayarea = tripmode_bayarea.to_frame().T.set_index(
        ["home_county", "purpose"]
    )

    tripmc = pd.concat((tripmc, tripmc_countytotals, tripmode_bayarea))
    tripmc = tripmc.div(tripmc["All Modes"], axis=0)
    tripmc = tripmc.reindex(
        pd.MultiIndex.from_product(
            [tripmc.index.levels[0], purposes_mtc_with_total],
            names=tripmc.index.names,
        )
    )[modes_mtc]
    tripmc.to_csv(out_dir / "I-ForecastModeChoice-2050.csv")


def trips_stats(model_run_dir, out_dir, taz_filepath):
    pers_cols = ["hhno", "pno", "pptyp"]
    tour_cols = [
        "hhno",
        "pno",
        "tour",
        "pdpurp",
        "tmodetp",
        "tpathtp",
        "parent",
        "subtrs",
        "tdtaz",
        "id",
    ]
    trip_cols = [
        "hhno",
        "pno",
        "tour",
        "mode",
        "otaz",
        "dtaz",
        "dpurp",
        "pathtype",
        "travdist",
        "id",
        "tour_id",
    ]
    tours = parse_tours(
        read_tours_with_home_geog(
            model_run_dir,
            taz_filepath,
            pers_usecols=pers_cols,
            tours_usecols=tour_cols,
        )
    )
    trips = read_trips(model_run_dir, usecols=trip_cols)
    trips = pd.merge(tours, trips, on=["hhno", "pno", "tour"])
    trips["trip_mode"] = trips["mode"].map(modes_champ_to_mtc)
    trips["purpose"] = trips["pdpurp"].map(purposes_champ_to_mtc)

    calculate_trip_freq(trips, out_dir)
    calculate_trip_len(trips, out_dir)
    calculate_mode_choice(trips, out_dir)


if __name__ == "__main__":
    config = load_config()
    trips_stats(
        Path(config["champ"]["forecast"]["model_run_dir"]),
        Path(config["out_dir"]),
        config["champ"]["forecast"]["taz_filepath"],
    )
