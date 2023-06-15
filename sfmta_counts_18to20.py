import json
import re
from datetime import date, datetime, time, timedelta
from itertools import pairwise
from pathlib import Path

import pandas as pd

from champ_network import (
    filter_by_direction,
    find_paths_from_streetnames,
    load_champ_network,
    read_champ_nodes,
)

sfmta_counts_dir = (
    r"Q:\Data\Observed\Streets\Counts\PreCountDracula"
    r"\2018-2020\OneDrive_2023-05-15\2018-Feb 2020"
)

filename_re_pattern = re.compile(
    r"([\w ][^_]+)_([NSEW]B(?:_[NSEW]B)?)_([\w ]+)_([\w ]+).xls[xm]"
)
primary_st_re_pattern = re.compile(r"([\w. ]+) +(\w+)")


def parse_filename(filename):
    filename_regex_match = filename_re_pattern.fullmatch(filename)
    if filename_regex_match:
        (
            primary_st,
            directions,
            cross_st_1_name,
            cross_st_2_name,
        ) = filename_regex_match.groups()
        primary_st_name, primary_st_type = primary_st_re_pattern.fullmatch(
            primary_st
        ).groups()
        directions = directions.split("_")
        return (
            primary_st_name,
            primary_st_type,
            directions,
            cross_st_1_name,
            cross_st_2_name,
        )
    else:
        return None


def _parse_time(t: str | datetime | time) -> time:
    try:
        return datetime.strptime(t, "%I:%M %p").time()
    except ValueError:
        return time.fromisoformat(t)
    except TypeError:  # assume datetime.time or .datetime (with a wrong date)
        try:
            return t.time()
        except AttributeError:  # assume datetime.time
            return t


def load_counts_sheet(filename, direction, sfmta_counts_dir=sfmta_counts_dir):
    filepath = Path(sfmta_counts_dir, filename)
    # the UC col are counts where there are no associated speed data
    cols = (
        ["Date", "Time"]
        + [f"{i}-{i}" for i in range(1, 60)]
        + ["60-9999", "UC"]
    )
    # skip reading the "Time" column because the first 4 entries of the first
    # '1MPH Speed' Sheet is poorly formatted and is thus hard to handle
    try:
        sheet_name = f"{direction} - 1MPH Speed"
        df = pd.read_excel(
            filepath, sheet_name=sheet_name, header=11, usecols=cols
        )
    except ValueError:
        sheet_name = f"{direction}- 1MPH Speed"
        df = pd.read_excel(
            filepath, sheet_name=sheet_name, header=11, usecols=cols
        )
    df = df[df["Date"].notna()]  # filter out empty rows
    # The xlsx sheets actually have the time inside the Date field already.
    # But commenting out the following shortcut for .xlsx; just parse xlsx and
    # xlsm both using the same logic, hence I need to take row["Date"].date()
    # if filepath.suffix == ".xlsx":
    #     assert len(df["Date"]) == len(df)
    #     df.index = df["Date"]
    # elif filepath.suffix == ".xlsm":
    df["Time"] = df["Time"].apply(_parse_time)
    df.index = df.apply(
        # take .date() and .time() in case
        lambda row: datetime.combine(row["Date"].date(), row["Time"]),
        axis=1,
    )
    df.drop(columns=["Date", "Time"], inplace=True)
    return df


def get_counts_totals(df):
    """Return summed counts for each time period"""
    return df.sum(axis=1)


def bin_count_totals_by_champ_periods(count_totals):
    arbitrary_date = date(2000, 1, 1)
    champ_periods = [
        time(3, 0),
        time(6, 0),
        time(9, 0),
        time(15, 30),
        time(18, 30),
    ]
    champ_periods = [
        datetime.combine(arbitrary_date, t) for t in champ_periods
    ]
    # split EV at midnight:
    # since all times are assigned to the same day, add extra bin (edges)
    champ_periods = (
        [datetime.combine(arbitrary_date, time(0, 0))]
        + champ_periods
        + [datetime.combine(arbitrary_date + timedelta(days=1), time(0, 0))]
    )
    champ_period_labels = ["EV", "EA", "AM", "MD", "PM", "EV"]

    # Remove date because it's irrelevant, we've checked that this set of
    # SFMTA counts data only contain counts from Tue/Wed/Thu.
    # Unfortunately pd.cut cannot take datetime.time objs as is,
    # so we need to convert it back to datetime objs with an arbitrary date
    count_totals.index = [
        datetime.combine(arbitrary_date, t) for t in count_totals.index.time
    ]

    binned_times = pd.cut(
        count_totals.index,
        champ_periods,
        labels=champ_period_labels,
        ordered=False,
        right=False,
    )
    binned_count_totals = (
        count_totals.groupby(binned_times)
        .sum()  # BUG TODO this sums over multiple days!!! need to take daily average instead
        .reindex(champ_period_labels[1:])
    )
    return binned_count_totals


def compare_sfmta_counts_to_champ_network(
    champ_nodes, champ_digraph, sfmta_counts_dir=sfmta_counts_dir
):
    filename_parse_skipped = []
    counts_extract_skipped = []
    geomatch_skipped = []
    geomatch_direction_skipped = []

    comparison_df_rows = []  # the output

    for p in Path(sfmta_counts_dir).glob("*.xls*"):
        # without maybe monads, `continue` is cleaner than using nested
        # if-else/try-except structures
        parsed_filename = parse_filename(p.name)
        if not parsed_filename:  # if None
            filename_parse_skipped.append(p.name)
            print("can't parse filename:", p.name)
            continue
        (
            primary_st_name,
            primary_st_type,
            directions,
            cross_st_1_name,
            cross_st_2_name,
        ) = parsed_filename
        champ_paths_found = find_paths_from_streetnames(
            champ_digraph,
            primary_st_name,
            primary_st_type,
            cross_st_1_name,
            None,
            cross_st_2_name,
            None,
        )
        if not champ_paths_found:  # if []
            geomatch_skipped.append(p.name)
            print(
                "geo-matching unsuccessful; paths not found:",
                f"{primary_st_name}/{primary_st_type}",
                directions,
                cross_st_1_name,
                cross_st_2_name,
            )
            continue
        for direction in directions:
            try:
                champ_path = filter_by_direction(
                    champ_nodes, champ_paths_found, direction
                )
            except:
                counts_extract_skipped.append([p.name, direction])
                print("can't extract counts from", direction, p.name)
                continue
            try:
                count_totals = bin_count_totals_by_champ_periods(
                    get_counts_totals(load_counts_sheet(p.name, direction))
                )
            except (ValueError, RuntimeError):
                geomatch_direction_skipped.append([p.name, direction])
                print(
                    f"{primary_st_name}/{primary_st_type}",
                    directions,
                    cross_st_1_name,
                    cross_st_2_name,
                )
                continue
            row = count_totals.to_dict()  # or maybe consider using OrderedDict
            # now join the count_totals to the champ_path
            # add identifying info to the Series count_totals,
            # to prepare for turning into a row of the output_df later
            row["primary_st_name"] = primary_st_name
            row["primary_st_type"] = primary_st_type
            row["cross_st_1_name"] = cross_st_1_name
            row["cross_st_2_name"] = cross_st_2_name
            row["direction"] = direction
            for A, B in pairwise(champ_path):
                # beware of mutability,
                # thus not adding champ_nodes to row directly
                comparison_df_rows.append(row | {"CHAMP_A": A, "CHAMP_B": B})
    columns = [
        "CHAMP_A",
        "CHAMP_B",
        "primary_st_name",
        "primary_st_type",
        "cross_st_1_name",
        "cross_st_2_name",
        "direction",
        "EA",
        "AM",
        "MD",
        "PM",
        "EV",
    ]
    comparison_df = pd.DataFrame.from_records(
        comparison_df_rows, columns=columns
    )
    skipped = {
        "filename_parse_skipped": filename_parse_skipped,
        "counts_extract_skipped": counts_extract_skipped,
        "geomatch_skipped": geomatch_skipped,
        "geomatch_direction_skipped": geomatch_direction_skipped,
    }
    return (comparison_df, skipped)


if __name__ == "__main__":
    champ_links_gis_filepath = (
        r"X:\Projects\DTX\CaltrainValidation\s8_2019_Base\freeflow.shp"
    )
    champ_nodes_gis_filepath = (
        r"X:\Projects\DTX\CaltrainValidation\s8_2019_Base\FREEFLOW_nodes.shp"
    )
    comparison_df_filepath = (
        r"Q:\Data\Observed\Streets\Counts\PreCountDracula"
        r"\2018-2020\OneDrive_2023-05-15\2018-Feb 2020\_CHAMP_comparison"
        "\join_of-SFMTA_counts-CHAMP_AB-DTX_CaltrainValidation_s8_2019_Base.csv"
    )
    skipped_log_filepath = (
        r"Q:\Data\Observed\Streets\Counts\PreCountDracula"
        r"\2018-2020\OneDrive_2023-05-15\2018-Feb 2020\_CHAMP_comparison"
        "\skipped-log.json"
    )
    champ_nodes = read_champ_nodes(champ_nodes_gis_filepath)
    champ_digraph = load_champ_network(champ_links_gis_filepath, champ_nodes)
    comparison_df, skipped = compare_sfmta_counts_to_champ_network(
        champ_nodes, champ_digraph
    )
    comparison_df.to_csv(comparison_df_filepath, index=False)
    with open(skipped_log_filepath, "w") as f:
        json.dump(skipped, f, indent=2)
