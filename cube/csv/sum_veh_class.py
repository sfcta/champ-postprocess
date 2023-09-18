import argparse
from pathlib import Path

import polars as pl


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("directory")
    args = parser.parse_args()

    champ_periods = ["EA", "AM", "MD", "PM", "EV"]
    veh_class_cols = [f"V{i}_1" for i in range(1, 19)]
    for champ_period in champ_periods:
        filename_root = f"LOAD{champ_period}_FINAL"
        df = pl.scan_csv(
            Path(args.directory, f"{filename_root}.csv"),
            dtypes={col: pl.Float64 for col in veh_class_cols},
        ).with_columns(
            pl.sum_horizontal(veh_class_cols).alias("sum(V1_1..V18_1)")
        ).select(
            ["A", "B", "sum(V1_1..V18_1)"]
        ).collect().write_csv(
            Path(args.directory, f"{filename_root}-veh_class_sum.csv")
        )
