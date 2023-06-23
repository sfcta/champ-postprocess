from pathlib import Path

import polars as pl

champ_periods = ["EA", "AM", "MD", "PM", "EV"]
veh_class_cols = [f"V{i}_1" for i in range(1, 19)]
dir = r"X:\Projects\CHAMP7\Run6_Third"
for champ_period in champ_periods:
    filename_root = f"LOAD{champ_period}_FINAL"
    pl.scan_csv(
        Path(dir, f"{filename_root}.csv"),
        dtypes={col: pl.Float64 for col in veh_class_cols},
    ).with_columns(
        pl.sum([f"V{i}_1" for i in range(1, 19)]).alias("sum(V1_1..V18_1)")
    ).select(
        ["A", "B", "sum(V1_1..V18_1)"]
    ).collect().write_csv(
        Path(dir, f"{filename_root}-veh_class_sum.csv")
    )
