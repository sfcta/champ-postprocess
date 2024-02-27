from pathlib import Path

import pandas as pd
import polars as pl
from core import load_config, time_period_conversion_champ_to_mtc, time_periods


def screenline(model_run_dir, out_dir):
    out_filepath = out_dir / "J-Traffic&TransitAssignment-screenline-2050.csv"

    screenline_AB = pl.read_csv(out_dir / "screenline-AB.csv").select(
        "Route Number/Direction",
        "Link Description",
        "A",
        "B",
    )
    loaded_network_veh_class_cols = [f"V{i}_1" for i in range(1, 19)]
    screenline_vols = screenline_AB
    champ_volume_columns = []
    for time_period in time_periods:
        filename_root = f"LOAD{time_period}_FINAL"
        column_name = f"CHAMP-{time_period}"
        champ_volume_columns.append(column_name)
        loaded_network_vol_cols = loaded_network_veh_class_cols + [
            f"BUSVOL_{time_period}"
        ]
        loaded_network = (
            # read with pandas then convert because for some reason polars fails
            # to read some rows and also doesn't raise an error for this
            pl.from_pandas(
                pd.read_csv(
                    Path(model_run_dir, f"{filename_root}.csv"),
                    sep=",",
                    quotechar="'",
                    dtype={col: float for col in loaded_network_vol_cols},
                    usecols=["A", "B"] + loaded_network_vol_cols,
                )
            )
            # pl.scan_csv(
            #     Path(model_run_dir, f"{filename_root}.csv"),
            #     separator=",",
            #     quote_char="'",
            #     dtypes={col: pl.Float64 for col in loaded_network_vol_cols},
            # )
            .with_columns(
                # columns to sum:
                # cf. Y:\champ\dev\...\scripts\summarize\create-daily.s
                # this script is called from modelRunTopsheet, which was
                # referenced when I created this script
                pl.sum_horizontal(loaded_network_vol_cols).alias(column_name)
            ).select(["A", "B", column_name])
        )
        screenline_vols = screenline_vols.join(
            loaded_network, on=["A", "B"], how="left"
        )
    screenline_vols = screenline_vols.group_by(
        ["Route Number/Direction", "Link Description"], maintain_order=True
    ).agg((pl.sum(col) for col in champ_volume_columns))
    time_period_conversion_champ_to_mtc(screenline_vols).write_csv(
        out_filepath
    )


if __name__ == "__main__":
    config = load_config()
    screenline(
        config["champ"]["forecast"]["model_run_dir"],
        config["out_dir"],
    )
