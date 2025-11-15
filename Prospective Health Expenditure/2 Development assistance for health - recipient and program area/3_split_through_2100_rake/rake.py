"""
Rake a single year of data for the specified data set type.

Args:
    --year YEAR[int] to rake for
    --tag TAG[str] either 'chr' for Channel-HFA-Recipient raking or 'sr' for Source-Recipient raking


"""
from pathlib import Path
import argparse
import time
import numpy as np
import pandas as pd
from raking.experimental import DataBuilder, DualSolver


parser = argparse.ArgumentParser(description="Rake data for a single year")
parser.add_argument(
    "--year",
    type=float,
    help="Year to rake"
)
parser.add_argument(
    "--tag",
    type=str,
    help="Either 'chr' for Channel-HFA-Recipient raking or 'sr' for Source-Recipient raking"
)
parser.add_argument(
    "--work_dir",
    type=str,
    help="Working directory"
)


def prep_data(data_year, input_dir, output_dir):
    margin1 = pd.read_feather(input_dir / f"FILEPATH")
    margin2 = pd.read_feather(input_dir / f"FILEPATH")
    cells = pd.read_feather(input_dir / f"FILEPATH")

    # Reformat the data for the experimental version of the raking package
    cells["weights"] = 1.0
    margin1.rename(columns={"value_agg_over_src": "value"}, inplace=True)
    margin1["src"] = "all"
    margin1["weights"] = np.inf
    margin2.rename(columns={"value_agg_over_recip": "value"}, inplace=True)
    margin2["recip"] = "all"
    margin2["weights"] = np.inf

    return cells, margin1, margin2

def rake(cells, margin1, margin2, disp=True, xtol=1e-10, maxiter=200):
    """
    Rake the data frame using the experimental version of the raking package.
    """
    df = pd.concat([cells, margin1, margin2])
    data_builder = DataBuilder(
        dim_specs={"src": "all", "recip": "all"},
        value="value",
        weights="weights"
    )
    data = data_builder.build(df)
    solver = DualSolver(distance="entropic", data=data)
    raked = solver.solve(
        options={
            "disp": disp,
            "xtol": xtol,
            "maxiter": maxiter
        }
    )

    raked = raked.rename(columns={"soln": "raked_value"})
    return raked


if __name__ == "__main__":
    np.random.seed(3980)
    args = parser.parse_args()
    data_year = int(args.year)
    data_tag = str(args.tag)
    WORK_DIR = str(args.work_dir)

    if not data_year or not (data_year >= 2024 and data_year <= 2100 ):
        raise ValueError("--year is required, and must be between 2024 and 2100")

    if not data_tag or data_tag not in ["chr", "sr"]:
        raise ValueError("--tag is required, and must be either 'chr' or 'sr'")

    input_dir = Path(WORK_DIR) / "data" / "rake_inputs" / data_tag
    output_dir = Path(WORK_DIR) / "data" / "raked" / data_tag


    print(f"\n* {data_year} - {data_tag.upper()} *")
    print(f"* Prepping data...")
    cells, margin1, margin2 = prep_data(data_year, input_dir, output_dir)

    # Rake
    print(f"* Beginning raking...")
    t0 = time.time()
    raked = rake(cells, margin1, margin2)
    rake_time_s = time.time() - t0
    print(f"* Raking took {rake_time_s :.2f} seconds.")

    # finalize and save
    raked["year"] = data_year
    raked["rake_time_s"] = rake_time_s
    ## merge on original value
    raked = raked.merge(cells[["src", "recip", "value"]], on=["src", "recip"], how="left")

    raked.to_feather(output_dir / f"FILEPATH")

    # =========================================================================
    # print some info and quickly test that constraints are being met 
    target = margin1[["value"]].sum().values[0]
    raked_target = raked[["raked_value"]].sum().values[0]
    print(f"** Total difference ($) between annual total and raked total: {target/1e9 :.3f}B - {raked_target/1e9 :.3f}B = {target - raked_target :,.2f}") 

    raked_recip = raked.groupby("recip")["raked_value"].sum().reset_index()
    raked_src = raked.groupby("src")["raked_value"].sum().reset_index()

    margin1_tst = margin1.merge(raked_recip, on="recip", how="left")
    margin1_tst["diff"] = margin1_tst["value"] - margin1_tst["raked_value"]
    m1_max_diff = margin1_tst["diff"].max()
    print(f"** Margin1: largest diff: {m1_max_diff:,.2f}")

    margin2_tst = margin2.merge(raked_src, on="src", how="left")
    margin2_tst["diff"] = margin2_tst["value"] - margin2_tst["raked_value"]
    m2_max_diff = margin2_tst["diff"].max()
    print(f"** Margin2: largest diff: {m2_max_diff:,.2f}")

    max_diff = max(m1_max_diff, m2_max_diff)
    if max_diff > 1000:
        raise ValueError(f"Max diff is too large: {max_diff:,.2f}")
