#!/usr/bin/env python3

import polars as pl

path = "./data/input_d01.txt"

# Part I
df = (
    pl.read_csv(path, has_header=False, separator=" ")
    # There are three spaces in between columns. Drop empty columns.
    .drop(["column_2", "column_3"])
    .rename({"column_1": "left_list", "column_4": "right_list"})
)

result = df.select(
    (pl.col("left_list").sort() - pl.col("right_list").sort()).abs().sum()
)

print(result)

# Part II
result = (
    df.filter(pl.col("right_list").is_in(pl.col("left_list")))
    .select(pl.col("right_list"))
    .group_by(pl.col("right_list"))
    .count()
    .select(pl.col("right_list") * pl.col("count"))
    .sum()
)

print(result)
