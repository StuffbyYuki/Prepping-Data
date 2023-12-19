import polars as pl

clubs_df = pl.read_csv("data/After School Club.csv")
events_df = pl.read_csv("data/Events.csv", try_parse_dates=True)

weekdays_mapping = {
    1: "Monday",
    2: "Tuesday",
    3: "Wednesday",
    4: "Thursday",
    5: "Friday",
}

df = (
    events_df.set_sorted("Date")
    .upsample(time_column="Date", every="1d", maintain_order=True)
    .with_columns(
        pl.col("Date").dt.weekday().map_dict(weekdays_mapping).alias("Day of Week"),
        pl.col("Event").fill_null("N/A"),
    )
    .filter(pl.col("Day of Week").is_not_null())
    .join(clubs_df, on="Day of Week", how="left")
)
print(df)
