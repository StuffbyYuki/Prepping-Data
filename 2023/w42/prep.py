import polars as pl

yr5_contact = pl.read_csv('data/Year 5 Contact Details.csv')
yr6_contact = pl.read_csv('data/Year 6 Contact Details.csv')
yr5_songs = pl.read_csv('data/Year 5 Song Choices.csv')
yr6_songs = pl.read_csv('data/Year 6 Song Choices.csv')

yr5_summary = yr5_songs.join(yr5_contact, on='Full Name', how='left').rename({'Song Choice': 'Song Recommendation'})
yr6_summary = yr6_songs.join(yr6_contact, on='Full Name', how='left')

df = (
    pl.concat([yr5_summary, yr6_summary])
    .group_by('Year Group', 'Song Recommendation')
    .agg(pl.count().alias('Number of Votes'))
    .sort('Song Recommendation', 'Year Group')
)

print(df)
