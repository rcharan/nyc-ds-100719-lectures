import csv
import string
import matplotlib.pyplot as plt


# Read the album data
f = open('data.csv')
reader = csv.reader(f)
albums = [row for row in reader]
header, albums = albums[0], albums[1:]
albums = [dict(zip(header, album)) for album in albums]

# Lookup by column
def _find_by_key(key, value, data):
    for datum in data:
        if str(datum[key]) == str(value):
            return datum

    return None

def find_by_album(val, data): return _find_by_key('album' , val, data)
def find_by_name(val): return find_by_album(val, albums)
def find_by_rank(val, data): return _find_by_key('number', val, data)


# Filtering by column
def _filter_by_key(key, property, data):
    return list(filter(lambda datum : property(datum[key]), data))

def _in_range(lo, hi):
    def out(val):
        return lo <= int(val) <= hi
    return out

def find_by_year(year, data):
    return _filter_by_key('year', lambda y : int(y) == year, data)

def find_by_years(lo, hi, data):
    return _filter_by_key('year', _in_range(lo, hi), data)

def find_by_ranks(lo, hi, data):
    return _filter_by_key('number', _in_range(lo, hi), data)

# Select columns
def _select_column(column, data):
    return [datum[column] for datum in data]

def get_titles(data):
    return _select_column('album', data)

def get_artists(data):
    return _select_column('artist', data)

def get_years(data):
    return _select_column('year', data)

def get_genres(data):
    def parse_genres(genre_str):
        gs = genre_str.split(',')
        return list(map(lambda s : s.strip(), gs))

    genres = list(map(parse_genres, _select_column('genre', data)))
    return genres

def get_genres_flat(data):
    return sum(get_genres(data), [])

def _get_highest_freq(elts):
    count_dict = {}
    for elt in elts:
        if elt in count_dict:
            count_dict[elt] += 1
        else:
            count_dict[elt] = 1

    # most_frequent = max(count_dict.items(), key = lambda t : t[1])
    high_frequency = max(count_dict.values())
    maximi = filter(lambda t : t[1] == high_frequency, count_dict.items())
    return list(map(lambda t : t[0], maximi))

def most_albums(data = albums):
    return _get_highest_freq(get_artists(data))

def most_words(data = albums):
    titles = get_titles(data)
    def filter_out_special_chars(string_):
        good_chars = string.ascii_letters + ' '
        good_list  = filter(lambda c : c in good_chars, string_)
        good_str   = ''.join(good_list)
        return good_str

    def get_words(string_):
        good_str = filter_out_special_chars(string_)
        return good_str.split(' ')

    words = sum(map(lambda t : get_words(t), titles), [])

    return _get_highest_freq(words)


def make_year_hist(data = albums):
    plt.clf()
    years = list(map(lambda s : int(s), get_years(data)))
    plt.hist(years, bins=[1950, 1960, 1970, 1980, 1990, 2000, 2010, 2020])


def make_genre_hist(data = albums):
    plt.clf()
    plt.hist(get_genres_flat(data))
    plt.show()

################################################################################
#
# Alternative pandas implementation
#  - obvious extensions like selecting a column not included.
#  - finding max frequency would proceed by essentially the same method and
#  -   is not reimplemented
#
################################################################################
import pandas as pd
import numpy  as np

albums_df = pd.read_csv('data.csv')
albums_df.head()

def _pd_filter_by_key(key, val, df = albums_df):
    return df[df[key] == val]

def _pd_filter_by_property(key, property_, df = albums_df):
    return df[df[key].apply(property_)]

def _get_top_result(df):
    if len(df) == 0:
        return None
    else:
        return df[:1]


def pd_find_by_name(val):
    results = _pd_filter_by_key('album', val)
    return _get_top_result(results)

def pd_find_by_year(val):
    return _pd_filter_by_key('year', val)

def pd_find_by_rank(val):
    results = _pd_filter_by_key('number', val)
    return _get_top_result(results)

def pd_find_by_years(lo, hi):
    return _pd_filter_by_property('year', lambda y : lo <= y <= hi)

def pd_find_by_ranks(lo, hi):
    return _pd_filter_by_property('number', lambda n : lo <= n <= hi)

top_500_songs_df = pd.read_csv('top-500-songs.txt', delimiter = '\t', header = None)
top_500_songs_df.columns = ['number', 'track', 'artist', 'year']

# This merge does not work. We need more data
# left_merge_df = pd.merge(top_500_songs_df, albums_df, on = ['artist', 'year'],
#                     indicator = True, how = 'left', validate = 'm:1')
# detect_duplicates(albums_df, ['artist', 'year'])

def detect_duplicates(df, keys):
    if not isinstance(keys, list):
        keys = [keys]
    df['duplicate'] = df.duplicated(subset = keys, keep = False)
    duplicates_df = _pd_filter_by_key('duplicate', True, df = df).sort_values(keys)
    return duplicates_df

album_track_df = pd.read_json('track_data.json')

# Expand the songs to bring them to the top level of the data structure
#  There has to be a cleaner way to do this
expanded = []
for i in range(len(album_track_df)):
    r = album_track_df.iloc[i:i+1]
    tracks = r.iloc[0].tolist()[2]
    num_tracks = len(tracks)
    s = pd.concat([r]*num_tracks)
    s['tracks'] = tracks
    expanded.append(s)

album_tracks_df_expanded = pd.concat(expanded)
album_tracks_df_expanded.rename({'tracks' : 'track'}, axis = 1, inplace = True)
album_tracks_df_expanded.drop_duplicates(inplace = True)


# Merge in the data on the album to obtain the year in the album track dataset
album_tracks_df_expanded = pd.merge(album_tracks_df_expanded, albums_df,
                           on = ['album', 'artist'],
                           indicator = False, how = 'left', validate = 'm:1')


top_500_songs_merged_df =  \
        pd.merge(top_500_songs_df, album_tracks_df_expanded,
                 on = ['artist', 'track', 'year'], indicator = True,
                 how = 'left', validate = '1:1')

albums_merged_df = \
        pd.merge(album_tracks_df_expanded, top_500_songs_df,
                 on = ['artist', 'track', 'year'], indicator = True,
                 how = 'left', validate = '1:1')

good = albums_merged_df['_merge'] == 'both'
albums_with_top_songs = albums_merged_df[good].groupby(['artist', 'album', 'year'], as_index = False).count()
albums_with_top_songs = albums_with_top_songs[['album', 'track', 'artist', 'year']].sort_values('track', ascending = False)

good = top_500_songs_merged_df['_merge'] == 'both'
top_songs_on_top_albums = top_500_songs_merged_df[good]
del good

# Assignmen
# albumWithMostTopSongs - returns the name of the artist and album that has that most songs featured on the top 500 songs list
# albumsWithTopSongs - returns a list with the name of only the albums that have tracks featured on the list of top 500 songs
# songsThatAreOnTopAlbums - returns a list with the name of only the songs featured on the list of top albums
# top10AlbumsByTopSongs - returns a histogram with the 10 albums that have the most songs that appear in the top songs list. The album names should point to the number of songs that appear on the top 500 songs list.
# topOverallArtist - Artist featured with the most songs and albums on the two lists. This means that if Brittany Spears had 3 of her albums featured on the top albums listed and 10 of her songs featured on the top songs, she would have a total of 13. The artist with the highest aggregate score would be the top overall artist.
def albumWithMostTopSongs():
    return albums_with_top_songs['album'].tolist()[0]

def albumsWithTopSongs():
    return albums_with_top_songs[['album']]

def top10AlbumsByTopSongs():
    data = albums_with_top_songs[:10][['album', 'track']]
    data = sum([[t[0]] * t[1] for t in data.to_records(index = False)], [])

    plt.clf()
    plt.hist(data)
    plt.show()

def songsThatAreOnTopAlbums():
    return top_songs_on_top_albums

def topOverallArtist():
    album_counts = albums_with_top_songs.groupby('artist', as_index = False).count()[['artist','album']]
    track_counts = top_500_songs_df.groupby('artist', as_index = False).count()[['artist', 'track']]

    counts = pd.merge(album_counts, track_counts, on = 'artist', indicator = True, how = 'outer', validate='1:1')
    counts.replace(to_replace = {np.nan : 0}, inplace = True)
    counts['score'] = counts['album'] + counts['track']
    counts.sort_values('score', ascending = False, inplace = True)

    return counts.iloc[0].tolist()[0]
