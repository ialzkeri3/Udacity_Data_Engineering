import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    Description: This function can be used to read the file in the filepath
    (data/song_data)to get the song and artist info and used to populate the
    songs and artists dim tables.

    Arguments:
        cur: the cursor object.
        filepath: song data file path.

    Returns:
        None
    """
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = song_data = df[['song_id', 'title',
                                'artist_id', 'year', 'duration']].values[0].tolist()
    cur.execute(song_table_insert, song_data)

    # insert artist record
    artist_data = artist_data = df[['artist_id', 'artist_name',
                                    'artist_location', 'artist_latitude',
                                    'artist_longitude']].values[0].tolist()


def process_log_file(cur, filepath):
    """
    Description: This function can be used to read the file in the filepath
    (data/log_data) to get the user, time, and songplay info and used to populate
    the users and time dim tables, and songplay fact table.

    Arguments:
        cur: the cursor object.
        filepath: log data file path.

    Returns:
        None
    """

    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')

    # insert time data records
    time_data = (t, t.dt.hour.values, t.dt.day.values, t.dt.week.values,
                 t.dt.month.values, t.dt.year.values, t.dt.day_name().values)
    column_labels = ('start_time', 'hour', 'day',
                     'week', 'month', 'year', 'weekday')
    time_df = pd.DataFrame(dict(zip(column_labels, time_data)))

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():

        # get songid and artistid from song and artist tables
        results = cur.execute(song_select, (row.song, row.artist, row.length))
        #results = cur.fetchone()
        songid, artistid = results if results else None, None

        # insert songplay record
        songplay_data = (pd.to_datetime(row.ts, unit='ms'), row.userId, row.level,
                         songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    Description: This function can be used to produce a list of all json files
    in the givin path. Then it passes each file to the specified function along
    with cusrsor givin in the argument. Then it commit the changes for each file
    using the connection object. It also keeps track of the files being processesd
    by printing the actual status of the data process to get the user and time info
    and used to populate the users and time dim tables.

    Arguments:
        cur: the cursor object.
        conn: the connection object.
        filepath: log data file path.
        func: function to be used in processing the data.

    Returns:
        None
    """

    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files:
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    conn = psycopg2.connect(
        "host=localhost dbname=sparkifydb user=ialzkeri password=udacity123")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
