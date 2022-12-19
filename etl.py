"""
etl.py script outlining the stl process using the sql queries in the sql_queries.py file
"""
import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *
import math
import numpy as np


def round_up(n):
    """
    convert float number to next highest int
    n - input number

    returns next highest integer
    """
    return math.ceil(n)


def process_song_file(cur, filepath):
    """
    Processes the song files and inserts row into song and artist tables
    cur - Postgre db cursor for query execution
    filepath - filepath of the song_file

    returns: Nothing
    """
    # open song file
    df = pd.read_json(filepath, lines=True)
    df["duration"] = df["duration"].apply(np.ceil)
    # insert song record
    song_data = (
        df[["song_id", "title", "artist_id", "year", "duration"]]
        .values.flatten()
        .tolist()
    )
    cur.execute(song_table_insert, song_data)

    # insert artist record
    artist_data = (
        df[
            [
                "artist_id",
                "artist_name",
                "artist_location",
                "artist_latitude",
                "artist_longitude",
            ]
        ]
        .values.flatten()
        .tolist()
    )

    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    Processes the log files and inserts row into time, users and songplay tables
    cur - Postgre db cursor for query execution
    filepath - filepath of the song_file

    returns: Nothing
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df["page"] == "NextSong"]
    df["length"] = df["length"].apply(np.ceil)
    # convert timestamp column to datetime
    df["ts"] = pd.to_datetime(df["ts"], unit="ms")
    df["hour"] = df["ts"].dt.hour
    df["day"] = df["ts"].dt.day
    df["week"] = df["ts"].dt.isocalendar().week
    df["month"] = df["ts"].dt.month
    df["year"] = df["ts"].dt.year
    df["weekday"] = df["ts"].dt.weekday
    # t =

    # insert time data records
    time_data = df[["ts", "hour", "day", "week", "month", "year", "weekday"]]
    column_labels = {
        "ts": "start_time",
        "hour": "hour",
        "day": "day",
        "week": "week",
        "month": "month",
        "year": "year",
        "weekday": "weekday",
    }
    time_df = time_data.rename(column_labels)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df.loc[:, ["userId", "firstName", "lastName", "gender", "level"]]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():

        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()

        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (
            row.ts,
            row.userId,
            row.level,
            songid,
            artistid,
            row.sessionId,
            row.location,
            row.userAgent,
        )
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    wrapper function mapping data files to the processing functions
    cur - Postgre db cursor for query execution
    conn - db connection
    filepath - filepath of the song_file
    func - processing function

    returns: Nothing
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, "*.json"))
        for f in files:
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print("{} files found in {}".format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print("{}/{} files processed.".format(i, num_files))


def main():
    """
    main function wrapping the entire etl functions defined above
    Establishes data connection -> processes files -> inserts rows
    returns nothing
    """
    conn = psycopg2.connect(
        "host=127.0.0.1 dbname=sparkifydb user=postgres password=sriram"
    )
    cur = conn.cursor()

    process_data(cur, conn, filepath="data/song_data", func=process_song_file)
    process_data(cur, conn, filepath="data/log_data", func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
