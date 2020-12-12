import os
import glob
import psycopg2
import pandas as pd
from sql_queries import song_table_insert
from sql_queries import artist_table_insert
from sql_queries import user_table_insert
from sql_queries import time_table_insert
from sql_queries import songplay_table_insert
from sql_queries import song_select


##############################################################################
def process_song_file(cur, file_path):
    """
    Processes song files and insert into sparkifydb in song_table and
    artist_table
    :param cur: cursor to database
    :param file_path: path to song files 
    :return:
    """

    # open song file 
    df = pd.DataFrame(
        [pd.read_json(file_path, typ='series', convert_dates=False)]
    )

    for val in df.values:
        num_songs, artist_id, artist_latitude, artist_longitude, \
        artist_location, artist_name, song_id, title, duration, year = val

        # insert song record
        song_data = (
            song_id, title, artist_id, year, duration
        )
        cur.execute(song_table_insert, song_data)

        # insert artist record
        artist_data = (
            artist_id, artist_name, artist_location, artist_latitude,
            artist_longitude
        )
        cur.execute(artist_table_insert, artist_data)

    print(f"Successfully inserted record for file: {file_path}")


def process_log_file(cur, file_path):
    """
    Processes log files and insert into user_table, time_table, and
    songplay_table
    :param cur: cursor to database
    :param file_path: path to database
    :return:
    """

    # open log file 
    df = pd.read_json(file_path, lines=True)

    # filter by NextSong action and convert timestamp column to datetime
    df = df[df['page'] == "NextSong"].astype({'ts': 'datetime64[ms]'})
    t = pd.Series(df['ts'], index=df.index)

    # insert time data records
    time_data = []
    column_labels = [
        "timestamp", "hour", "day", "weekofyear", "month", "year", "weekday"
    ]
    for data in t:
        time_data.append([
            data,
            data.hour,
            data.day,
            data.weekofyear,
            data.month,
            data.year,
            data.day_name()
        ])

    time_df = pd.DataFrame.from_records(data=time_data, columns=column_labels)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[
        ['userId', 'firstName', 'lastName', 'gender', 'level']
    ]

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
        songplay_data = (row.ts, row.userId, row.level, songid, artistid,
                         row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    This function loads data from files and executes functions to process song
    and log files
    :param cur: cursor to database
    :param conn: connection to database
    :param filepath: path to files
    :param func: functions to process files
    :return:
    """

    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, "*.json"))
        for f in files:
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print(f"{num_files} files found in {filepath}")

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print(f"{i}/{num_files} files processed.")


def main():
    """
    Drives functions to process files and load them in sparkifydb
    :return:
    """

    conn = psycopg2.connect(
        "host=127.0.0.1 dbname=sparkifydb user=student password=student"
    )
    cur = conn.cursor()

    process_data(cur, conn, filepath="data/song_data", func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == '__main__':
    main()
    print("Finished processing and loading")
