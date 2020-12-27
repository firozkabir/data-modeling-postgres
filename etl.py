#!/usr/bin/env python3
import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


import numpy as np
from psycopg2.extensions import register_adapter, AsIs
psycopg2.extensions.register_adapter(np.int64, psycopg2._psycopg.AsIs)

def addapt_numpy_float64(numpy_float64):
    """
    helper function needed to adapt numpy float 64 for postgresql
    # see https://stackoverflow.com/questions/50626058/psycopg2-cant-adapt-type-numpy-int64
    """
    return AsIs(numpy_float64)

def addapt_numpy_int64(numpy_int64):
    """
    helper function needed to adapt numpy float 64 for postgresql
    # see https://stackoverflow.com/questions/50626058/psycopg2-cant-adapt-type-numpy-int64

    """
    return AsIs(numpy_int64)

register_adapter(np.float64, addapt_numpy_float64)
register_adapter(np.int64, addapt_numpy_int64)

psycopg2.extensions.register_adapter(np.int64, psycopg2._psycopg.AsIs)


def process_song_file(cur, filepath):
    """
    - Takes a database cursor and a json filepath as parameters
    - Opens a json song file and loads it into a pandas dataframe
    - Extracts the "song record" from the df and inserts it to song_table with the given cursor
    - Extracts the "artist record" from the df and inserts it to song_table with the given cursor
    """
    # open song file
    df = pd.read_json(filepath, lines=True)
    df.fillna(0,inplace=True)

    # insert song record
    song_data = df[['song_id','title','artist_id','year','duration']].iloc[0].values.tolist()
    song_data = tuple(song_data)
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df[['artist_id','artist_name','artist_location','artist_latitude','artist_longitude']].iloc[0].values.tolist()
    artist_data = tuple(artist_data)
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    - Takes a database cursor and a json filepath as parameters
    - In this case, the json file is the log  file
    - Loads the logs onto a df then filters for NextSong 
    - Extract time data and converts it to h/d/w/m/y for the time dimention
    - stores the data in the database
    """
    # open log file
    df = pd.read_json(filepath, lines=True)
    df.fillna(0,inplace=True)
    


    # filter by NextSong action
    is_next_song = df['page'] == 'NextSong'
    df = df[is_next_song]

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'],unit='ms')
    
    # insert time data records
    time_data = zip(t, t.dt.hour, t.dt.dayofyear, t.dt.isocalendar().week, t.dt.month, t.dt.year, t.dt.weekday)
    column_labels = ['timestamp','hour','day','week','month','year','weekday']
    time_df = pd.DataFrame(list(time_data), columns=column_labels)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId','firstName','lastName','gender','level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (str(row.song), str(row.artist), row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (index, row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    - walks the given filepath to get a list of json files
    - shows you how many json  files were found
    - iterates over the files and calls the func function to apply ETL logic
        - func function should be designed:
        - to take a cursor and the json file 
        - to perform the ETL on the json file
        - and to save the data in the database
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
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
    """
        The main() function where all the execution takes place
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()