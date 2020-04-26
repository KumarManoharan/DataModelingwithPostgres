"""etl.py is the reads the song data files, log data files and loads into the Postgres DB"""
import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *

"""
Arguments: cursor and filepath
Return value: none"""
    """ 
    Summary line. 
  
    process_song_file function reads the song file and inserts data into song table and artist table 
  
    Parameters: 
    cur (psycopg2.extensions.cursor): Contains the cursor information
    filepath (string): Contails song data file location information 
  
    Returns: 
    None
  
    """
def process_song_file(cur, filepath):
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = song_data = df[["song_id", "title", "artist_id", "year", "duration"]].values[0].tolist()
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = artist_data = df[["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"]].values[0].tolist()
    cur.execute(artist_table_insert, artist_data)

    """ 
    Summary line. 
  
    process_log_file function reads the JSON log files and inserts data into three tables, time, users and songplays 
  
    Parameters: 
    cur (psycopg2.extensions.cursor): Contains the cursor information
    filepath (string): Contails log data file location information 
  
    Returns: 
    None
  
    """
def process_log_file(cur, filepath):
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[(df['page'] == 'NextSong')]

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')
    df['ts'] = pd.to_datetime(df['ts'], unit='ms')
    
    # insert time data records
    time_data = list((t, t.dt.hour, t.dt.day, t.dt.weekofyear, t.dt.month, t.dt.year, t.dt.weekday))
    column_labels = list(('start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday'))
    time_df =  pd.DataFrame.from_dict(dict(zip(column_labels, time_data)))

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[["userId", "firstName", "lastName", "gender", "level"]]

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
        songplay_data = (index, row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


    """ 
    Summary line. 
  
    process_data function gets the list of JSON files to be processed and calls process_song_file or process_log_file function based on the arguments.
  
    Parameters: 
    cur (psycopg2.extensions.cursor): Contains the cursor information
    conn (psycopg2.extensions.connection): Contains DB connection information
    filepath (string): song data/ log data file path
    func (string) - contains information about the function to be called, values would be process_song_file or process_log_file
  
    Returns: 
    None
  
    """
def process_data(cur, conn, filepath, func):
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

    """ 
    Summary line. 
  
    main establishes the connection with Postgres DB sets the cursor and call the process_data function two times, for song and lod data
  
    Parameters: 
    None
  
    Returns: 
    None 
  
    """
def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()