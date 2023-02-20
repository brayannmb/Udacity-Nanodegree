import os
import glob
import json
import psycopg2
import pandas as pd
from sql_queries import *

def correct_json(datafile):
    
    """
    Open json data and return a pandas dataframe.
    
    This fucntion just read/collect data from json file and
    create a pandas dataframe.
    
    Parameters:
    -------------------------------------------------------
    datafile = filepath
    
    Returns:
    -------------------------------------------------------
    pandas dataframe
    
    """
    
    print(datafile)
    f = open(f'{datafile}')
    data = json.load(f)
    for i in data:
        data[i] = [data[i]]
    df_aux = pd.DataFrame(data)
    return df_aux

def process_song_file(cur, filepath):
        
    # open song file
    df = correct_json(filepath)

    # insert artist record
    artist_data = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].copy()
    
    # identify None values in artist_latitude and artist_longitude
    if artist_data.loc[0, 'artist_latitude'] == None and artist_data.loc[0, 'artist_longitude'] == None: 
        artist_data = (str(artist_data.loc[0, 'artist_id']), str(artist_data.loc[0, 'artist_name']), str(artist_data.loc[0, 'artist_location']), artist_data.loc[0, 'artist_latitude'], artist_data.loc[0, 'artist_longitude'])
    elif artist_data.loc[0, 'artist_latitude'] == None and artist_data.loc[0, 'artist_longitude'] != None:
        artist_data = (str(artist_data.loc[0, 'artist_id']), str(artist_data.loc[0, 'artist_name']), str(artist_data.loc[0, 'artist_location']), artist_data.loc[0, 'artist_latitude'], float(artist_data.loc[0, 'artist_longitude']))
    elif artist_data.loc[0, 'artist_latitude'] != None and artist_data.loc[0, 'artist_longitude'] == None:    
        artist_data = (str(artist_data.loc[0, 'artist_id']), str(artist_data.loc[0, 'artist_name']), str(artist_data.loc[0, 'artist_location']), float(artist_data.loc[0, 'artist_latitude']), artist_data.loc[0, 'artist_longitude'])
    else:
        artist_data = (str(artist_data.loc[0, 'artist_id']), str(artist_data.loc[0, 'artist_name']), str(artist_data.loc[0, 'artist_location']), float(artist_data.loc[0, 'artist_latitude']), float(artist_data.loc[0, 'artist_longitude']))
    try:
        cur.execute(artist_table_insert, artist_data)
    except Exception as e:
        print(e)
    
    # insert song record
    song_data =  df[['song_id', 'title', 'artist_id', 'year', 'duration']].copy()
    song_data = (str(song_data.loc[0, 'song_id']), str(song_data.loc[0, 'title']), str(song_data.loc[0, 'artist_id']), int(song_data.loc[0, 'year']), float(song_data.loc[0, 'duration']))
    
    try:
        cur.execute(song_table_insert, song_data)
    except Exception as e:
        print(e)
    
    
def process_log_file(cur, filepath):
    duplicate_user = 0
    
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page'] == 'NextSong'] 

    # convert timestamp column to datetime
    #t = 
    df['ts'] = pd.to_datetime(df['ts'], unit='ms')
    df.reset_index(drop=True, inplace=True)

    df['hour'] = df['ts'].dt.hour  # extract hour by ts column
    df['day'] = df['ts'].dt.day  # extract day by ts column
    df['week'] = df['ts'].dt.week  # extract week by ts column
    df['month'] = df['ts'].dt.month  # extract month by ts column
    df['year'] = df['ts'].dt.year  # extract year by ts column
    df['weekday'] = df['ts'].dt.weekday  # extract weekday by ts column
    
    # insert time data records
    time_data = df[['ts', 'hour', 'day', 'week', 'month', 'year', 'weekday']].copy()
    #column_labels = 
    time_df = time_data.copy()
   
    for i, row in time_df.iterrows():
        try:
            cur.execute(time_table_insert, list(row))
        except Exception as e:
            print(e)
    
    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']].copy()
    try:
        user_df = user_df[user_df['userId'] != '']  # remove null values
    except Exception as e:
        print(e)
    user_df.loc[:,'userId'] = user_df.loc[:,'userId'].astype(int)  # convert use_id column to int 
    user_df.drop_duplicates(subset=['userId'], inplace=True)  # remove duplicates based on user_id column

    # insert user records
    for i, row in user_df.iterrows():
        try:
            cur.execute(user_table_insert, list(row))
        except Exception as e:
            print(e)

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
        songplay_data = (row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        try:    
            cur.execute(songplay_table_insert, songplay_data)
        except Exception as e:
            print(e)
            
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
        if '.ipynb_checkpoints' in datafile:
            pass
        else:
            #print(datafile)
            func(cur, datafile)
            #conn.commit()
            print('{}/{} files processed.'.format(i, num_files))


def main():
    global conn
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    conn.autocommit = True
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    #conn.close()


if __name__ == "__main__":
    main()