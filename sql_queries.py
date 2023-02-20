# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

# CREATE TABLES

artist_table_create = (f"""
                           CREATE TABLE artists (artist_id VARCHAR(255) NOT NULL,
                                                 name VARCHAR(255) NOT NULL,
                                                 location VARCHAR(255) NOT NULL,
                                                 latitude FLOAT,
                                                 longitude FLOAT,
                                                 PRIMARY KEY(artist_id))  
                       """)


user_table_create = (f"""
                         CREATE TABLE users (user_id INT NOT NULL,
                                             first_name VARCHAR(50) NOT NULL,
                                             last_name VARCHAR(50) NOT NULL,
                                             gender VARCHAR(50) NOT NULL,
                                             level VARCHAR(50) NOT NULL,
                                             PRIMARY KEY(user_id))
                     """)

song_table_create = (f"""
                         CREATE TABLE songs (song_id VARCHAR(255) NOT NULL,
                                             title VARCHAR(255) UNIQUE,
                                             artist_id VARCHAR(255) NOT NULL,
                                             year INT NOT NULL,
                                             duration FLOAT NOT NULL,
                                             PRIMARY KEY(song_id),
                                             CONSTRAINT artist_fk FOREIGN KEY(artist_id) REFERENCES artists(artist_id))
                     """)

songplay_table_create = (f"""
                             CREATE TABLE songplays (start_time TIMESTAMP NOT NULL,
                                                     user_id INT NOT NULL,
                                                     level VARCHAR(50) NOT NULL,
                                                     song_id VARCHAR(255),
                                                     artist_id VARCHAR(255),
                                                     session_id INT NOT NULL,
                                                     location VARCHAR(255) NOT NULL,
                                                     user_agent VARCHAR(255) NOT NULL,
                                                     CONSTRAINT user_fk FOREIGN KEY(user_id) REFERENCES users(user_id),
                                                     CONSTRAINT song_fk FOREIGN KEY(song_id) REFERENCES songs(song_id),
                                                     CONSTRAINT artist_fk FOREIGN KEY(artist_id) REFERENCES artists(artist_id))
                             """)


time_table_create = (f"""
                         CREATE TABLE time (start_time TIMESTAMP NOT NULL,
                                            hour INT NOT NULL,
                                            day INT NOT NULL,
                                            week INT NOT NULL,
                                            month INT NOT NULL,
                                            year INT NOT NULL, 
                                            weekday INT NOT NULL)
                     """)

# INSERT RECORDS

songplay_table_insert = (f"""
                             INSERT INTO songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
                             VALUES(%s, %s, %s, %s, %s, %s, %s, %s)
                         """)

user_table_insert = (f"""
                         INSERT INTO users(user_id, first_name, last_name, gender, level)
                         VALUES(%s, %s, %s, %s, %s)
                     """)

song_table_insert = (f"""
                         INSERT INTO songs(song_id, title, artist_id, year, duration)
                         VALUES(%s, %s, %s, %s, %s)
                     """)

artist_table_insert = (f"""
                           INSERT INTO artists(artist_id, name, location, latitude, longitude)
                           VALUES(%s, %s, %s, %s, %s)
                       """)


time_table_insert = (f"""
                         INSERT INTO time(start_time, hour, day, week, month, year, weekday)
                         VALUES(%s, %s, %s, %s, %s, %s, %s)
                     """)

# FIND SONGS

song_select = ("""
               SELECT 
                   s.song_id,
                   a.artist_id
               FROM songs s INNER JOIN artists a ON s.artist_id = a.artist_id
               WHERE s.title = %s AND a.name = %s AND s.duration = %s
               """)

# QUERY LISTS

create_table_queries = [user_table_create, artist_table_create, song_table_create, songplay_table_create, time_table_create]