# Queries to drop tables

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# Queries to create tables

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays(
        songplay_id SERIAL CONSTRAINT songplay_primary PRIMARY KEY,
        start_time TIMESTAMP REFERENCES time (start_time),
        user_id INT REFERENCES users (user_id),
        level VARCHAR NOT NULL,
        song_id VARCHAR REFERENCES songs (song_id),
        artist_id VARCHAR REFERENCES artists (artist_id),
        session_id INT NOT NULL,
        location VARCHAR,
        user_agent TEXT
    )
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users(
        user_id INT CONSTRAINT user_primary PRIMARY KEY,
        first_name VARCHAR,
        last_name VARCHAR,
        gender CHAR(1),
        level VARCHAR NOT NULL, 
    )
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs(
        song_id VARCHAR CONSTRAINT song_primary PRIMARY KEY,
        title VARCHAR,
        artist_id VARCHAR REFERENCES artists (artist_id),
        year INT CHECK (year >= 0),
        duration FLOAT
    )
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists(
        artist_id VARCHAR CONSTRAINT artist_primary PRIMARY KEY,
        name VARCHAR,
        location VARCHAR,
        latitude DECIMAL(9,6),
        longitude DECIMAL(9,6)
    )
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time(
        start_time TIMESTAMP CONSTRAINT time_primary PRIMARY KEY,
        hour INT NOT NULL CHECK (hour >= 0),
        day INT NOT NULL CHECK (day >= 0),
        week INT NOT NULL CHECK (week >= 0),
        month INT NOT NULL CHECK (month >= 0),
        year INT NOT NULL CHECK (year >= 0),
        weekday VARCHAR NOT NULL
    )
""")

