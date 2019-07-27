#Below are the tables need to be create
CREATE_staging_events_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS staging_events (
	artist varchar(256),
	auth varchar(256),
	firstname varchar(256),
	gender varchar(256),
	iteminsession int4,
	lastname varchar(256),
	length numeric(18,0),
	"level" varchar(256),
	location varchar(256),
	"method" varchar(256),
	page varchar(256),
	registration numeric(18,0),
	sessionid int4,
	song varchar(256),
	status int4,
	ts int8,
	useragent varchar(256),
	userid int4)
"""

CREATE_staging_songs_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS staging_songs (
	num_songs int4,
	artist_id varchar(256),
	artist_name varchar(256),
	artist_latitude numeric(18,0),
	artist_longitude numeric(18,0),
	artist_location varchar(256),
	song_id varchar(256),
	title varchar(256),
	duration numeric(18,0),
	"year" int4)
"""

CREATE_artists_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS artists (
	artistid varchar(256) NOT NULL,
	name varchar(256),
	location varchar(256),
	lattitude numeric(18,0),
	longitude numeric(18,0)
    )
"""

CREATE_songplays_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS songplays (
	playid varchar(32) NOT NULL,
	start_time timestamp NOT NULL,
	userid int4 NOT NULL,
	"level" varchar(256),
	songid varchar(256),
	artistid varchar(256),
	sessionid int4,
	location varchar(256),
	user_agent varchar(256),
	CONSTRAINT songplays_pkey PRIMARY KEY (playid)
);
"""

CREATE_songs_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS songs (
	songid varchar(256) NOT NULL,
	title varchar(256),
	artistid varchar(256),
	"year" int4,
	duration numeric(18,0),
	CONSTRAINT songs_pkey PRIMARY KEY (songid)
);
"""

CREATE_users_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS users (
	userid int4 NOT NULL,
	first_name varchar(256),
	last_name varchar(256),
	gender varchar(256),
	"level" varchar(256),
	CONSTRAINT users_pkey PRIMARY KEY (userid)
);
"""

CREATE_time_TABLE_SQL = ("""
CREATE TABLE IF NOT EXISTS time(
start_time timestamp NOT NULL,
hour integer,
day integer,
week integer,
month integer,
year integer,
dayofweek integer)
""")

#Below are the insert statements
songplay_table_insert = ("""
        SELECT Distinct
                md5(events.start_time) songplay_id,
                events.start_time, 
                events.userid, 
                events.level, 
                songs.song_id, 
                songs.artist_id, 
                events.sessionid, 
                events.location, 
                events.useragent
                FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
            FROM staging_events
            WHERE page='NextSong') events
            LEFT JOIN staging_songs songs
            ON events.song = songs.title
                AND events.artist = songs.artist_name
                AND events.length = songs.duration
            WHERE (songs.song_id<>'' or songs.artist_id<>'')
            AND length(events.userid)>0
    """)

user_table_insert = ("""
        SELECT distinct userid, firstname, lastname, gender, level
        FROM staging_events
        WHERE page='NextSong'
    """)

song_table_insert = ("""
        SELECT distinct song_id, title, artist_id, year, duration
        FROM staging_songs
    """)

artist_table_insert = ("""
        SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM staging_songs
    """)

time_table_insert = ("""
        SELECT start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time), 
               extract(month from start_time), extract(year from start_time), extract(dayofweek from start_time)
        FROM songplays
    """)