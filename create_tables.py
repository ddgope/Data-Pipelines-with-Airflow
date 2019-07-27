CREATE TABLE IF NOT EXISTS artists (
	artistid varchar(256) NOT NULL,
	name varchar(256),
	location varchar(256),
	lattitude numeric(18,0),
	longitude numeric(18,0)
 );
 
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
 
CREATE TABLE IF NOT EXISTS songs (
	songid varchar(256) NOT NULL,
	title varchar(256),
	artistid varchar(256),
	"year" int4,
	duration numeric(18,0),
	CONSTRAINT songs_pkey PRIMARY KEY (songid)
 );
 
CREATE TABLE IF NOT EXISTS users (
	userid int4 NOT NULL,
	first_name varchar(256),
	last_name varchar(256),
	gender varchar(256),
	"level" varchar(256),
	CONSTRAINT users_pkey PRIMARY KEY (userid)
); 


CREATE TABLE IF NOT EXISTS time(
start_time timestamp NOT NULL,
hour integer,
day integer,
week integer,
month integer,
year integer,
dayofweek integer)

