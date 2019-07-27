from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    ui_color = '#F98866'
    songplay_table_insert = ("""
        INSERT INTO songplays (playid,start_time,userid,level,songid,artistid,sessionid,location,user_agent)
        SELECT Distinct
                md5(events.ts) songplay_id,
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

    @apply_defaults
    def __init__(self,                               
                 redshift_conn_id="",                
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)      
        self.redshift_conn_id = redshift_conn_id
       
    def execute(self, context):
        self.log.info('Loading into fact table Songplay!')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        facts_sql =LoadFactOperator.songplay_table_insert         
        redshift.run(facts_sql)
