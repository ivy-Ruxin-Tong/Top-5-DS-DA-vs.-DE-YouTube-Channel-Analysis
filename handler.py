import re
import googleapiclient.discovery
import googleapiclient.errors
import json
import pandas as pd
import isodate
from dateutil import parser
import datetime as dt
import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from nltk import pos_tag
nltk.download('stopwords')
nltk.download('punkt')
from nltk.stem import WordNetLemmatizer
from nltk.corpus import wordnet
import pymysql
from pymysql.constants import CLIENT
from sqlalchemy import create_engine



api_key = 'AIzaSyBoO6MSKPQ9baLdYL_CXnfHygBy_jf0nTg'
api_service_name = "youtube"
api_version = "v3"


data_coding_channel = {
# DS/DA
'Tina_Huang' : 'UC2UXDak6o7rBm23k3Vv5dww',
'Luke Barousse' : 'UCLLw7jmFsvfIVaUFsLs8mlQ',
'Thu Vu data analytics' : 'UCJQJAI7IjbLcpsjWdSzYz0Q',
'Alex The Analyst' : 'UC7cs8q-gJRlGwj4A8OmCmXg',
'Data Interview Pro' : 'UCAWsBMQY4KSuOuGODki-l7A',
# DE
'Andreas Kretz' : 'UCY8mzqqGwl5_bTpBY9qLMAA',
'Data with Zach' : 'UCAq9f7jFEA7Mtl3qOZy2h1A',
'Karolina Sowinska' : 'UCAxnMry1lETl47xQWABvH7g',
'E-Learning Bridge' : 'UCBGcs9XTL5U34oaSn_AsHqw',
'Seattle Data Guy' : 'UCmLGJ3VYBcfRaWbP6JLJcpA'}


DS_DA = ['UC2UXDak6o7rBm23k3Vv5dww','UCLLw7jmFsvfIVaUFsLs8mlQ','UCJQJAI7IjbLcpsjWdSzYz0Q','UC7cs8q-gJRlGwj4A8OmCmXg','UCAWsBMQY4KSuOuGODki-l7A']

youtube = googleapiclient.discovery.build(
    api_service_name, api_version, developerKey=api_key )

host = 'youtubedb.c0rhkwuqbdtc.us-west-2.rds.amazonaws.com'
port = 3306
user = 'admin'
password = 'Coco0326happy!12'
database = 'youtube'

stop_words = stopwords.words('english')
stop_words.extend(["thank","what's up","time","thing","best","ask","about","get","vs","vs.","find","help","real","work","website","also","almost","as","any","blow","mind","choice",'content','effective','non','frequently','ever','learn','change','sunday','2020','2021',"2022","p0","p1","p2","p3","p4","air","p5","p6","late","truth","solve",
"full","different","world","word","next","minute","month","year","get","go","self","story","super","really","better","measure","what's,","start","over","successfully",'success','succeed','begin','start','good','fresh','course','tip','pro',"3rd","beautiful","bad","improve","step","must","previous","44","34","24","solve","instal",
"people","person","according","real","great","stay","sane","come","point","favorite","idea","choose","halo","make","used","week","start","write","based","base",'connect','v','realize','realistic','beginner','intermediate','advance','advanced',"near","close","funny","w","no info","install","everything",
"land","easily","smoothly","personal","cant","end","like","tell","every","everyone","all","most","more","easy","simple","guy","crack","role","position",'want','useful','new','important','watch','youtube','know','other','worth','part','from','wali','alex','wild','youll','yes','no','try','episode','top','way','yet','worker','from','without','do','to','single','page',
'pages','update','updates','would','dont','donts','wins','winning','boy','boys','zhai','need','use','uses','using','knows','woudnt','live','first','short','shorts','parts','other','others','knew','show','shows','topic','topics','how','talk','needs','went',"intuition","disappointment","massive","32","1799","buy","feel",
'zero','1','2','3','4','5','6','7','8','9','10','15','who','in','door','area','veronica','worse','worst','wish','wannable','wohhoo','worksdont','hero','single','free','whats','level','freshers','monthly','bi-weekly','yealy',"level","hard","easy","swing","sub","back","despite",
'difference','hour','per','stop','lazy','like','hate','dislike','love','still','one','five','ten','two','unique','temp','try','summer','winter','type','unique','rural','face','matter','york','value','void','avoid','who','big','small','become','create',
'strong','type','understand','understood','weekly','take','took','suck','tell','talk','ultimate','wrong','year','avoid','upload','stand','upcoming','unblock','underrate','successful','gonna','build','actual','actually','amazing','custom','proper','random','im','maximize','minimize','outcome','recommend','fry','burn','give','havent',
"complete","reject","50","competitive","right","major","reach","20th","21st","pick","much","fast","online","consult","modern","last","break","reason","reality","replace","win","natural","x","west","eliza","load","legendary","video","dirty","chat","issue","final","run","luck","follow","lead","32","believe","gritty","ledengary","cut","west","return","massive"])

stop_words = set(stop_words)
wordnet_lemmatizer = WordNetLemmatizer()



def get_channel_info(youtube):
    request = youtube.channels().list(
    part="snippet,contentDetails,statistics",
    id=','.join(data_coding_channel[channel] for channel in data_coding_channel))
    response = request.execute()

    channel_data = []
    for channel in response['items']:
        channel_id = channel['id']
        channel_title = channel['snippet']['title']
        channel_description = channel['snippet']['description']
        channel_publish_at = channel['snippet']['publishedAt']
        channel_country = channel['snippet']['country']
        channel_view_count = channel['statistics']['viewCount']
        channel_subscriber_count = channel['statistics']['subscriberCount']
        channel_video_count = channel['statistics']['videoCount']
        channel_playlist_id = channel['contentDetails']['relatedPlaylists']['uploads']
        channel_element = {
            "channel_id" : channel_id,
            "channel_title" : channel_title,
            "channel_publish_at" : channel_publish_at,
            "channel_description" : channel_description,
            "channel_country" : channel_country, 
            "channel_view_count" : channel_view_count,
            "channel_subscriber_count" : channel_subscriber_count,
            "channel_video_count" : channel_video_count,
            'channel_playlist_id' :  channel_playlist_id }
        channel_data.append(channel_element)
    return pd.DataFrame(channel_data)

def get_playlist_info(youtube):
    playlist_df = pd.DataFrame()
    def get_individual_playlist_data(youtube, playlist_id):  
        request = youtube.playlistItems().list(
                            part="snippet,contentDetails",
                            maxResults = 50,
                            playlistId= playlist_id)
        response = request.execute()

        all_playlist_id = []
        all_video_id = []
        for playlist in response['items']:
            all_playlist_id.append(playlist["snippet"]['playlistId'])
            all_video_id.append(playlist['contentDetails']["videoId"])

        next_page_token = response.get('nextPageToken')
        more_pages = True

        while more_pages:
            if next_page_token is None:
                more_pages = False
            else:
                request = youtube.playlistItems().list(
                        part='snippet,contentDetails',
                        playlistId= playlist_id,
                        maxResults = 50,
                        pageToken = next_page_token)
                response = request.execute()

                for playlist in response['items']:
                    all_playlist_id.append(playlist["snippet"]['playlistId'])
                    all_video_id.append(playlist['contentDetails']["videoId"])
            
                next_page_token = response.get('nextPageToken')

        return pd.DataFrame({"playlist_id" : all_playlist_id, 
                            "video_id" : all_video_id})

    for playlist_id in get_channel_info(youtube)['channel_playlist_id'].tolist():
        playlist_df = playlist_df.append(get_individual_playlist_data(youtube, playlist_id))
    return playlist_df

def get_video_info(youtube):
    all_video = []
    video_ids = get_playlist_info(youtube)['video_id']
    for video_id in video_ids:
        request = youtube.videos().list(
                part="snippet,contentDetails,statistics",
                id= video_id
            )
        response = request.execute()
        for video in response['items']:
            stats = {'snippet' : ['publishedAt','title','description','tags'],
                    'statistics': ['viewCount','likeCount','favoriteCount','commentCount'],
                    'contentDetails': ['duration','definition','caption']}
            video_detail = {}
            video_detail['video_id'] = video['id']

            for stat in stats:
                for detail in stats[stat]:
                    try: 
                        video_detail[detail] = video[stat][detail]
                    except:
                        video_detail[detail] = None
            all_video.append(video_detail)

    return pd.DataFrame(all_video)

# stop_words.extend("I'm",'hello','')
def channel_data_processing(channel, category_list = DS_DA):
    numeric_cols = ['channel_view_count','channel_subscriber_count','channel_video_count']
    channel[numeric_cols] = channel[numeric_cols].apply(pd.to_numeric, errors = 'coerce', axis = 1)
    channel['channel_publish_at'] = channel['channel_publish_at'].apply(lambda x: parser.parse(x).date())
    channel['channel_category'] = channel['channel_id'].apply(lambda x: 'DS_DA' if x in category_list else 'DE') 
    channel['insert_date'] = pd.to_datetime("today").date()
    return channel

def playlist_data_processing(playlist):
    playlist['insert_date'] = pd.to_datetime("today").date()
    return playlist
    


# text = 'apple apples be successes successful succeed successfully one day stopped stops stopping weekly weeks week data science simulate simulation simulates visualization visuals visual, visually!'

def clean_video_title(title, stop_words = stop_words):

    lemma_word = []
    for w in title.lower().split():
        word1 = wordnet_lemmatizer.lemmatize(w.strip(), pos = "n")
        word2 = wordnet_lemmatizer.lemmatize(word1, pos = "v")
        word3 = wordnet_lemmatizer.lemmatize(word2, pos = ("a"))
        word4 = wordnet_lemmatizer.lemmatize(word3, pos = ("r"))
        lemma_word.append(word4)
        filtered_setence = [w for w in lemma_word if w not in stop_words]
    return filtered_setence

def video_data_processing(video, stop_words = stop_words):
    numeric_cols = ['viewCount','likeCount','favoriteCount','commentCount']
    video[numeric_cols] = video[numeric_cols].apply(pd.to_numeric, errors = 'coerce', axis = 1)
    video['tags_count'] = video['tags'].apply(lambda x : 0 if x is None else len(x))
    video['publishedAt'] = video['publishedAt'].apply(lambda x: parser.parse(x)) 
    video['publishDay'] = video['publishedAt'].apply(lambda x: x.strftime("%a")) 
    video['durationinSecs'] = video['duration'].apply(lambda x: isodate.parse_duration(x))
    video['durationinSecs'] = video['durationinSecs'].astype('timedelta64[s]')
    video['insert_date'] = pd.to_datetime("today").date()
    video['title_2'] = video['title'].apply(lambda x : re.sub(r'[^\w\s]','', x))
    video['title_word'] = video['title_2'].apply(lambda x : clean_video_title(x, stop_words=stop_words) )
    video = video.drop(columns =['duration'], axis = 1 )
    return video


def tag_processing(video):
    tags = video[['video_id','tags']].explode('tags').fillna('No Info')
    tags['insert_at'] = pd.to_datetime('today').date()
    tags = tags.reset_index(drop=True)
    return tags
def video_key_word(video):
    video_key_words = video[['video_id','title_word']].explode('title_word').fillna('no info').drop_duplicates()
    video_key_words['insert_at'] = pd.to_datetime('today').date()
    video_key_words = video_key_words.reset_index(drop=True)
    return video_key_words

def final_video(video):
    video = video.drop(['tags','title_word','title_2'], axis = 1)
    return video


def connect_rds_mysql(port, host, user, database):
    db = pymysql.connect(host = host, user = user, password=password, port = port, database= database, client_flag =  CLIENT.MULTI_STATEMENTS, autocommit = True )
    cursor = db.cursor()
    return cursor, db
    
def connect_engine(port, host, user, database ):
    engine = create_engine('mysql+pymysql://{user}:{password}@{host}:3306/{database}'.format(user = user, password = password, host = host, database = database))
    return engine
    
def channel_update(cursor, db, engine, channel):
    engine.connect().execute(
    """
    CREATE TEMPORARY TABLE IF NOT EXISTS tmp_channel AS SELECT * FROM channel LIMIT 0;
    """)
    channel.to_sql("tmp_channel", con = engine, if_exists='append', index = False)
    #Moving data from temp table to production table
    engine.connect().execute(
    """
    INSERT INTO channel
    SELECT *
    FROM tmp_channel
    ON DUPLICATE KEY 
    update channel_description = tmp_channel.channel_description,
    channel_country = tmp_channel.channel_country,
    channel_view_count = tmp_channel.channel_view_count,
    channel_subscriber_count = tmp_channel.channel_subscriber_count,
    channel_video_count = tmp_channel.channel_video_count,
    channel_playlist_id = tmp_channel.channel_playlist_id,
    channel_category = tmp_channel.channel_category,
    insert_date  = tmp_channel.insert_date;
    
    """)
    cursor.execute(
    """
   DROP TABLE tmp_channel;   
    """)
    db.commit()
def playlist_update(cursor, db, engine, playlist):  
    engine.connect().execute(
    """
    CREATE TEMPORARY TABLE IF NOT EXISTS tmp_playlist AS SELECT * FROM playlist LIMIT 0;
    """)
    playlist.to_sql("tmp_playlist", con = engine, if_exists='append', index = False)
    #Moving data from temp table to production table
    
    engine.connect().execute(
    """
    INSERT INTO playlist
    SELECT *
    FROM tmp_playlist 
    ON DUPLICATE KEY 
    update playlist_id  = tmp_playlist.playlist_id ,
    video_id  = tmp_playlist.video_id,
    insert_date  = tmp_playlist.insert_date;
    
    """)
    cursor.execute("""
    drop table tmp_playlist;""")
    db.commit()
def video_update(cursor, db, engine, video): 
    engine.connect().execute(
    """
    CREATE TEMPORARY TABLE IF NOT EXISTS tmp_video AS SELECT * FROM video LIMIT 0;
    """)
    db.commit()
    video.to_sql("tmp_video", con = engine, if_exists='append', index = False)
    #Moving data from temp table to production table
    engine.connect().execute(
    """
    INSERT INTO video
    SELECT *
    FROM tmp_video 
    ON DUPLICATE KEY 
    update description  = tmp_video.description ,
     viewCount  = tmp_video.viewCount,
     likeCount  = tmp_video.likeCount,
     favoriteCount   = tmp_video.favoriteCount ,
     commentCount   = tmp_video.commentCount ,
     insert_date    = tmp_video.insert_date ;

    """)
    cursor.execute("""  drop table tmp_video;""")
    db.commit()

def tags_update(cursor, db, engine, tags): 
    engine.connect().execute(
    """
    CREATE TEMPORARY TABLE IF NOT EXISTS tmp_tag AS SELECT * FROM tag LIMIT 0;
    """)
    tags.to_sql("tmp_tag", con = engine, if_exists='replace', index = False)
    #Moving data from temp table to production table
    engine.connect().execute(
    """
    INSERT INTO tag
    SELECT *
    FROM tmp_tag
    ON DUPLICATE KEY    
    update
    insert_at   = tmp_tag.insert_at ;

    """)
    cursor.execute("""  drop table tmp_tag;
""")
    db.commit()


def video_key_words_update(cursor, db, engine, video_key_words): 
    engine.connect().execute(
    """
    CREATE TEMPORARY TABLE IF NOT EXISTS tmp_video_key_word AS SELECT * FROM video_key_word LIMIT 0;
    """)
    video_key_words.to_sql("tmp_video_key_word", con = engine, if_exists='replace', index = False)
    #Moving data from temp table to production table
    engine.connect().execute(
    """
    INSERT INTO video_key_word
    SELECT *
    FROM tmp_video_key_word
    ON DUPLICATE KEY    
    update
    insert_at   = tmp_video_key_word.insert_at ;

    """)

    cursor.execute("""  drop table tmp_video_key_word;
""")
    db.commit()

def clean_video_key_words(cursor, db, engine):
    engine.connect().execute(
    """
    INSERT INTO clean_video_key_word
    SELECT channel_id,channel_title, channel_category, video_key_word.video_id, video_key_word.title_word, video_key_word.insert_at
    FROM channel join playlist on channel.channel_playlist_id = playlist.playlist_id
    join video_key_word on playlist.video_id = video_key_word.video_id
    ON DUPLICATE KEY    
    update
    insert_at  = video_key_word.insert_at ;

    """)
    db.commit() 
    engine.connect().close()

def lambda_handler(event, context):
    channel = channel_data_processing(get_channel_info(youtube))
    playlist = playlist_data_processing(get_playlist_info(youtube))
    video = video_data_processing(get_video_info(youtube))
    # print(video.head(1))
    tags = tag_processing(video)
    # print(tags.head(1))
    video_key_words = video_key_word(video)
    # print(video_key_words.head(1))
    video = final_video(video)
    # print(video.head(1))


    cursor, db = connect_rds_mysql(port, host, user, database)
    engine = connect_engine(port = port, host = host, user = user, database =database)
    channel_update(cursor, db, engine, channel)
    playlist_update(cursor, db, engine, playlist)
    video_update(cursor, db, engine, video)
    tags_update(cursor, db, engine, tags)
    video_key_words_update(cursor, db, engine, video_key_words)
    clean_video_key_words(cursor, db, engine)
    
