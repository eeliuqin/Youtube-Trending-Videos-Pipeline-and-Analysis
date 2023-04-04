from utils import get_connection, insert_data
from youtube_video import *
import pandas as pd
import os

USER = os.environ["db_user"]
PWD = os.environ["db_password"]
HOST = os.environ["db_host"]
PORT = os.environ["db_port"]
DATABASE = os.environ["database"]
YOUTUBE_API_KEY = os.environ["youtube_api_key"]
RAPID_API_KEY = os.environ["rapid_api_key"]
TRENDING_RECORD_TABLE = os.environ["trending_record_table"]
TRENDING_VIDEO_TABLE = os.environ["trending_video_table"]
TRENDING_VIDEO_CHANNEL_TABLE = os.environ["trending_video_channel_table"]

def create_tables(connection):
    """ Create trending_record, trending_video, and trending_channel table """

    cursor = connection.cursor()
    query1 = f"""
        CREATE TABLE IF NOT EXISTS {TRENDING_RECORD_TABLE} (
            id INT AUTO_INCREMENT PRIMARY KEY,
            video_id VARCHAR(255) NOT NULL,
            channel_id VARCHAR(255) NOT NULL,
            `rank` INT,
            views_millions DECIMAL(10,2),
            extracted_at TIMESTAMP,
            INDEX video_id_idx (video_id),
            INDEX channel_id_idx (channel_id)
        )
    """
    query2 = f"""
        CREATE TABLE IF NOT EXISTS {TRENDING_VIDEO_TABLE} (
            id INT AUTO_INCREMENT PRIMARY KEY,
            video_id VARCHAR(255) NOT NULL,
            title VARCHAR(255),
            duration_sec DECIMAL(10,1),
            tags VARCHAR(255),
            category VARCHAR(255),
            published_at TIMESTAMP,
            UNIQUE KEY unique_video_id (video_id)  
        )
    """
    query3 = f"""
        CREATE TABLE IF NOT EXISTS {TRENDING_VIDEO_CHANNEL_TABLE} (
            id INT AUTO_INCREMENT PRIMARY KEY,
            channel_id VARCHAR(255) NOT NULL,
            channel_title VARCHAR(255),
            custom_url VARCHAR(255),
            country VARCHAR(255),
            published_at TIMESTAMP,
            UNIQUE KEY unique_channel_id (channel_id) 
        )
    """
    try:
        cursor.execute(query1)
        print(f"Create table if not exists: {TRENDING_RECORD_TABLE}")
        cursor.execute(query2)
        print(f"Create table if not exists:{TRENDING_VIDEO_TABLE}")
        cursor.execute(query3)
        print(f"Create table if not exists: {TRENDING_VIDEO_CHANNEL_TABLE}")
        
    except Exception as e:
        print(f"Found errors when creating tables: {e}")

    cursor.close()
    
    
def insert_record(connection, data):
    """ Insert into the table of records of trending videos """

    df_record = data[['video_id', 'channel_id', 'rank', 'views_millions', 'extracted_at']]
    insert_query = f"""
        INSERT INTO {TRENDING_RECORD_TABLE} (`video_id`, `channel_id`, `rank`, `views_millions`, `extracted_at`) 
        VALUES (%s, %s, %s, %s, %s)
    """

    insert_data(connection, df_record, insert_query)


def insert_video(connection, data):
    """ Insert into the table of trending videos """

    df_video = data[['video_id', 'title', 'duration_sec', 'tags', 'category', 'published_at']].copy()
    df_video['tags'] = [','.join(map(str, l)) for l in df_video['tags']]
    insert_query = f"""
        INSERT INTO {TRENDING_VIDEO_TABLE} (`video_id`, `title`, `duration_sec`, `tags`, `category`, `published_at`) 
        VALUES (%s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
        title=VALUES(title), duration_sec=VALUES(duration_sec), tags=VALUES(tags),
        category=VALUES(category), published_at=VALUES(published_at)
    """

    insert_data(connection, df_video, insert_query)

def insert_channel(connection, data):
    """ Insert into the table of channels of trending videos """

    df_channel = data[['channel_id', 'channel_title', 'custom_url', 'country', 'published_at']]
    insert_query = f"""
        INSERT INTO {TRENDING_VIDEO_CHANNEL_TABLE} (`channel_id`, `channel_title`, `custom_url`, `country`, `published_at`) 
        VALUES (%s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
        channel_id=VALUES(channel_id), channel_title=VALUES(channel_title), custom_url=VALUES(custom_url),
        country=VALUES(country), published_at=VALUES(published_at)
    """

    insert_data(connection, df_channel, insert_query)

def lambda_handler(event, context):
    # connect to MySQL
    connection = get_connection(
        host=HOST,
        database=DATABASE,
        port=PORT,
        user=USER,
        password=PWD
    )

    # get trending videos, channels
    yt = YoutubeVideo(YOUTUBE_API_KEY, RAPID_API_KEY)
    # get trending video ids
    trending_video_ids = yt.get_trending_ids()
    # get trending videos
    trending_videos = yt.get_all_videos(trending_video_ids)
    # get channels of those trending videos
    channel_ids = trending_videos["channel_id"].tolist()
    trending_videos_channels = yt.get_all_channels(channel_ids)

    print("Finished getting required data.")

    # create 3 tables (record, video, channel) if not exist
    create_tables(connection)
    # insert into 3 tables
    insert_record(connection, trending_videos)
    print("Finished inserting record")
    insert_video(connection, trending_videos)
    print("Finished inserting videos")
    insert_channel(connection, trending_videos_channels)
    print("Finished inserting channels")

    # close connection
    connection.close()
