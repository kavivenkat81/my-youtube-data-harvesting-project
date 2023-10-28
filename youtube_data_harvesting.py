import googleapiclient.discovery
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import streamlit as st
import pymongo

import pandas as pd
import numpy as np
import mysql.connector
import plotly.express as px
import datetime
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

API_KEY = "AIzaSyBN2RrZCqYkCWjnL1corhBSGf8DkrNsP48"

API_NAME = 'youtube'
API_VERSION = 'v3'
SCOPES = ['https://www.googleapis.com/auth/youtube.readonly']

# Function to create a YouTube API service client

youtube = googleapiclient.discovery.build('youtube', 'v3', developerKey=API_KEY)

# Set the title and page icon
st.set_page_config(
    page_title="YouTube Data Harvesting and Warehousing",
    page_icon="📊",
)

# Define the page layout
st.header(":rainbow[Welcome to YouTube Data Harvesting and Warehousing]",divider="rainbow")
st.write(
    "This is a web application for collecting and warehousing YouTube data."
)

# Create sections for different project components
st.header(":violet[Project Components]")
st.subheader(":blue[1. Data Harvesting]")
st.write("Collect data from YouTube using the YouTube Data API.")
st.subheader(":blue[2. Data Storing]")
st.write("Store the collected data in MongoDB Atlas.")
st.subheader(":blue[3. Data Warehousing]")
st.write("Migrate the data from MongoDB Atlas to MySql.")
st.subheader(":blue[4. Sql Queries]")
st.write("Generate reports and insights from the data.")
st.subheader(":blue[5. Data Analysis]")
st.write("Analyze and visualize the collected data using plotly charts.")


# Function to retrieve channel details
def get_channel_details(youtube, CHANNEL_ID):
    try:
        response = youtube.channels().list(
            part="contentDetails,snippet,statistics",
            id=CHANNEL_ID
        ).execute()


        if 'items' in response and len(response['items']) > 0:

            channel = response.get("items")[0]
            channel_name = response['items'][0]['snippet']['title']

            channel_info = response['items'][0]
            snippet = channel_info['snippet']
            statistics = channel_info['statistics']
            contentDetails = channel_info['contentDetails']

            channel_data = {
                'channel_id': channel_info['id'],
                'channel_title': snippet['title'],
                'channel_description': snippet['description'],
                'channel_published_date': snippet['publishedAt'],
                'total_views': statistics['viewCount'],
                'total_subscribers': statistics['subscriberCount'],
                'total_videos': statistics['videoCount'],
                'upload_id': contentDetails['relatedPlaylists']['uploads'],
                'country': snippet.get('country', 'N/A')  # Handle the case where 'country' is missing
            }

            st.write(f"**Channel Title:** {snippet['title']}")
            st.write(f"**Channel Description:** {snippet['description']}")
            st.write(f"**Channel Published Date:** {snippet['publishedAt']}")
            st.write(f"**Total Views:** {statistics['viewCount']}")
            st.write(f"**Total Subscribers:** {statistics['subscriberCount']}")
            st.write(f"**Total Videos:** {statistics['videoCount']}")
            st.write(f"**Upload_id:** {contentDetails['relatedPlaylists']['uploads']}")
            st.write(f"**Country:** {snippet['country']}")

            return channel_name,channel,channel_data
        else:
            print("No channel data found for Channel_id:", CHANNEL_ID)
            return None,[],[]


    except HttpError as e:
        st.write(f"No channel data found for Channel_id:", CHANNEL_ID)
        return None,[],[]


# Function to retrieve all playlist IDs for a channel
def get_playlist_ids(youtube, CHANNEL_ID):
    try:
        response = youtube.playlists().list(
            part="snippet,contentDetails,status",
            channelId=CHANNEL_ID,
            maxResults=10  # You can adjust this to retrieve more playlists if needed
        ).execute()

        if 'items' in response:
            playlist_info = response['items'][0]
            snippet = playlist_info['snippet']
            contentDetails = playlist_info['contentDetails']
            playlist_details=[]
        for i in range(0, len(response['items'])):
            data = {'playlist_id': response['items'][i]['id'],
                    'playlist_name': response['items'][i]['snippet']['title'],
                    'channel_id': CHANNEL_ID
                    }
            playlist_details.append(data)
            st.write(data)

        playlists = response.get("items", [])
        playlist_ids = [playlist["id"] for playlist in playlists]

        return playlist_ids,playlist_details

    except HttpError as e:
        st.write(f"An error occurred while retrieving playlist_id ")
        return None,[]


# Function to retrieve all videos in a playlist
def get_videos_in_playlist(youtube, playlist_id):
    try:
        response = youtube.playlistItems().list(
            part="contentDetails,snippet",
            playlistId=playlist_id,
            maxResults=20  # You can adjust this to retrieve more videos if needed
        ).execute()

        videos = response.get("items", [])

        noofvideo=len(videos)
        st.write("Total number of videos:",noofvideo)

        return videos,noofvideo

    except HttpError as e:
        st.write(f"video not found")
        return None,[]


# Function to retrieve video details and comments for a video
def get_video_details_and_comments(youtube, video_id):
    try:
        video_response = youtube.videos().list(
            part='contentDetails, snippet, statistics',
            id=video_id,
            maxResults=20
        ).execute()

        def time_duration(t):
            a = pd.Timedelta(t)
            b = str(a).split()[-1]
            return b

        video = video_response.get("items", [])
        if 'items' in video_response and len(video_response['items']) > 0:
            video_data = video_response['items'][0]
            stat_data = {
                'view_count': video_data['statistics']['viewCount'],
                'like_count': video_data['statistics'].get('likeCount', 0),
                # 'favourite_count': video_data['statistics']['favoriteCount'],
                'comment_count': video_data['statistics'].get('commentCount', 0),
                'duration': time_duration(video_data['contentDetails']['duration']),
                'caption_status': video_data['contentDetails']['caption']
            }

        else:
            # Handle the case where no video data is found
            stat_data = None
            print("No video data found for video_id:", video_id)

        comment_response = youtube.commentThreads().list(
            part="snippet",
            videoId=video_id,
            maxResults=50  # You can adjust this to retrieve more comments if needed
        ).execute()

        comments = comment_response.get("items", [])

        return video, comments, stat_data

    except HttpError as e:
        st.write(f"video not found")
        return None, [], []

def data_extraction_youtube(channel_name,CHANNEL_ID):


    data_youtube_list=[]
    data_youtube1=[]

    if channel_name:
        # Retrieve playlist IDs
        st.subheader("\nPlaylist IDs:")
        playlist_ids,playlist_details = get_playlist_ids(youtube, CHANNEL_ID)
        data_youtube_list.append({"playlist_ids": playlist_details})

        # Retrieve videos in each playlist and their details
        for playlist_id in playlist_ids:
            st.subheader(f"\nPlaylist: {playlist_id}")
            videos, noofvideo = get_videos_in_playlist(youtube, playlist_id)

            for video in videos:

                video_id = video["snippet"]["resourceId"]["videoId"]
                st.write("Video_Id:", video_id)

                video_details, comments, stat_data = get_video_details_and_comments(youtube, video_id)

                if video_details:
                    st.subheader("\nVideo Details:")
                    cap = {'true': 'Available', 'false': 'Not Available'}

                    data_youtube1 = []

                    data = {'video_id': video["snippet"]["resourceId"]['videoId'],
                            'video_name': video['snippet']['title'],
                            'video_description': video['snippet']['description'],
                            'published_date': video['snippet']['publishedAt'][0:10],
                            'published_time': video['snippet']['publishedAt'][11:19],
                            'thumbnail': video['snippet']['thumbnails']['default']['url'],
                            'channel_id':CHANNEL_ID}

                    stat1_data={"video_id":video_id}
                    stat1_data.update(stat_data)
                    st.write(data,"video statistics:",stat1_data)
                    data_youtube1.extend([{"video_details":data},{"video statistics":stat1_data}])
                    st.subheader("\nComments:")
                    data_youtube2=[]
                    for comment in comments:

                        data1 = {'comment_id': comment['id'],
                                 'comment_text': comment['snippet']['topLevelComment']['snippet'][
                                     'textDisplay'],
                                 'comment_author': comment['snippet']['topLevelComment']['snippet'][
                                     'authorDisplayName'],
                                 'comment_published_date': comment['snippet']['topLevelComment']['snippet'][
                                                               'publishedAt'][
                                                           0:10],
                                 'comment_published_time': comment['snippet']['topLevelComment']['snippet'][
                                                               'publishedAt'][
                                                           11:19],
                                 'video_id': video_id}
                        st.write(data1)
                        data_youtube2.append({"comments":data1})

                    data_youtube1.append({"overall_comments":data_youtube2})

                data_youtube_list.append({"video-id":data_youtube1})

    st.success("Data retrieved successfully",icon="💯")
    return data_youtube_list


def list_mongodb_collection_names(database):
    uri = "mongodb+srv://venkatkavi71:kavitha@kavi.zvzqs4r.mongodb.net/test?retryWrites=true&w=majority"
    # Create a new client and connect to the server
    client = MongoClient(uri, server_api=ServerApi('1'))
    db = client[database]
    col = db.list_collection_names()
    col.sort(reverse=False)
    return col

def mongodb(database,channel_data,channel_name,data_extraction):

    uri = "mongodb+srv://venkatkavi71:kavitha@kavi.zvzqs4r.mongodb.net/test?retryWrites=true&w=majority"
    # Create a new client and connect to the server
    client1 = MongoClient(uri, server_api=ServerApi('1'))
    # Send a ping to confirm a successful connection
    try:
        client1.admin.command('ping')
        st.success("Pinged your deployment. You successfully connected to MongoDB!")

    except Exception as e:
        st.write(e)
    uri = "mongodb+srv://venkatkavi71:kavitha@kavi.zvzqs4r.mongodb.net/test?retryWrites=true&w=majority"
    # Create a new client and connect to the server
    client = MongoClient(uri, server_api=ServerApi('1'))
    data_youtube = {}

    db=client[database]
    col1 = db[channel_name]

    for i in col1.find():
        data_youtube.update(i)

    data_youtube = {"channel":channel_data}

    list_collections_name = list_mongodb_collection_names(database)



    if channel_name not in list_collections_name:

             data_youtube.update({"channel Details":data_extraction})
             col1.insert_one(data_youtube)
             st.write(data_youtube)

             st.success("The data has been successfully stored in the MongoDB database",icon="🚨")
             st.balloons()


    else:
            st.warning("The data has already been stored in MongoDB database")

#---------------------------------------Data Migration------------------------------------------------------------------

# Create Database and Table in SQL
def sql_create_tables():

    host = "localhost"
    user = "root"
    password = "kavitha"
    database = "youtube_project_data"


    # Create a connection to the MySQL server
    connection = mysql.connector.connect(
           host=host,
           user=user,
           password=password,
           database=database
    )


    cursor = connection.cursor()

        # Execute SQL queries here
    cursor.execute("SELECT VERSION()")
    data = cursor.fetchone()
    st.write("MySQL Server Version:", data)

    cursor.execute("""CREATE TABLE IF NOT EXISTS  channel(channel_id                VARCHAR(255) PRIMARY KEY,
                                                          channel_title             VARCHAR(255),
                                                          channel_description       TEXT,
                                                          channel_published_date    DATE,
                                                          total_views               INT,
                                                          total_subscribers         INT,
                                                          total_videos              INT,
                                                          upload_id                 VARCHAR(255),
                                                          country                   VARCHAR(255))""")

    cursor.execute("""CREATE TABLE IF NOT EXISTS playlist(playlist_id		VARCHAR(255) PRIMARY KEY,
                                                          playlist_name	    VARCHAR(255),
                                                          channel_id		VARCHAR(255))""")

    cursor.execute("""CREATE TABLE IF NOT EXISTS video(
                                        video_id			VARCHAR(255),
                                        video_name			VARCHAR(255),
                                        video_description	TEXT,
                                        published_date		DATE,
                                        published_time		TIME,
                                        thumbnail			VARCHAR(255),
                                        channel_id          VARCHAR(255))""")

    cursor.execute("""CREATE TABLE IF NOT EXISTS video_stat(
                                            video_id			VARCHAR(255),    
                                            view_count			INT,
                                            like_count			INT,
                                            comment_count		INT,
                                            duration			TIME,
                                            caption_status		VARCHAR(255))""")


    cursor.execute("""CREATE TABLE IF NOT EXISTS comment(
                                        comment_id				VARCHAR(255), 
                                        comment_text			TEXT,
                                        comment_author			VARCHAR(255),
                                        comment_published_date	DATE,
                                        comment_published_time	TIME,
                                        video_id				VARCHAR(255))""")

    connection.commit()


# SQL channel names list
def list_sql_channel_names():
    host = "localhost"
    user = "root"
    password = "kavitha"
    database ="youtube_project_data"

    # Create a connection to the MySQL server
    connection = mysql.connector.connect(
        host=host,
        user=user,
        password=password,
        database=database
    )
    cursor = connection.cursor()


    try:
        cursor.execute("SELECT channel_title FROM channel")
        s = cursor.fetchall()
        print(s)
    except Exception as e:
         print("Error:", e)

    s = [i[0] for i in s]
    s.sort(reverse=False)
    return s


# display the all channel names from SQL channel table

# data migrating to channel table
def sql_channel(database, col_input):
    uri = "mongodb+srv://venkatkavi71:kavitha@kavi.zvzqs4r.mongodb.net/test?retryWrites=true&w=majority"
    # Create a new client and connect to the server
    client = MongoClient(uri, server_api=ServerApi('1'))
    db = client[database]
    col = db[col_input]

    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)

    data = []
    for i in col.find({}, {'_id': 0, 'channel': 1}):
        data.append(i['channel'])


    channel = pd.DataFrame(data)
    st.write("pd",channel)

    channel = channel.reindex(columns=['channel_id', 'channel_title','channel_description','channel_published_date',
                                       'total_views','total_subscribers','total_videos',
                                       'upload_id', 'country'])
    channel['channel_published_date'] = pd.to_datetime(channel['channel_published_date']).dt.date
    channel['total_subscribers'] = pd.to_numeric(channel['total_subscribers'])
    channel['total_views'] = pd.to_numeric(channel['total_views'])
    return channel



# data migrating to playlist table
def sql_playlists(database, col_input):
    uri = "mongodb+srv://venkatkavi71:kavitha@kavi.zvzqs4r.mongodb.net/test?retryWrites=true&w=majority"
    # Create a new client and connect to the server
    client = MongoClient(uri, server_api=ServerApi('1'))
    db = client[database]
    col = db[col_input]

    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    playlist_info=[]
    # Use the find method to retrieve data from the collection
    for doc in col.find({}, {'_id': 0, 'channel Details': 1}):

        channel_details = doc.get('channel Details', [])


        # Process each channel_details entry
        for entry in channel_details:
            if 'playlist_ids' in entry:
                    playlist_ids = entry['playlist_ids']
                    for playlist_data in playlist_ids:
                        playlist_id = playlist_data.get('playlist_id', '')
                        playlist_name = playlist_data.get('playlist_name', '')
                        channel_id = playlist_data.get('channel_id', '')
                        playlist_info.append((playlist_id, playlist_name, channel_id))

    # Create a pandas DataFrame from the extracted data
    playlist_ids = pd.DataFrame(playlist_info, columns=['playlist_id', 'playlist_name', 'channel_id'])

    # Now you have a pandas DataFrame with your data
    st.write(playlist_ids)

    playlist_ids = playlist_ids.reindex(columns=['playlist_id', 'playlist_name', 'channel_id'])
    return playlist_ids


# data migrating to video table
def sql_videos(database, col_input):
    uri = "mongodb+srv://venkatkavi71:kavitha@kavi.zvzqs4r.mongodb.net/test?retryWrites=true&w=majority"
    # Create a new client and connect to the server
    client = MongoClient(uri, server_api=ServerApi('1'))
    db = client[database]
    col = db[col_input]

    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)


    for doc in col.find({}, {'_id': 0, 'channel Details': 1}):
        channel_details = doc.get('channel Details', [])

        all_videos = []
        all_video_statistics = []
        # Iterate through the data and extract video details
        for item in channel_details:
            # Check if the item is a dictionary (contains video details)
            if isinstance(item, dict) and "video-id" in item:
                videos = item["video-id"]

                for video_data in videos:
                    # Check if the video_data is a dictionary (contains video details)
                    if isinstance(video_data, dict):
                        video_details = video_data.get("video_details", {})
                        video_id = video_details.get("video_id", "")
                        video_name = video_details.get("video_name", "")
                        video_description = video_details.get("video_description", "")
                        published_date = video_details.get("published_date", "")
                        published_time = video_details.get("published_time", "")
                        thumbnail = video_details.get("thumbnail", "")
                        channel_id = video_details.get("channel_id", "")
                        video_stat = video_data.get("video statistics", {})
                        video_id1 = video_stat.get("video_id", "")
                        view_count = video_stat.get("view_count", "")
                        like_count = video_stat.get("like_count", "")
                        #favourite_count = video_stat.get("favourite_count", "")
                        comment_count = video_stat.get("comment_count", "")
                        duration = video_stat.get("duration", "")
                        caption_status = video_stat.get("caption_status", "")

                        # Create a dictionary for each video
                        video = {
                            "video_id": video_id,
                            "video_name": video_name,
                            "video_description": video_description,
                            "published_date": published_date,
                            "published_time": published_time,
                            "thumbnail": thumbnail,
                            "channel_id":channel_id
                        }
                        video_statistics = {
                            "video_id": video_id1,
                            "view_count":view_count,
                            "like_count":like_count,
                            #"favourite_count":favourite_count,
                            "comment_count":comment_count,
                            "duration":duration,
                            "caption_status":caption_status
                        }

                        # Append the video details to the list
                        all_videos.append(video)
                        all_video_statistics.append(video_statistics)
        video_id = pd.DataFrame(all_videos)
        video_statistics=pd.DataFrame(all_video_statistics)
        video_id = video_id.replace('', np.nan)
        video_statistics = video_statistics.replace('', np.nan)

        # Now, drop rows with NaN values
        video_id_cleaned = video_id.dropna()
        video_statistics_cleaned = video_statistics.dropna()

        # Reset the index if needed
        video_id_cleaned = video_id_cleaned.reset_index(drop=True)
        video_statistics_cleaned = video_statistics_cleaned.reset_index(drop=True)

        # Print the cleaned DataFrames
        st.write("pd", video_id_cleaned)
        st.write("pd", video_statistics_cleaned)


    videos = video_id_cleaned.reindex(
        columns=['video_id', 'video_name', 'video_description', 'published_date', 'published_time',
                 'thumbnail','channel_id'])
    video_statistics = video_statistics_cleaned.reindex(
                 ['video_id','view_count', 'like_count','comment_count', 'duration',
                  'caption_status'])

    videos['published_date'] = pd.to_datetime(videos['published_date']).dt.date
    videos['published_time'] = pd.to_datetime(videos['published_time'], format='%H:%M:%S').dt.time
    video_statistics['view_count'] = pd.to_numeric(video_statistics['view_count'])
    video_statistics['like_count'] = pd.to_numeric(video_statistics['like_count'])

    video_statistics['comment_count'] = pd.to_numeric(video_statistics['comment_count'])
    video_statistics['duration'] = pd.to_datetime(video_statistics['duration'], format='%H:%M:%S').dt.time

    return videos,video_statistics_cleaned


# data migrating to comment table
def sql_comments(database, option_sql):
    uri = "mongodb+srv://venkatkavi71:kavitha@kavi.zvzqs4r.mongodb.net/test?retryWrites=true&w=majority"
    # Create a new client and connect to the server
    client = MongoClient(uri, server_api=ServerApi('1'))
    db = client[database]
    col_input=option_sql
    col = db[col_input]

    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)

    for doc in col.find({}, {'_id': 0, 'channel Details': 1}):
        channel_details = doc.get('channel Details', [])
        #print("Channel Details:", channel_details)
        all_comments = []
        data_videos = []

        # Iterate through the data and extract comments
        for item in channel_details:
            # Check if the item is a dictionary
            if isinstance(item, dict) and "video-id" in item:
                      videos = item["video-id"]

                      for video_data in videos:
                          # Check if the video_data is a dictionary (contains video details)
                          if isinstance(video_data, dict) and "overall_comments" in video_data:
                              comments_data = video_data["overall_comments"]
                              print("comments_data", comments_data)
                              print("type(comments_data)",type(comments_data))

                              for comment_data in comments_data:
                                  if isinstance(comment_data, dict) and "comments" in comment_data:
                                      comment_details = comment_data["comments"]
                                      print("hai",type(comment_details))
                                      comment_id = comment_details.get("comment_id", "")
                                      comment_text = comment_details.get("comment_text", "")
                                      comment_author = comment_details.get("comment_author", "")
                                      comment_published_date = comment_details.get("comment_published_date", "")
                                      comment_published_time = comment_details.get("comment_published_time", "")
                                      video_id = comment_details.get("video_id", "")


                                  # Create a dictionary for each comment
                                      comment_info = {
                                        "comment_id": comment_id,
                                        "comment_text": comment_text,
                                        "comment_author": comment_author,
                                        "comment_published_date": comment_published_date,
                                        "comment_published_time": comment_published_time,
                                        "video_id": video_id
                                      }
                                      print("comment_info",comment_info)




                                  # Append the comment details to the list
                                  all_comments.append(comment_info)

                          # Create DataFrame
        comments = pd.DataFrame(all_comments)
        comments = comments.replace('', np.nan)
        comments_cleaned = comments.dropna()
        comments_cleaned = comments_cleaned.reset_index(drop=True)
        st.write(comments_cleaned)


    comments = comments_cleaned.reindex(columns=['comment_id', 'comment_text', 'comment_author',
                                     'comment_published_date', 'comment_published_time', 'video_id'])
    comments['comment_published_date'] = pd.to_datetime(comments['comment_published_date']).dt.date
    comments['comment_published_time'] = pd.to_datetime(comments['comment_published_time'], format='%H:%M:%S').dt.time
    return comments



def sql(database,col_input):
    st.write("connecting to mysql")
    sql_create_tables()
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)

    channel = sql_channel(database, col_input)
    playlists = sql_playlists(database, col_input)
    videos, video_statistics_cleaned = sql_videos(database, col_input)
    comments = sql_comments(database, col_input)

    connection = mysql.connector.connect(
        host="localhost",
        user="root",
        password="kavitha",
        database="youtube_project_data"
    )
    cursor = connection.cursor()
    cursor.executemany("""INSERT INTO channel(channel_id, channel_title, channel_description,
                                                            channel_published_date, total_views,
                                                            total_subscribers, total_videos, upload_id, country)
                                                            VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                       channel.values.tolist())
    print(cursor.rowcount, "records inserted successfully")

    cursor.executemany("""INSERT INTO playlist(playlist_id, playlist_name, channel_id)
                                                          VALUES(%s,%s,%s)""", playlists.values.tolist())
    videos = videos.where(pd.notna(videos), None)
    cursor.executemany("""INSERT INTO video(video_id, video_name, video_description, published_date,
                                               published_time, thumbnail,channel_id) VALUES(%s,%s,%s,%s,%s,%s,%s)""",
                       videos.values.tolist())
    comments = comments.dropna()
    cursor.executemany("""INSERT INTO comment(comment_id, comment_text, comment_author, comment_published_date,
                                                                         comment_published_time, video_id) VALUES(%s,%s,%s,%s,%s,%s)""",
                       comments.values.tolist())

    print(cursor.rowcount, "comment records inserted successfully")


    try:

        # Convert NaN values to None for insertion into MySQL
        #video_statistics_cleaned = video_statistics_cleaned.where(pd.notna(video_statistics_cleaned), None)
        for row in video_statistics_cleaned.values.tolist():
            print('r',row)

        # Insert cleaned data into the database
        cursor.executemany("""INSERT INTO video_stat(video_id, view_count, like_count, comment_count, duration, caption_status)
                                  VALUES (%s, %s, %s, %s, %s, %s)""",
                           video_statistics_cleaned.values.tolist())


        print(cursor.rowcount, "video records inserted successfully")


    except mysql.connector.Error as err:
        print("Error:", err)



    connection.commit()
    print(cursor.rowcount, "records inserted successfully")

    st.success("Migrated Data Successfully to SQL Data Warehouse",icon="🎉")
    st.balloons()
    connection.close()

    # Main function to retrieve and print channel details, playlists, videos, and comments


def query1():
    connection = mysql.connector.connect(
        host="localhost",
        user="root",
        password="kavitha",
        database="youtube_project_data"
    )
    cursor = connection.cursor()
    cursor.execute('SELECT video.video_name, channel.channel_title\
                    FROM  video\
                    INNER JOIN  channel ON video.channel_id = channel.channel_id\
                    ORDER BY channel.channel_title ASC')



    s = cursor.fetchall()
    i = [i for i in range(1, len(s) + 1)]
    pd.set_option('display.max_columns', None)
    data1 = pd.DataFrame(s, columns=['Video Names', 'Channel Names'], index=i)
    st.dataframe(data1)

    return data1


def query2():
    connection = mysql.connector.connect(
        host="localhost",
        user="root",
        password="kavitha",
        database="youtube_project_data"
    )
    cursor = connection.cursor()
    cursor.execute("select distinct channel.channel_title, count(distinct video.video_id) as total\
                    FROM video\
                    INNER JOIN  channel ON channel.channel_id = video.channel_id\
                    GROUP BY channel.channel_title,channel.channel_id\
                    ORDER BY total DESC"))
    s = cursor.fetchall()
    i = [i for i in range(1, len(s) + 1)]
    pd.set_option('display.max_columns', None)
    data2 = pd.DataFrame(s, columns=['Channel Names', 'Total Videos'], index=i)

    return data2


def query3():
    connection = mysql.connector.connect(
        host="localhost",
        user="root",
        password="kavitha",
        database="youtube_project_data"
    )
    cursor = connection.cursor()
    cursor.execute("SELECT distinct v.video_name, vs.view_count, c.channel_title \
                    FROM video AS v \
                    INNER JOIN video_stat AS vs ON v.video_id = vs.video_id \
                    INNER JOIN channel AS c ON v.channel_id = c.channel_id \
                    ORDER BY vs.view_count DESC \
                    LIMIT 10")
    s = cursor.fetchall()

    i = [i for i in range(1, len(s) + 1)]
    pd.set_option('display.max_columns', None)
    data3 = pd.DataFrame(s, columns=['Video Names', 'Total Views', 'Channel Names'], index=i)
    st.dataframe(data3)
    return data3


def query4():
    connection = mysql.connector.connect(
        host="localhost",
        user="root",
        password="kavitha",
        database="youtube_project_data"
    )
    cursor = connection.cursor()
    cursor.execute("SELECT distinct v.video_name, vs.comment_count, c.channel_title \
                    FROM video AS v \
                    INNER JOIN video_stat AS vs ON v.video_id = vs.video_id \
                    INNER JOIN channel AS c ON v.channel_id = c.channel_id \
                    ORDER BY vs.comment_count DESC \
                    LIMIT 10")
    s = cursor.fetchall()
    i = [i for i in range(1, len(s) + 1)]
    pd.set_option('display.max_columns', None)
    data4 = pd.DataFrame(s, columns=['Video Names', 'Total Comments', 'Channel Names'], index=i)
    st.dataframe(data4)
    return data4


def query5():
    connection = mysql.connector.connect(
        host="localhost",
        user="root",
        password="kavitha",
        database="youtube_project_data"
    )
    cursor = connection.cursor()
    cursor.execute("SELECT v.video_name, vs.like_count, c.channel_title \
                    FROM video AS v \
                    INNER JOIN video_stat AS vs ON v.video_id = vs.video_id \
                    INNER JOIN channel AS c ON v.channel_id = c.channel_id \
                    WHERE vs.like_count = (SELECT max(like_count) FROM video_stat)")
    s = cursor.fetchall()
    i = [i for i in range(1, len(s) + 1)]
    pd.set_option('display.max_columns', None)
    data5 = pd.DataFrame(s, columns=['Video Names', 'Most Likes', 'Channel Names'], index=i)
    st.dataframe(data5)

    return data5


def query6():
    connection = mysql.connector.connect(
        host="localhost",
        user="root",
        password="kavitha",
        database="youtube_project_data"
    )
    cursor = connection.cursor()
    cursor.execute("SELECT distinct v.video_name, vs.like_count, c.channel_title \
                    FROM video AS v \
                    INNER JOIN video_stat AS vs ON v.video_id = vs.video_id \
                    INNER JOIN channel AS c ON v.channel_id = c.channel_id \
                    GROUP BY v.video_id, c.channel_title \
                    ORDER BY vs.like_count DESC \
                    LIMIT 10")

    s = cursor.fetchall()
    i = [i for i in range(1, len(s) + 1)]
    pd.set_option('display.max_columns', None)
    data6 = pd.DataFrame(s, columns=['Video Names', 'Total Likes', 'Channel Names'], index=i)
    st.dataframe(data6)
    return data6


def query7():
    connection = mysql.connector.connect(
        host="localhost",
        user="root",
        password="kavitha",
        database="youtube_project_data"
    )
    cursor = connection.cursor()
    cursor.execute("SELECT channel_title, total_views FROM channel\
                    ORDER BY total_views DESC")
    s = cursor.fetchall()
    i = [i for i in range(1, len(s) + 1)]
    data7 = pd.DataFrame(s, columns=['Channel Names', 'Total Views'], index=i)
    st.dataframe(data7)
    return data7


def query8(year):
    connection = mysql.connector.connect(
        host="localhost",
        user="root",
        password="kavitha",
        database="youtube_project_data"
    )
    cursor = connection.cursor()
    cursor.execute('SELECT channel.channel_title, count(video.video_id) AS total\
         FROM video\
         INNER JOIN channel on channel.channel_id = video.channel_id\
         WHERE extract(year FROM video.published_date) = \'' + str(year) + '\'\
         GROUP BY channel.channel_title\
         ORDER BY total DESC')
    s = cursor.fetchall()
    i = [i for i in range(1, len(s) + 1)]
    data8 = pd.DataFrame(s, columns=['Channel Names', 'Published Videos'], index=i)
    st.dataframe(data8)
    return data8


def query9():
    connection = mysql.connector.connect(
        host="localhost",
        user="root",
        password="kavitha",
        database="youtube_project_data"
    )
    cursor = connection.cursor()
    cursor.execute("SELECT  channel.channel_title, SUBSTRING(CAST(AVG(vs.duration) AS CHAR), 1, 8) AS average\
                   FROM video_stat AS vs \
                   INNER JOIN  video AS v ON v.video_id = vs.video_id\
                   INNER JOIN  channel ON channel.channel_id = v.channel_id\
                   GROUP BY channel.channel_title\
                   ORDER BY  average DESC;")


    s = cursor.fetchall()
    i = [i for i in range(1, len(s) + 1)]
    data9 = pd.DataFrame(s, columns=['Channel Names', 'Average Video Duration'], index=i)
    st.dataframe(data9)
    return data9


def query10():
    connection = mysql.connector.connect(
        host="localhost",
        user="root",
        password="kavitha",
        database="youtube_project_data"
    )
    cursor = connection.cursor()
    cursor.execute('SELECT v.video_name, vs.comment_count, channel.channel_title\
                     FROM video AS v\
                     INNER JOIN video_stat AS vs ON v.video_id = vs.video_id \
                     INNER JOIN channel ON channel.channel_id = v.channel_id\
                     ORDER BY vs.comment_count DESC\
                     LIMIT 1')
    s = cursor.fetchall()
    i = [i for i in range(1, len(s) + 1)]
    pd.set_option('display.max_columns', None)
    data10 = pd.DataFrame(s, columns=['Video Names', 'Channel Names', 'Total Comments'], index=i)
    st.dataframe(data10)
    return data10


def main():
    st.subheader(":rainbow[YouTube Channel Details]")

    # Input field for the channel ID
    CHANNEL_ID = st.text_input("Enter the YouTube Channel ID:")
    # channel_name,channel,channel_data = get_channel_details(youtube, CHANNEL_ID)
    # Display channel details when the user clicks the "Get Details" button

    if st.button("Get Details"):

        channel_name, channel, channel_data = get_channel_details(youtube, CHANNEL_ID)
        database = "youtube_data_collection"
        data_extraction = data_extraction_youtube(channel_name, CHANNEL_ID)
        #st.write(data_extraction)

        st.caption(':green[_Store Extracted Data in MongoDB Atlas_]')
        mongodb(database, channel_data, channel_name, data_extraction)
        #st.write("Data successfully stored in MongoDb Atlas")

        col_input=channel_name
        sql(database,col_input)

        st.title(':violet[_Data Visualisation Using Bar and Pie Chart_]')

        st.subheader(':blue[Names of all the videos and their corresponding channels]',divider='violet')

        data1 = query1()
        st.subheader(':rainbow[Channel wise Videos]',divider='rainbow')
        data2 = query2()
        data2_sorted = data2.sort_values(by='Total Videos', ascending=True)
        fig = px.bar(data2_sorted, x='Total Videos', y='Channel Names', template='seaborn')
        fig.update_traces(text=data2_sorted['Total Videos'], textposition='outside')
        colors = px.colors.qualitative.Plotly
        fig.update_traces(marker=dict(color=colors[:len(data2_sorted)]))
        st.plotly_chart(fig, use_container_width=True)

        fig = px.pie(data2_sorted, names='Channel Names', values='Total Videos', hole=0.5)
        fig.update_traces(text=data2_sorted['Channel Names'], textinfo='percent+label', texttemplate='%{percent:.2%}',
                          textposition='outside',
                          textfont=dict(color='white'))
        st.plotly_chart(fig, use_container_width=True)

        st.subheader(':blue[Top view count videos with their names]', divider='violet')
        data3 = query3()
        st.subheader(':rainbow[Highest video view count]',divider='rainbow')
        data3_sorted = data3.sort_values(by='Total Views', ascending=True)
        col3, col4 = st.columns([1, 2])

        with col4:

            fig = px.bar(data3_sorted, x='Total Views', y='Video Names', template='seaborn')
            fig.update_traces(text=data3_sorted['Total Views'], textposition='outside')
            colors = px.colors.qualitative.Plotly
            fig.update_traces(marker=dict(color=colors[:len(data3_sorted)]))
            st.plotly_chart(fig, use_container_width=True)

            fig = px.pie(data3_sorted, names='Video Names', values='Total Views', hole=0.5)
            fig.update_traces(text=data3_sorted['Video Names'], textinfo='percent+label', texttemplate='%{percent:.2%}',
                              textposition='outside',
                              textfont=dict(color='white'))
            st.plotly_chart(fig, use_container_width=True)

        st.subheader(':blue[Videos with highest comment count and their channel names]', divider='violet')
        data4 = query4()
        st.subheader(':rainbow[Total comments for each video of  all channel]',divider='violet')
        data4_sorted = data4.sort_values(by='Total Comments', ascending=True)
        col7, col8 = st.columns([1, 2])

        with col8:
            fig = px.bar(data4_sorted, x='Total Comments', y='Video Names', template='seaborn')
            fig.update_traces(text=data4_sorted['Total Comments'], textposition='outside')
            colors_set1 = px.colors.qualitative.Set1
            fig.update_traces(marker=dict(color=colors_set1[:len(data4_sorted)]))
            st.plotly_chart(fig, use_container_width=True)

            fig = px.pie(data4_sorted, names='Video Names', values='Total Comments', hole=0.5)
            fig.update_traces(text=data4_sorted['Video Names'], textinfo='percent+label', texttemplate='%{percent:.2%}',
                              textposition='outside',
                              textfont=dict(color='white'))
            st.plotly_chart(fig, use_container_width=True)

        st.subheader(':blue[Videos with highest likes and their channel names]', divider='violet')
        data5 = query5()
        st.subheader(':rainbow[Highest likes on video]',divider='rainbow')
        data5_sorted = data5.sort_values(by='Most Likes', ascending=True)
        col9, col10 = st.columns([1, 2])

        with col10:
            fig = px.bar(data5_sorted, x='Most Likes', y='Video Names', template='seaborn')
            fig.update_traces(text=data5_sorted['Most Likes'], textposition='outside')
            colors = px.colors.qualitative.Plotly
            fig.update_traces(marker=dict(color=colors[:len(data5_sorted)]))
            st.plotly_chart(fig, use_container_width=True)

        st.subheader(':blue[Total likes for each video with channel name]', divider='violet')
        data6 = query6()
        st.subheader(':rainbow[Total likes on video]',divider='rainbow')
        data6_sorted = data6.sort_values(by='Total Likes', ascending=True)
        col11, col12 = st.columns([1, 2])

        with col12:
            fig = px.bar(data6_sorted, x='Total Likes', y='Video Names', template='seaborn')
            fig.update_traces(text=data6_sorted['Total Likes'], textposition='outside')
            colors = px.colors.qualitative.Plotly
            fig.update_traces(marker=dict(color=colors[:len(data6_sorted)]))
            st.plotly_chart(fig, use_container_width=True)

            fig = px.pie(data6_sorted, names='Video Names', values='Total Likes', hole=0.5)
            fig.update_traces(text=data6_sorted['Video Names'], textinfo='percent+label', texttemplate='%{percent:.2%}',
                              textposition='outside',
                              textfont=dict(color='white'))
            st.plotly_chart(fig, use_container_width=True)

        st.subheader(':blue[Total views on each channel]', divider='violet')
        data7 = query7()
        st.subheader(':rainbow[Total views on channel]',divider='rainbow')
        data7_sorted = data7.sort_values(by='Total Views', ascending=True)
        col13, col14 = st.columns([1, 2])

        with col14:
            fig = px.bar(data7_sorted, x='Total Views', y='Channel Names', template='seaborn')
            fig.update_traces(text=data7_sorted['Total Views'], textposition='outside')
            colors_pastel1 = px.colors.qualitative.Pastel1
            fig.update_traces(marker=dict(color=colors_pastel1[:len(data7_sorted)]))
            st.plotly_chart(fig, use_container_width=True)

            fig = px.pie(data7_sorted, names='Channel Names', values='Total Views', hole=0.5)
            fig.update_traces(text=data7_sorted['Channel Names'], textinfo='percent+label', texttemplate='%{percent:.2%}',
                              textposition='outside',
                              textfont=dict(color='white'))
            st.plotly_chart(fig, use_container_width=True)

        st.subheader(':blue[Total Published videos on channel in current year]', divider='violet')
        data8 = query8(2023)
        st.subheader(':rainbow[Total Published videos on channel]',divider='rainbow')
        data8_sorted = data8.sort_values(by='Published Videos', ascending=True)
        col15, col16 = st.columns([1, 2])

        with col16:
            fig = px.bar(data8_sorted, x='Published Videos', y='Channel Names', template='seaborn')
            fig.update_traces(text=data8_sorted['Published Videos'], textposition='outside')
            colors_dark24 = px.colors.qualitative.Dark24
            fig.update_traces(marker=dict(color=colors_dark24[:len(data8_sorted)]))
            st.plotly_chart(fig, use_container_width=True)

            fig = px.pie(data8_sorted, names='Channel Names', values='Published Videos', hole=0.5)
            fig.update_traces(text=data8_sorted['Channel Names'], textinfo='percent+label', texttemplate='%{percent:.2%}',
                              textposition='outside',
                              textfont=dict(color='white'))
            st.plotly_chart(fig, use_container_width=True)

        st.subheader(':blue[average video duration on each channel]', divider='violet')
        data9 = query9()
        st.subheader(':rainbow[Average Duration of videos]',divider='rainbow')
        data9_sorted = data9.sort_values(by='Average Video Duration', ascending=True)
        col17, col18 = st.columns([1, 2])

        with col18:
            fig = px.bar(data9_sorted, x='Channel Names', y='Average Video Duration', template='seaborn')
            fig.update_traces(text=data9_sorted['Average Video Duration'], textposition='outside')
            colors_set1 = px.colors.qualitative.Set1
            fig.update_traces(marker=dict(color=colors_set1[:len(data9_sorted)]))
            st.plotly_chart(fig, use_container_width=True)

            fig = px.pie(data9_sorted, names='Channel Names', values='Average Video Duration', hole=0.5)
            fig.update_traces(text=data9_sorted['Channel Names'], textinfo='percent+label', texttemplate='%{percent:.2%}',
                              textposition='outside',
                              textfont=dict(color='white'))
            st.plotly_chart(fig, use_container_width=True)

        st.subheader(':blue[Highest video comment count with channel name]', divider='violet')
        data10 = query10()
        st.subheader(':rainbow[Highest comment count of a video]',divider='rainbow')
        data10_sorted = data10.sort_values(by='Total Comments', ascending=True)
        col19, col20 = st.columns([1, 2])

        with col20:
            fig = px.bar(data10_sorted, x='Total Comments', y='Channel Names', template='seaborn')
            fig.update_traces(text=data10_sorted['Channel Names'], textposition='outside')
            colors_dark24 = px.colors.qualitative.Dark24
            fig.update_traces(marker=dict(color=colors_dark24[:len(data10_sorted)]))
            st.plotly_chart(fig, use_container_width=True)


            fig = px.scatter(
                data10_sorted,
                x='Total Comments',
                y='Channel Names',
                color_discrete_sequence=colors_dark24,
                title='Scatter Plot')
            st.plotly_chart(fig, use_container_width=True)



if __name__ == "__main__":
    main()
