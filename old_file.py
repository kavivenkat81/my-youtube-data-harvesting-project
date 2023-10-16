import os
import json
import googleapiclient.discovery
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import streamlit as st
import pymongo
import pandas as pd
import mysql.connector
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

# Replace with your own API key or OAuth 2.0 credentials
API_KEY = "AIzaSyBN2RrZCqYkCWjnL1corhBSGf8DkrNsP48"

#CHANNEL_ID = 'UCiEmtpFVJjpvdhsQ2QAhxVA'

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
st.title("Welcome to YouTube Data Harvesting and Warehousing")
st.write(
    "This is a web application for collecting and warehousing YouTube data."
)

# Create sections for different project components
st.header("Project Components")
st.markdown("1. Data Harvesting")
st.write("Collect data from YouTube using the YouTube Data API.")
st.markdown("2. Data Storing")
st.write("Store the collected data in MongoDB Atlas.")
st.markdown("4. Data Warehousing")
st.write("Migrate the data from MongoDB Atlas to MySql.")
st.markdown("4. Data Analysis")
st.write("Analyze and visualize the collected data.")
st.markdown("5. Sql Queries")
st.write("Generate reports and insights from the data.")


# Function to retrieve channel details
def get_channel_details(youtube, CHANNEL_ID):
    try:
        response = youtube.channels().list(
            part="contentDetails,snippet,statistics",
            id=CHANNEL_ID
        ).execute()

        channel = response.get("items")[0]
        channel_name = response['items'][0]['snippet']['title']
        if 'items' in response and len(response['items']) > 0:
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
            return channel_name,channel,channel_data
        else:
            print("No channel data found for CHANNEL_ID:", CHANNEL_ID)
            return None,[],[]





        st.write(f"**Channel Title:** {snippet['title']}")
        st.write(f"**Channel Description:** {snippet['description']}")
        st.write(f"**Channel Published Date:** {snippet['publishedAt']}")
        st.write(f"**Total Views:** {statistics['viewCount']}")
        st.write(f"**Total Subscribers:** {statistics['subscriberCount']}")
        st.write(f"**Total Videos:** {statistics['videoCount']}")
        st.write(f"**Upload_id:** {contentDetails['relatedPlaylists']['uploads']}")
        st.write(f"**Country:** {snippet['country']}")
        #upload_id = {contentDetails['relatedPlaylists']['uploads']}
        return channel_name,channel,channel_data

    except HttpError as e:
        st.write(f"An error occurred: {e}")
        return None


# Function to retrieve all playlist IDs for a channel
def get_playlist_ids(youtube, CHANNEL_ID):
    try:
        response = youtube.playlists().list(
            part="snippet,contentDetails,status",
            channelId=CHANNEL_ID,
            maxResults=5  # You can adjust this to retrieve more playlists if needed
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
        #print(data1)


        playlists = response.get("items", [])

        playlist_ids = [playlist["id"] for playlist in playlists]

        return playlist_ids,playlist_details

    except HttpError as e:
        st.write(f"An error occurred: {e}")
        return None,[]


# Function to retrieve all videos in a playlist
def get_videos_in_playlist(youtube, playlist_id):
    try:
        response = youtube.playlistItems().list(
            part="contentDetails,snippet",
            playlistId=playlist_id,
            maxResults=5  # You can adjust this to retrieve more videos if needed
        ).execute()

        videos = response.get("items", [])

        noofvideo=len(videos)
        st.write("Total number of videos:",noofvideo)

        return videos,noofvideo

    except HttpError as e:
        st.write(f"An error occurred: {e}")
        return None,[]


# Function to retrieve video details and comments for a video
def get_video_details_and_comments(youtube, video_id):
    try:
        video_response = youtube.videos().list(
            part='contentDetails, snippet, statistics',
            id=video_id,
            maxResults=5
        ).execute()

        def time_duration(t):
            a = pd.Timedelta(t)
            b = str(a).split()[-1]
            return b

        video = video_response.get("items", [])
        if 'items' in video_response and len(video_response['items']) > 0:
            video_data = video_response['items'][0]
            stat1data = {
                'view_count': video_data['statistics']['viewCount'],
                'like_count': video_data['statistics'].get('likeCount', 0),
                'favourite_count': video_data['statistics']['favoriteCount'],
                'comment_count': video_data['statistics'].get('commentCount', 0),
                'duration': time_duration(video_data['contentDetails']['duration']),
                'caption_status': video_data['contentDetails']['caption']
            }
            print("video", stat1data)
        else:
            # Handle the case where no video data is found
            stat1data = None
            print("No video data found for video_id:", video_id)

        comment_response = youtube.commentThreads().list(
            part="snippet",
            videoId=video_id,
            maxResults=5  # You can adjust this to retrieve more comments if needed
        ).execute()

        comments = comment_response.get("items", [])

        return video, comments, stat1data

    except HttpError as e:
        st.write(f"An error occurred: {e}")
        return None, [], []

def data_extraction_youtube(channel_name,CHANNEL_ID):


    data_youtube_list=[]
    data_youtube1=[]
    #data_youtube_list.append({"channel_name":channel_name})

    if channel_name:
        # Retrieve playlist IDs
        st.subheader("\nPlaylist IDs:")
        playlist_ids,playlist_details = get_playlist_ids(youtube, CHANNEL_ID)
        data_youtube_list.append({"playlist_ids":playlist_details})

        # Retrieve videos in each playlist and their details
        for playlist_id in playlist_ids:
            st.subheader(f"\nPlaylist: {playlist_id}")
            videos, noofvideo = get_videos_in_playlist(youtube, playlist_id)

            for video in videos:

                video_id = video["snippet"]["resourceId"]["videoId"]
                st.write("Video_Id:", video_id)
                #data_youtube1.append(video_id)

                video_details, comments, stat1data = get_video_details_and_comments(youtube, video_id)
                print(stat1data)
                if video_details:
                    st.subheader("\nVideo Details:")
                    cap = {'true': 'Available', 'false': 'Not Available'}
                    data_youtube1 = []

                    data = {'video_id': video["snippet"]["resourceId"]['videoId'],
                            'video_name': video['snippet']['title'],
                            'video_description': video['snippet']['description'],
                            'published_date': video['snippet']['publishedAt'][0:10],
                            'published_time': video['snippet']['publishedAt'][11:19],
                            'thumbnail': video['snippet']['thumbnails']['default']['url']}

                    st.write(data,"video statistics:",stat1data)
                    data_youtube1.append(data)
                    data_youtube1.append(stat1data)
                    st.subheader("\nComments:")

                    i=0
                    for comment in comments:


                        data1 = {"comments":{'comment_id': comment['id'],
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
                                 'video_id': video_id}}

                        st.write(data1)
                        data_youtube1.append(data1)

                data_youtube_list.append(data_youtube1)

    st.write("Data retrieved successfully")
    st.write(data_youtube_list)
    return data_youtube_list

#Function to store collected data in Mongodb
def list_mongodb_collection_names(database):
    uri = "mongodb+srv://venkatkavi71:kavitha@kavi.zvzqs4r.mongodb.net/test?retryWrites=true&w=majority"
    # Create a new client and connect to the server
    client = MongoClient(uri, server_api=ServerApi('1'))
    db = client[database]
    col = db.list_collection_names()
    col.sort(reverse=False)
    return col


def data_store_mongodb(channel_name, database, data_youtube,CHANNEL_ID):
    uri = "mongodb+srv://venkatkavi71:kavitha@kavi.zvzqs4r.mongodb.net/test?retryWrites=true&w=majority"
    # Create a new client and connect to the server
    client = MongoClient(uri, server_api=ServerApi('1'))
    db = client[database]
    col = db[channel_name]
    st.write("datastore",db[channel_name])
    #col.insert_one(data_youtube)
    #st.write(data_youtube)
    data_youtube1 = data_extraction_youtube(database,CHANNEL_ID)
    #st.write(data_youtube1)
    data_youtube.update(data_youtube1)
    #st.write(data_youtube)
    col.insert_one(data_youtube)
    st.write("data_store",data_youtube)



def mongodb(database,channel_data,channel_name,CHANNEL_ID,data_extraction):


    uri = "mongodb+srv://venkatkavi71:kavitha@kavi.zvzqs4r.mongodb.net/test?retryWrites=true&w=majority"
    # Create a new client and connect to the server
    client1 = MongoClient(uri, server_api=ServerApi('1'))
    # Send a ping to confirm a successful connection
    try:
        client1.admin.command('ping')
        st.success("Pinged your deployment. You successfully connected to MongoDB!")

    except Exception as e:
        print(e)
    uri = "mongodb+srv://venkatkavi71:kavitha@kavi.zvzqs4r.mongodb.net/test?retryWrites=true&w=majority"
    # Create a new client and connect to the server
    client = MongoClient(uri, server_api=ServerApi('1'))
    data_youtube = {}
    #database = "youtube_data_collection"
    db=client[database]
    col1 = db[channel_name]
    print(col1)
    for i in col1.find():
        data_youtube.update(i)
    #print(data_youtube)
    data_youtube = {"channel":channel_data}

    #print("mongodb",data_youtube)
    list_collections_name = list_mongodb_collection_names(database)
    #print(list_collections_name)


    if channel_name not in list_collections_name:

             #data_youtube_list = data_extraction_youtube(channel,CHANNEL_ID)
             # data_youtube1.update(data_youtube)
             # data_youtube2=data_youtube1.update(data_youtube_list)
             data_youtube.update({"channel details":data_extraction})
             col1.insert_one(data_youtube)
             st.write(data_youtube)

             st.success("The data has been successfully stored in the MongoDB database")
             st.balloons()

    else:
            st.warning("The data has already been stored in MongoDB database")
            option = st.radio('Do you want to overwrite the data currently stored?',
                              ['Select the option below', 'Yes', 'No'])
            if option == 'Yes':
                uri = "mongodb+srv://venkatkavi71:kavitha@kavi.zvzqs4r.mongodb.net/test?retryWrites=true&w=majority"
                # Create a new client and connect to the server
                client = MongoClient(uri, server_api=ServerApi('1'))
                db = client[database]
                db[channel_name].drop()
                data_store_mongodb(channel_name, database, data_youtube,CHANNEL_ID)
                st.success("The data has been successfully overwritten and updated in MongoDB database")
                st.balloons()

            elif option == 'No':
                st.info("The data overwrite process has been skipped")
#---------------------------------------Data Migration------------------------------------------------------------------

# Create Database and Table in SQL
def sql_create_tables():

    host = "localhost"
    user = "root"
    password = "kavitha"
    database = "myd1"


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
                                        video_id			VARCHAR(255) PRIMARY KEY,
                                        video_name			VARCHAR(255),
                                        video_description	TEXT,
                                        published_date		DATE,
                                        published_time		TIME,
                                        thumbnail			VARCHAR(255),
                                        view_count			INT,
                                        like_count			INT,
                                        favourite_count		INT,
                                        comment_count		INT,
                                        duration			TIME,
                                        caption_status		VARCHAR(255))""")


    cursor.execute("""CREATE TABLE IF NOT EXISTS comment(
                                        comment_id				VARCHAR(255) PRIMARY KEY,
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
    database ="myd1"

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
    all_videos = []
    for doc in col.find({}, {'_id': 0, 'channel Details': 1}):
        channel_details = doc.get('channel Details', [])

        all_videos = []

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
                        video_stat = video_data.get("video statistics", {})
                        view_count = video_stat.get("view_count", "")
                        like_count = video_stat.get("like_count", "")
                        favourite_count = video_stat.get("favourite_count", "")
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
                            "view_count":view_count,
                            "like_count":like_count,
                            "favourite_count":favourite_count,
                            "comment_count":comment_count,
                            "duration":duration,
                            "caption_status":caption_status
                        }

                        # Append the video details to the list
                        all_videos.append(video)

        video_id = pd.DataFrame(all_videos)
    st.write("pd", video_id)


    videos = video_id.reindex(
        columns=['video_id', 'video_name', 'video_description', 'published_date', 'published_time','thumbnail',
                 'view_count', 'like_count', 'favourite_count', 'comment_count', 'duration',
                 'caption_status', 'comments'])

    videos['published_date'] = pd.to_datetime(videos['published_date']).dt.date
    videos['published_time'] = pd.to_datetime(videos['published_time'], format='%H:%M:%S').dt.time
    videos['view_count'] = pd.to_numeric(videos['view_count'])
    videos['like_count'] = pd.to_numeric(videos['like_count'])
    videos['favourite_count'] = pd.to_numeric(videos['favourite_count'])
    videos['comment_count'] = pd.to_numeric(videos['comment_count'])
    videos['duration'] = pd.to_datetime(videos['duration'], format='%H:%M:%S').dt.time
    videos.drop(columns='comments', inplace=True)
    return videos


# data migrating to comment table
def sql_comments(database, col_input):
    uri = "mongodb+srv://venkatkavi71:kavitha@kavi.zvzqs4r.mongodb.net/test?retryWrites=true&w=majority"
    # Create a new client and connect to the server
    client = MongoClient(uri, server_api=ServerApi('1'))
    db = client[database]
    col = db[col_input]

    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)

    data_videos = []
    for i in col.find({}, {'_id': 0, 'channel details': 1,}):
        data_videos.append(i.values())

    videos = pd.DataFrame(data_videos[0])
    videos = videos.reindex(
        columns=['video_id', 'video_name', 'video_description', 'upload_id', 'tags', 'published_date', 'published_time',
                 'view_count', 'like_count', 'favourite_count', 'comment_count', 'duration', 'thumbnail',
                 'caption_status', 'comments'])

    videos['published_date'] = pd.to_datetime(videos['published_date']).dt.date
    videos['published_time'] = pd.to_datetime(videos['published_time'], format='%H:%M:%S').dt.time
    # videos['view_count'] = pd.to_numeric(videos['view_count'])
    #videos['like_count'] = pd.to_numeric(videos['like_count'])
    #videos['favourite_count'] = pd.to_numeric(videos['favourite_count'])
    #videos['comment_count'] = pd.to_numeric(videos['comment_count'])
    #videos['duration'] = pd.to_datetime(videos['duration'], format='%H:%M:%S').dt.time

    data = []
    for i in videos['comments'].tolist():
        if isinstance(i, dict):
            data.extend(list(i.values()))
        else:
            pass

    comments = pd.DataFrame(data)
    st.write(comments)
    comments = comments.reindex(columns=['comment_id', 'comment_text', 'comment_author',
                                         'comment_published_date', 'comment_published_time', 'video_id'])
    comments['comment_published_date'] = pd.to_datetime(comments['comment_published_date']).dt.date
    comments['comment_published_time'] = pd.to_datetime(comments['comment_published_time'], format='%H:%M:%S').dt.time
    return comments



def sql(database):
    st.write("connecting to mysql")
    sql_create_tables()
    m = list_mongodb_collection_names(database)
    s = list_sql_channel_names()
    if s == m == []:
        st.info("Both Mongodb and SQL databases are currently empty")
    else:
        list_mongodb_notin_sql = ['Select the option']
        m = list_mongodb_collection_names(database)
        s = list_sql_channel_names()
        for i in m:
            if i not in s:
                list_mongodb_notin_sql.append(i)
                print(list_mongodb_notin_sql)
        option_sql = st.selectbox('', list_mongodb_notin_sql)
        st.write(option_sql)
        if option_sql:
                col_input = "ISRO Official"

                pd.set_option('display.max_rows', None)
                pd.set_option('display.max_columns', None)

                channel = sql_channel(database, col_input)
                playlists = sql_playlists(database, col_input)
                videos = sql_videos(database, col_input)
                comments = sql_comments(database, col_input)

                connection = mysql.connector.connect(
                    host="localhost",
                    user="root",
                    password="kavitha",
                    database="myd1"
                )
                cursor = connection.cursor()

                cursor.executemany("""INSERT INTO channel(channel_id, channel_title, channel_description,
                                      channel_published_date, total_views,
                                      total_subscribers,total_videos, upload_id, country)
                                      VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                                      channel.values.tolist())
                print(cursor.rowcount, "records inserted successfully")
                cursor.executemany("""INSERT INTO playlist(playlist_id, playlist_name, channel_id)
                                                    VALUES(%s,%s,%s)""", playlists.values.tolist())
                cursor.executemany("""INSERT INTO video(video_id, video_name, video_description, tags, published_date,
                                                    published_time, thumbnail)
                                                    VALUES(%s,%s,%s,%s,%s,%s,%s)""",
                                   videos.values.tolist())
                cursor.executemany("""INSERT INTO comment(comment_id, comment_text, comment_author, comment_published_date,
                                                          comment_published_time, video_id) VALUES(%s,%s,%s,%s,%s,%s)""",
                                   comments.values.tolist())

                connection.commit()
                print(cursor.rowcount, "records inserted successfully")
                st.success("Migrated Data Successfully to SQL Data Warehouse")
                st.balloons()
                connection.close()

# Main function to retrieve and print channel details, playlists, videos, and comments
def main():
  st.title("YouTube Channel Details")

    # Input field for the channel ID
  CHANNEL_ID = st.text_input("Enter the YouTube Channel ID:")
  channel_name,channel,channel_data = get_channel_details(youtube, CHANNEL_ID)
    # Display channel details when the user clicks the "Get Details" button
  if st.button("Get Details"):
      data_youtube = {}
      database = "youtube_data_collection"
      data_extraction = data_extraction_youtube(channel_name,CHANNEL_ID)
      st.write(data_extraction)

      # channel_name = data_youtube['channel_name']['channel_name']
      mongodb(database,channel_data,channel_name,CHANNEL_ID,data_extraction)
      st.write("Data successfully stored in MongoDb Atlas")
      sql(database)


if __name__ == "__main__":
    main()
