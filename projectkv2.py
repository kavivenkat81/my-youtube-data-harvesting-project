import os
import json
import googleapiclient.discovery
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import streamlit as st
import pymongo
import pandas as pd

from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

# Replace with your own API key or OAuth 2.0 credentials
API_KEY = "AIzaSyBN2RrZCqYkCWjnL1corhBSGf8DkrNsP48"
# If using OAuth 2.0 credentials, set the OAuth client secrets file path here:
#CLIENT_SECRETS_FILE = "client_secrets.json"
# Channel ID for the channel you want to retrieve details for
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


        channel = response.get("items", [])[0]
        channel_name= response['items'][0]['snippet']['title']
        if 'items' in response:
            channel_info = response['items'][0]
            snippet = channel_info['snippet']
            statistics = channel_info['statistics']
            contentDetails = channel_info['contentDetails']

        st.write(f"**Channel Title:** {snippet['title']}")
        st.write(f"**Channel Description:** {snippet['description']}")
        st.write(f"**Channel Published Date:** {snippet['publishedAt']}")
        st.write(f"**Total Views:** {statistics['viewCount']}")
        st.write(f"**Total Subscribers:** {statistics['subscriberCount']}")
        st.write(f"**Total Videos:** {statistics['videoCount']}")
        st.write(f"**Upload_id:** {contentDetails['relatedPlaylists']['uploads']}")
        st.write(f"**Country:** {snippet['country']}")
        upload_id = {contentDetails['relatedPlaylists']['uploads']}
        return channel,channel_name

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

        for i in range(0, len(response['items'])):
            data = {'playlist_id': response['items'][i]['id'],
                    'playlist_name': response['items'][i]['snippet']['title'],
                    'channel_id': CHANNEL_ID
                    }

            st.write(data)

        playlists = response.get("items", [])

        playlist_ids = [playlist["id"] for playlist in playlists]
        return playlist_ids

    except HttpError as e:
        st.write(f"An error occurred: {e}")
        return []


# Function to retrieve all videos in a playlist
def get_videos_in_playlist(youtube, playlist_id):
    try:
        response = youtube.playlistItems().list(
            part="snippet",
            playlistId=playlist_id,
            maxResults=5  # You can adjust this to retrieve more videos if needed
        ).execute()

        videos = response.get("items", [])

        noofvideo=len(videos)
        st.write("Total number of videos:",noofvideo)

        return videos,noofvideo

    except HttpError as e:
        st.write(f"An error occurred: {e}")
        return []


# Function to retrieve video details and comments for a video
def get_video_details_and_comments(youtube, video_id):
    try:
        video_response = youtube.videos().list(
            part="snippet,statistics",
            id=video_id,
            maxResults=5
        ).execute()


        video = video_response.get("items", [])


        comment_response = youtube.commentThreads().list(
            part="snippet",
            videoId=video_id,
            maxResults=5  # You can adjust this to retrieve more comments if needed
        ).execute()

        comments = comment_response.get("items", [])
        return video, comments

    except HttpError as e:
        st.write(f"An error occurred: {e}")
        return None, []

def data_extraction_youtube(CHANNEL_ID):
    channel_data = get_channel_details(youtube, CHANNEL_ID)
    #upload_id = channel_data['upload_id']

    playlist_ids = get_playlist_ids(youtube,CHANNEL_ID)
    #total_video_ids = get_total_video_ids(youtube, upload_id)
    for playlist_id in playlist_ids:
       videos,noofvideo = get_videos_in_playlist(youtube, playlist_id)
       print(videos)
    merge = {}
    video_with_comments = {}
    v = 1

    for i in videos:
        video_data,comments_data = get_video_details_and_comments(youtube, i)
        st.write(video_data)
        merge.update(video_data)
        #if int(video_data['comment_count']) > 0:
        #comments_data = get_comments_details(youtube, i)
        merge['comments'] = comments_data
        v1 = 'video_id_' + str(v)
        video_with_comments[v1] = merge
        v += 1
        merge = {}

    final = {'channel_name': channel_data, 'playlists': playlist_ids}
    final.update(video_with_comments)
    print(final)
    return final

#Function to store collected data in Mongodb
def list_mongodb_collection_names(database):
    uri = "mongodb+srv://venkatkavi71:kavitha@kavi.zvzqs4r.mongodb.net/test?retryWrites=true&w=majority"
    # Create a new client and connect to the server
    client = MongoClient(uri, server_api=ServerApi('1'))
    db = client[database]
    col = db.list_collection_names()
    col.sort(reverse=False)
    return col


def data_store_mongodb(channel_name, database, data_youtube):
    uri = "mongodb+srv://venkatkavi71:kavitha@kavi.zvzqs4r.mongodb.net/test?retryWrites=true&w=majority"
    # Create a new client and connect to the server
    client = MongoClient(uri, server_api=ServerApi('1'))
    db = client[database]
    col = db[channel_name]
    st.write("datastore",db[channel_name])
    col.insert_one(data_youtube)
    st.write("data_store",data_youtube)



def mongodb(database,channel_name):


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
    for i in col1.find():
       data_youtube.update(i)
    print(data_youtube)
    list_collections_name = list_mongodb_collection_names(database)
    print(list_collections_name)
    if channel_name not in list_collections_name:
            data_store_mongodb(channel_name, database, data_youtube)
            st.success("The data has been successfully stored in the MongoDB database")
            st.balloons()
            #temp_collection_drop()
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
                data_store_mongodb(channel_name, database, data_youtube)
                st.success("The data has been successfully overwritten and updated in MongoDB database")
                st.balloons()
               # temp_collection_drop()
            elif option == 'No':
                #temp_collection_drop()


                st.info("The data overwrite process has been skipped")
"""
def sql(database):
    st.write("connected to mysql")
    sql_create_tables()
    m = list_mongodb_collection_names(database)
    s = sql_channel(database,"Zenclass from Guvi")
    if s==[]:
        st.info("sql is emppty")
        st.write(s)
    if s == m == []:
        st.info("Both Mongodb and SQL databases are currently empty")

    else:
        m = list_mongodb_collection_names(database)
        s = sql_channel(database,"Zenclass from Guvi")
        for i in m:
            if i not in s:
                list_mongodb_notin_sql.append(i)

        option_sql = st.selectbox('', list_mongodb_notin_sql)
        if option_sql:

            if option_sql == 'Select the option':
                st.warning('Please select the channel')
            else:
                col_input ="Zenclass from Guvi"

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
                    database="my_database3"
                )
                cursor = connection.cursor()

                cursor.executemany("insert into channel(channel_id, channel_name, subscription_count, channel_views,\
                                                    channel_description, upload_id, country) values(%s,%s,%s,%s,%s,%s,%s)",
                                   channel.values.tolist())
                cursor.executemany("insert into playlist(playlist_id, playlist_name, channel_id, upload_id)\
                                                    values(%s,%s,%s,%s)", playlists.values.tolist())
                cursor.executemany("insert into video(video_id, video_name, video_description, upload_id, tags, published_date,\
                                                    published_time, view_count, like_count, favourite_count, comment_count, duration, thumbnail,\
                                                    caption_status) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                                   videos.values.tolist())
                cursor.executemany("insert into comment(comment_id, comment_text, comment_author, comment_published_date,\
                                                    comment_published_time, video_id) values(%s,%s,%s,%s,%s,%s)",
                                   comments.values.tolist())

                connection.commit()
                st.success("Migrated Data Successfully to SQL Data Warehouse")
                st.balloons()
                connection.close()
"""
# Main function to retrieve and print channel details, playlists, videos, and comments
def main():
    st.title("YouTube Channel Details")

    # Input field for the channel ID
    CHANNEL_ID = st.text_input("Enter the YouTube Channel ID:")

    # Display channel details when the user clicks the "Get Details" button
    if st.button("Get Details"):

    # Retrieve channel details
      if CHANNEL_ID:
          channel,channel_name = get_channel_details(youtube, CHANNEL_ID)
          print(channel_name)
          if channel:
        #st.write(json.dumps(channel, indent=2))

        # Retrieve playlist IDs
            st.subheader("\nPlaylist IDs:")
            playlist_ids = get_playlist_ids(youtube, CHANNEL_ID)

        #st.write(playlist_ids)

        # Retrieve videos in each playlist and their details
            for playlist_id in playlist_ids:
               st.subheader(f"\nPlaylist: {playlist_id}")
               videos,noofvideo = get_videos_in_playlist(youtube, playlist_id)
               for video in videos:
                   video_id = video["snippet"]["resourceId"]["videoId"]
                   st.write("Video_Id:",video_id)
                   video_details, comments = get_video_details_and_comments(youtube, video_id)
                   if video_details:
                       st.subheader("\nVideo Details:")
                       cap = {'true': 'Available', 'false': 'Not Available'}
                       def time_duration(t):
                           a = pd.Timedelta(t)
                           b = str(a).split()[-1]
                           return b



                       data = {'video_id': video["snippet"]["resourceId"]['videoId'],
                               'video_name': video['snippet']['title'],
                               'video_description': video['snippet']['description'],
                               'tags': video['snippet'].get('tags', []),
                               'published_date': video['snippet']['publishedAt'][0:10],
                               'published_time': video['snippet']['publishedAt'][11:19],
                             #  'view_count': video['statistics']['viewCount'],
                            # 'like_count': video['statistics'].get('likeCount', 0),
                            # 'favourite_count': video['statistics']['favoriteCount'],
                            #'comment_count': video['statistics'].get('commentCount', 0),
                            # 'duration': time_duration(video['duration']),
                               'thumbnail': video['snippet']['thumbnails']['default']['url']}
                            #'caption_status': cap['snippet']['caption']}
                       if data['tags'] == []:
                          del data['tags']

                       st.write(data)



                       st.subheader("\nComments:")
                       for comment in comments:
                           data1 = {'comment_id': comment['id'],
                                    'comment_text': comment['snippet']['topLevelComment']['snippet'][
                                    'textDisplay'],
                                    'comment_author': comment['snippet']['topLevelComment']['snippet'][
                                    'authorDisplayName'],
                                    'comment_published_date':comment ['snippet']['topLevelComment']['snippet'][
                                                              'publishedAt'][
                                                          0:10],
                                    'comment_published_time': comment['snippet']['topLevelComment']['snippet'][
                                                              'publishedAt'][
                                                          11:19],
                                    'video_id': video_id }
                           st.write(data1)
          st.write("Data retrieved successfully")
          st.write("Store collected data in MongoDb Atlas")

          data_youtube = {}
          data_extraction = data_extraction_youtube(CHANNEL_ID)
          data_youtube.update(data_extraction)
          st.write(data_youtube)
          #channel_name = data_youtube['channel_name']['channel_name']

          database = "youtube_data_collection"
          mongodb(database,channel_name)
          st.write("Data successfully stored in MongoDb Atlas")

      else:
             st.write(f"Unable to retrieve video details for video ID: {video_id}")
    else:
         st.warning("Please enter a valid Channel ID.")


if __name__ == "__main__":
    main()
