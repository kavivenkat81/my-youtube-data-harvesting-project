import os
import google_auth_oauthlib.flow
import googleapiclient.discovery
from googleapiclient.discovery import build
import json
import googleapiclient.discovery
from googleapiclient.errors import HttpError
import streamlit as st


# Set your API key here. You can also set it as an environment variable.
API_KEY = 'AIzaSyBN2RrZCqYkCWjnL1corhBSGf8DkrNsP48'

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




def get_channel_details(youtube,channel_id):
    # Create a YouTube Data API client
    #youtube = googleapiclient.discovery.build('youtube', 'v3', developerKey=API_KEY)

    try:
        # Call the channels().list() method to retrieve channel details
        response = youtube.channels().list(
            part='contentDetails,snippet,statistics',
            id=channel_id
        ).execute()

        # Extract the channel details
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

        else:
            st.write("Channel not found.")


    except HttpError as e:
        print(f"An error occurred: {e}")
        return None


# Streamlit web app
st.title("YouTube Channel Details")

# Input field for the channel ID
channel_id = st.text_input("Enter the YouTube Channel ID:")

# Display channel details when the user clicks the "Get Details" button
if st.button("Get Details"):
    if channel_id:
        get_channel_details(youtube,channel_id)
    else:
        st.warning("Please enter a valid Channel ID.")

#from googleapiclient.discovery import build

# Replace with your own credentials JSON file path
#CLIENT_SECRETS_FILE = 'C:\Users\venka\PycharmProjects\pythonProject6\temp.json'
#API_NAME = 'youtube'
#API_VERSION = 'v3'
#SCOPES = ['https://www.googleapis.com/auth/youtube.readonly']

def get_channel_playlists(youtube,channel_id):
    # Initialize the YouTube Data API client
    #youtube = build(API_NAME, API_VERSION, developerKey=API_KEY)

    # Get the list of playlists for the specified channel
    playlists = youtube.playlists().list(
        part='snippet',
        channelId=channel_id,
        maxResults=5 # Adjust as needed
    ).execute()

    playlist_ids = []

    # Iterate through the playlists and store their IDs
    for playlist in playlists['items']:
        playlist_id = playlist['id']
        playlist_ids.append(playlist_id)

    return playlist_ids

def main():
    channel_id = input("Enter the channel ID: ")

    playlist_ids = get_channel_playlists(youtube,channel_id)

    if playlist_ids:
        print("Playlist IDs for the channel:")
        for playlist_id in playlist_ids:
            print(playlist_id)
    else:
        print("No playlists found for the channel.")

if __name__ == '__main__':
    main()
