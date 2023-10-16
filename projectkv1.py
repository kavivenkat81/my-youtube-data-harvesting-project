import os
import json
import googleapiclient.discovery
from google.oauth2 import service_account

# Define your API credentials file path
credentials_file = 'your_credentials.json'


# Initialize the YouTube Data API client
def initialize_youtube_api():
    credentials = service_account.Credentials.from_service_account_file(credentials_file, scopes=[
        'https://www.googleapis.com/auth/youtube.force-ssl'])
    youtube_api = googleapiclient.discovery.build('youtube', 'v3', credentials=credentials)
    return youtube_api


# Get channel details by channel ID
def get_channel_details(youtube_api, channel_id):
    request = youtube_api.channels().list(
        part='snippet,statistics',
        id=channel_id
    )
    response = request.execute()
    return response['items'][0]


# Get playlist IDs for a channel
def get_playlist_ids(youtube_api, channel_id):
    playlist_ids = []
    request = youtube_api.playlists().list(
        part='snippet',
        channelId=channel_id,
        maxResults=50
    )
    while request:
        response = request.execute()
        for item in response['items']:
            playlist_ids.append(item['id'])
        request = youtube_api.playlists().list_next(request, response)
    return playlist_ids


# Get all videos in a playlist
def get_videos_in_playlist(youtube_api, playlist_id):
    videos = []
    request = youtube_api.playlistItems().list(
        part='contentDetails',
        maxResults=50,
        playlistId=playlist_id
    )
    while request:
        response = request.execute()
        for item in response['items']:
            videos.append(item['contentDetails']['videoId'])
        request = youtube_api.playlistItems().list_next(request, response)
    return videos


# Get video details and comments for a video
def get_video_details_and_comments(youtube_api, video_id):
    video_details = youtube_api.videos().list(
        part='snippet,statistics',
        id=video_id
    ).execute()

    comment_threads = []
    request = youtube_api.commentThreads().list(
        part='snippet',
        videoId=video_id,
        maxResults=100
    )

    while request:
        response = request.execute()
        for item in response['items']:
            comment_threads.append(item['snippet']['topLevelComment']['snippet']['textOriginal'])
        request = youtube_api.commentThreads().list_next(request, response)

    return video_details['items'][0], comment_threads


if __name__ == "__main__":
    # Replace 'YOUR_CHANNEL_ID' with the channel ID you want to retrieve information for
    channel_id = 'YOUR_CHANNEL_ID'

    youtube_api = initialize_youtube_api()

    channel_details = get_channel_details(youtube_api, channel_id)
    print("Channel Details:")
    print(json.dumps(channel_details, indent=4))

    playlist_ids = get_playlist_ids(youtube_api, channel_id)
    print("\nPlaylist IDs:")
    print(json.dumps(playlist_ids, indent=4))

    if playlist_ids:
        # Fetch the first playlist's videos and comments as an example
        playlist_id = playlist_ids[0]
        videos_in_playlist = get_videos_in_playlist(youtube_api, playlist_id)

        if videos_in_playlist:
            # Fetch details and comments for the first video in the playlist as an example
            video_id = videos_in_playlist[0]
            video_details, comments = get_video_details_and_comments(youtube_api, video_id)

            print("\nVideo Details:")
            print(json.dumps(video_details, indent=4))

            print("\nComments for Video ID", video_id, ":")
            for idx, comment in enumerate(comments):
                print(f"Comment {idx + 1}: {comment}")
