import os
import pymongo
from googleapiclient.discovery import build

# Set up your YouTube Data API key
youtube_api_key = 'AIzaSyBN2RrZCqYkCWjnL1corhBSGf8DkrNsP48'

# Set up the MongoDB Atlas connection
mongo_client = pymongo.MongoClient("mongodb+srv://venkatkavi71:kavitha@kavi.zvzqs4r.mongodb.net/test?retryWrites=true&w=majority")
db = mongo_client["youtube_channels1"]
collection = db["channel_details"]

# Function to retrieve YouTube channel details
def get_channel_details(channel_id):
    youtube = build("youtube", "v3", developerKey=youtube_api_key)
    request = youtube.channels().list(
        part="snippet,statistics",
        id=channel_id
    )
    response = request.execute()
    return response.get("items", [])[0]

# Example channel ID (replace with the one you want)
channel_id = "UC_x5XG1OV2P6uZZ5FSM9Ttw"

# Retrieve channel details
channel_data = get_channel_details(channel_id)

try:
    mongo_client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")

except Exception as e:
    print(e)

# Insert or update the channel details in MongoDB
collection.insert_one(channel_data)



print("Channel details stored in MongoDB Atlas.")






