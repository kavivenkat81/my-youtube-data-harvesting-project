# Import necessary libraries
from googleapiclient.discovery import build
import pymongo
import mysql.connector


# Set up MongoDB Atlas connection
client = pymongo.MongoClient("mongodb+srv://<username>:<password>@<cluster-url>/test?retryWrites=true&w=majority")
db = client.youtube_channels

# Set up YouTube Data API
api_key = "YOUR_YOUTUBE_API_KEY"
youtube = build("youtube", "v3", developerKey=api_key)

# Function to collect channel details and store them in MongoDB
def collect_and_store_channel_details(channel_id):
    request = youtube.channels().list(part="snippet,statistics", id=channel_id)
    response = request.execute()
    if response.get("items"):
        channel_data = response["items"][0]
        db.channels.insert_one(channel_data)

# Example usage
channel_id = "YOUR_CHANNEL_ID"
collect_and_store_channel_details(channel_id)



import mysql.connector

# Set up MySQL connection
mysql_conn = mysql.connector.connect(
    host="localhost",
    user="username",
    password="password",
    database="youtube_channels"
)

# Function to migrate data from MongoDB to MySQL
def migrate_data_to_mysql():
    cursor = mysql_conn.cursor()
    cursor.execute("CREATE TABLE IF NOT EXISTS channels (id VARCHAR(255), title VARCHAR(255), description TEXT, "
                   "viewCount INT, subscriberCount INT, videoCount INT)")

    # Fetch data from MongoDB and insert into MySQL
    for doc in db.channels.find():
        cursor.execute("INSERT INTO channels (id, title, description, viewCount, subscriberCount, videoCount) "
                       "VALUES (%s, %s, %s, %s, %s, %s)",
                       (doc["id"], doc["snippet"]["title"], doc["snippet"]["description"],
                        doc["statistics"]["viewCount"], doc["statistics"]["subscriberCount"],
                        doc["statistics"]["videoCount"]))

    mysql_conn.commit()
    cursor.close()

# Example usage
migrate_data_to_mysql()



import streamlit as st
import mysql.connector

# Set up MySQL connection
mysql_conn = mysql.connector.connect(
    host="localhost",
    user="username",
    password="password",
    database="youtube_channels"
)

# Function to fetch and display channel details
def display_channel_details():
    cursor = mysql_conn.cursor()
    cursor.execute("SELECT * FROM channels")
    data = cursor.fetchall()
    cursor.close()

    st.title("YouTube Channel Details")
    for row in data:
        st.write(f"Channel ID: {row[0]}")
        st.write(f"Title: {row[1]}")
        st.write(f"Description: {row[2]}")
        st.write(f"View Count: {row[3]}")
        st.write(f"Subscriber Count: {row[4]}")
        st.write(f"Video Count: {row[5]}")
        st.write("-" * 50)

# Streamlit app
if __name__ == "__main__":
    display_channel_details()
S