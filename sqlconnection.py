import mysql.connector
import streamlit as st

host = "localhost"
user = "root"
password = "kavitha"
database = "myd"


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
cursor.execute("CREATE TABLE customers2 (name VARCHAR(255), address VARCHAR(255))")

sql = "INSERT INTO customers2 (name, address) VALUES (%s, %s)"
val = [
  ('Peter', 'Lowstreet 4'),
  ('Amy', 'Apple st 652'),
  ('Hannah', 'Mountain 21'),
  ('Michael', 'Valley 345'),
  ('Sandy', 'Ocean blvd 2'),
  ('Betty', 'Green Grass 1'),
  ('Richard', 'Sky st 331'),
  ('Susan', 'One way 98'),
  ('Vicky', 'Yellow Garden 2'),
  ('Ben', 'Park Lane 38'),
  ('William', 'Central st 954'),
  ('Chuck', 'Main Road 989'),
  ('Viola', 'Sideway 1633')
]

try:
    # Execute the SQL query
    cursor.executemany(sql, val)

    # Commit the transaction
    connection.commit()
    print(cursor.rowcount, "records inserted successfully")

except Exception as e:
    # Handle any exceptions
    print("Error:", e)
    connection.rollback()

finally:
    # Close the database connection
    connection.close()
"""
cursor.executemany(sql, val)

print(cursor.rowcount, "was inserted.")

cursor.execute("create table if not exists channel(\
                                       channel_id 			varchar(255) primary key,\
                                       channel_name		varchar(255),\
                                       subscription_count	int,\
                                       channel_views		int,\
                                       channel_description	text,\
                                       upload_id			varchar(255),\
                                       country				varchar(255))")

cursor.execute("create table if not exists playlist(\
                                       playlist_id		varchar(255) primary key,\
                                       playlist_name	varchar(255),\
                                       channel_id		varchar(255),\
                                       upload_id		varchar(255))")

cursor.execute("create table if not exists video(\
                                       video_id			varchar(255) primary key,\
                                       video_name			varchar(255),\
                                       video_description	text,\
                                       upload_id			varchar(255),\
                                       tags				text,\
                                       published_date		date,\
                                       published_time		time,\
                                       view_count			int,\
                                       like_count			int,\
                                       favourite_count		int,\
                                       comment_count		int,\
                                       duration			time,\
                                       thumbnail			varchar(255),\
                                       caption_status		varchar(255))")

cursor.execute("create table if not exists comment(\
                                       comment_id				varchar(255) primary key,\
                                       comment_text			text,\
                                       comment_author			varchar(255),\
                                       comment_published_date	date,\
                                       comment_published_time	time,\
                                       video_id				varchar(255))")
"""


