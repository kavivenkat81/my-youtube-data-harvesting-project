import mysql.connector
import streamlit as st

def sql_create_tables():

        host = "localhost"
        user = "root"
        password = "kavitha"
        database = "m"

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

        # Define SQL statements for table creation
        create_channel_table = """
            CREATE TABLE IF NOT EXISTS channel(
                channel_id          VARCHAR(255) PRIMARY KEY,
                channel_name        VARCHAR(255),
                subscription_count INT,
                channel_views       INT,
                channel_description TEXT,
                upload_id           VARCHAR(255),
                country             VARCHAR(255)
            )
        """

        create_playlist_table = """
            CREATE TABLE IF NOT EXISTS playlist(
                playlist_id     VARCHAR(255) PRIMARY KEY,
                playlist_name   VARCHAR(255),
                channel_id      VARCHAR(255),
                upload_id       VARCHAR(255)
            )
        """

        # Define other table creation statements in a similar manner

        # Execute table creation queries
        cursor.execute(create_channel_table)
        cursor.execute(create_playlist_table)

        # Commit changes to the database
        connection.commit()

    except mysql.connector.Error as err:
        st.error(f"Error: {err}")
    finally:
        # Close the cursor and database connection
        cursor.close()
        connection.close()








