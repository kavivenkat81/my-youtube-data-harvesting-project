# YouTube Data Harvesting and Warehousing 

**Introduction**

YouTube Data Harvesting and Warehousing is a project aimed at developing a user-friendly Streamlit application that leverages the power of the Google API to extract valuable information from YouTube channels. The extracted data is then stored in a MongoDB database, subsequently migrated to a SQL data warehouse, and made accessible for analysis and exploration within the Streamlit app.

**Table of Contents**

1. Key Technologies and Skills
2. Installation
3. Usage
4. Features
5. Retrieving data from the YouTube API
6. Storing data in MongoDB
7. Migrating data to a SQL data warehouse
8. Data Analysis

**Key Technologies and Skills**
- Python scripting
- Data Collection
- API integration
- Streamlit
- Data Management using MongoDB (Atlas) and SQL

**Installation**

To run this project, you need to install the following packages:

pip install google-api-python-client
pip install pymongo
pip install pandas
pip install psycopg2
pip install streamlit



**Features**

- Retrieve data from the YouTube API, including channel information, playlists, videos, and comments.
- Store the retrieved data in a MongoDB database.
- Migrate the data to a SQL data warehouse.
- Analyze and visualize data using Streamlit and Plotly.
- Perform queries on the SQL data warehouse.


**Retrieving data from the YouTube API**

The project utilizes the Google API to retrieve comprehensive data from YouTube channels. The data includes information on channels, playlists, videos, and comments. By interacting with the Google API, we collect the data and collected data is displayed using streamlit.

**Storing data in MongoDB**

The retrieved data is stored in a MongoDB database based on user authorization. If the data already exists in the database, it identifies . This storage process ensures efficient data management and preservation, allowing for seamless handling of the collected data.

**Migrating data to a SQL data warehouse**

 To ensure compatibility with a structured format, the data is cleansed using the powerful pandas library. Following data cleaning, the information is segregated into separate tables, including channels, playlists, videos, and comments, utilizing SQL queries.

**Data Analysis**

The project provides comprehensive data analysis capabilities using Plotly and Streamlit. With the integrated Plotly library, it create interactive and visually appealing charts and graphs to gain insights from the collected data.

- **Channel Analysis:** Channel analysis includes insights on playlists, videos, subscribers, views, likes, comments, and durations. Gain a deep understanding of the channel's performance and audience engagement through detailed visualizations and summaries.

- **Video Analysis:** Video analysis focuses on views, likes, comments, and durations, enabling both an overall channel and specific channel perspectives. Leverage visual representations and metrics to extract valuable insights from individual videos.


The Streamlit app provides an intuitive interface to interact with the charts and explore the data visually. 

 the Data Analysis section empowers users to uncover valuable insights and make data-driven decisions.
 my-youtube-data-harvesting-project
