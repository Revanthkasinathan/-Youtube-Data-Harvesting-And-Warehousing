# Youtube-Data-Harvesting-And-Warehousing

**Problem Statement:**

Task is to create a Streamlit application that allows users to access and analyze data from multiple YouTube channels and this should be achieved by giving particular Youtube channel ID as an input and in return it should fetch the details of that channel such as Channel details, Video details ,Playlist details ,Comment details.

Extracted data from Youtube should be pushed to Mongodb to store it as a data lake and again retrieve that data as a dataframe using Pandas and create a table in SQL (MYSQL or Postgres ) and push that data to store it in the table in SQL.

With the given queries ,try to fetch the information needed by accessing all the tables in the SQL.

**Technology Stack Used**

* Python

* Mongodb

* Pandas

* SQL(Postgres)


**REQUIRED LIBRARIES:**
1.	googleapiclient.discovery
2.	pymongo
3.	psycopg2
4.	pandas
5.	Streamlit

   
**Approach**


As a first step, establish a connection to YouTube to fetch all the required details for the project and this can be achieved by using YouTube API key which will be available at https://console.cloud.google.com/.


Once the connection is established to the YouTube we can start getting the channel information such as Name, ID, Subscribers Count, View Count, Total videos, Description of the channel and the playlist ID from any particular channel by using the reference https://developers.google.com/youtube/v3/quickstart/python


With repeating the same process, we can fetch the details such as Video Ids, Video Information’s, Comment Info & Playlist Info. Also everything needs to be created as a function, so that we can get the details of multiple channels.


As a next step, establish a connection to mongo dB and create a Database with a collection. Now push all the data (Channel information, Playlist information, Video Information & comment information) as a data lake to store it as the document for the collection created.


Establish a connection to SQL ( MySQL or PostgreSQL) and create a table in SQL for the channel information available in  the mongo dB with the respected  columns.


By using Pandas with Data Frame concept, retrieve all the data of Channel information from Mongo db and push it to sql to store it in the table created for Channel information.


Repeat the same process to store all the data’s like Playlist information, Video information and Comment information from mongodb to sql using pandas.


Create all this as a function, to make sure, we can insert multiple channel data to mongo dB and can fetch that data to store it in sql.


With all the individual table in place, create a common function to call all the individual table function in one place.


As a final step, design the Streamlit application by using the inbuilt functions available for the libraries which we imported as streamlit. Once it is designed, then create a search bar to enter the channel details with a button which will collect and store all the data in Mongodb.


With all the data’s transferred to mongodb, create a button to migrate all the data’s to SQL and to display all the data in form of tables (channels, videos, playlist, comments)


With the given 10 Queries, create an if condition for all the questions to provide us with the expected output  in the streamlit application.
