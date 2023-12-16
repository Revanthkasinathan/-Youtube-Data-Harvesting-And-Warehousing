from googleapiclient.discovery import build
import pymongo
import pandas as pd
import psycopg2
import streamlit as st
from PIL import Image
from streamlit_marquee import streamlit_marquee
from streamlit_option_menu import option_menu




#Youtube API connection using function 
def Api_connect():
    Api_Id="AIzaSyBB136GbQPMBpCa9wn2LRkNNXVlvKKhrJ4"
    api_service_name="youtube"
    api_version="v3"
    youtube=build(api_service_name,api_version,developerKey=Api_Id)
    return youtube

youtube=Api_connect()


#get channel information using Function
def get_channel_info(channel_id):
    request=youtube.channels().list(
                    part="snippet,contentDetails,statistics",
                    id=channel_id)
    response=request.execute()

    for i in response['items']:
        data=dict(channel_Name=i["snippet"]["title"],
                    channel_ID=i["id"],
                    subscribers=i['statistics']['subscriberCount'],
                    view=i['statistics']['viewCount'],
                    Total_Videos=i['statistics']['videoCount'],
                    Channel_Desc=i["snippet"]['description'],
                    Playlist_ID=i["contentDetails"]["relatedPlaylists"]["uploads"])
        return(data)

#get Video ID's using function 
def get_videos_ids(channel_id):

    #
    video_ids=[]

    #creating a function called Playlist_ID
    response=youtube.channels().list(id=channel_id,
                                    part="contentDetails").execute()              
    Playlist_Id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    #to go to next page
    next_page_token=None

    #to get video ID
    while True:
        response1=youtube.playlistItems().list(
                                            part='snippet',
                                            playlistId=Playlist_Id,
                                            maxResults=50,
                                            pageToken=next_page_token).execute()
        for i in range(len(response1['items'])):                                    
            video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])                                    
        next_page_token=response1.get('nextPageToken')
        
        
        if next_page_token is None:
            break
    return video_ids

#get video information for all the ID using function 
def get_video_info(video_IDS):
    video_data=[]
    for video_id in video_IDS:
        request=youtube.videos().list(
            part="snippet,ContentDetails,statistics",
            id=video_id
        )
        response=request.execute()
        #nested for
        for item in response["items"]:
            data=dict(Channel_name=item['snippet']['channelTitle'],
            Channel_ID=item['snippet']['channelId'],
            video_Id=item['id'],
            title=item['snippet']['title'],
            Tags=item['snippet'].get('tags'),
            Thumbnail=item['snippet']['thumbnails']['default']['url'],
            Descriptions=item['snippet'].get('description'),
            date_of_upload=item['snippet']['publishedAt'],
            Duration=item['contentDetails']['duration'],
            views=item['statistics'].get('viewCount'),
            Likes=item['statistics'].get('likeCount'),
            comments=item['statistics'].get('commentCount'),
            favcount=item['statistics']['favoriteCount'],
            Definition=item['contentDetails']['definition'],
            caption_status=item['contentDetails']['caption']
            )
    #create a List and append all the video details 
        video_data.append(data)
    return video_data


#get comments of many videos using function
def get_comment_info(video_ids):
    Comment_data=[]
    try:
        for video_id in video_ids:
            request=youtube.commentThreads().list(
                part='snippet',
                videoId=video_id,
                maxResults=50
            )
            response=request.execute()

            for item in response['items']:
                data=dict(comment_Id=item['snippet']['topLevelComment']['id'],
                        video_id=item['snippet']['topLevelComment']['snippet']['videoId'],
                        comment_Text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                        comment_author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                        comment_date=item['snippet']['topLevelComment']['snippet']['publishedAt'])
                
                Comment_data.append(data)

    except:
        pass
    return Comment_data


#get playlist details of different channels using Function 
def get_playlist_details(channel_id):

    next_page_token=None
    playlist_data=[]
    while True:
        request=youtube.playlists().list(
            part='snippet,contentDetails',
            channelId=channel_id,
            maxResults=50,
            pageToken=next_page_token
        )
        response=request.execute()

        for item in response['items']:
            data=dict (playlist_id=item['id'],
                    title=item['snippet']['title'],
                    Channel_Id=item['snippet']['channelId'],
                    Channel_name=item['snippet']['channelTitle'],
                    PublishedAt=item['snippet']['publishedAt'],
                    Video_count=item['contentDetails']['itemCount'])
            playlist_data.append(data)

        next_page_token=response.get('nextPageToken')
        if next_page_token is None:
                break
        

    return playlist_data


#Mongodb Upload
client =pymongo.MongoClient("mongodb+srv://Revanth:Revanth123@cluster0.4lszxa0.mongodb.net/?retryWrites=true&w=majority")
#create a database
db=client['youtube']


def channel_details(channel_id):
    ch_details=get_channel_info(channel_id)
    pl_details=get_playlist_details(channel_id)
    vid_ids=get_videos_ids(channel_id)
    vid_details=get_video_info(vid_ids)
    com_details=get_comment_info(vid_ids)

    collection1=db["channel_details"]
    collection1.insert_one({"channel_information":ch_details,
                            "playlist_information":pl_details,
                            "video_information":vid_details,
                            "comment_information":com_details})

    return "upload complete"


#table creation for Channel details using function

def channels_table():
    #SQL Connection
    mydb=psycopg2.connect(
        host = "localhost",
        user = "postgres",
        password = "Qwerty@123",
        database = "youtube_data",
        port="5432"
    )

    cursor=mydb.cursor()

    drop_query='''drop table if exists channels'''
    cursor.execute(drop_query)
    mydb.commit()


    try:

        create_query='''create table if not exists channels(channel_Name varchar(100),
                                                        channel_ID varchar(80) primary key,
                                                            subscribers bigint,
                                                            View bigint,
                                                            Total_Videos int,
                                                            Channel_Desc text,
                                                            Playlist_ID varchar(80))'''
        cursor.execute(create_query)  
        mydb.commit()

    except:

        print("channel table already created") 


    ch_list=[]
    db=client['youtube']
    collection1=db["channel_details"]
    for ch_data in collection1.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])

    df=pd.DataFrame(ch_list)


    for index,row in df.iterrows():
        insert_query='''insert into channels(Channel_Name,
                                    channel_ID ,
                                    subscribers,
                                    view,
                                    Total_Videos,
                                    Channel_Desc,
                                    Playlist_ID)
                                    
                                    values(%s,%s,%s,%s,%s,%s,%s)'''
        values=(row['channel_Name'],
                row['channel_ID'],
                row['subscribers'],
                row['view'],
                row['Total_Videos'],
                row['Channel_Desc'],
                row['Playlist_ID']) 

        try:
            cursor.execute(insert_query,values)  
            mydb.commit()

        except:
            print("values are already inserted") 


#creating playlist table  using  function


def playlist_table():

    mydb=psycopg2.connect(
    host = "localhost",
    user = "postgres",
    password = "Qwerty@123",
    database = "youtube_data",
    port="5432"
        )
    cursor=mydb.cursor()
#create a table playlist in Mysql
    drop_query='''drop table if exists playlist'''
    cursor.execute(drop_query)
    mydb.commit()    

    create_query='''create table if not exists playlist(playlist_id varchar(100) primary key,
                                                            title varchar(100) ,
                                                                Channel_Id varchar(100),
                                                                Channel_name varchar(100),
                                                                PublishedAt timestamp,
                                                                Video_count int )'''
    cursor.execute(create_query)  
    mydb.commit()

#convert the data in mongodb to DataFrame
    pl_list=[]
    db=client['youtube']
    collection1=db["channel_details"]
    for pl_data in collection1.find({},{"_id":0,"playlist_information":1}):
        for i in range(len(pl_data["playlist_information"])):
            pl_list.append(pl_data["playlist_information"][i])

        df1=pd.DataFrame(pl_list)

#push the dataframe values to sql

    for index,row in df1.iterrows():
        insert_query='''insert into playlist(playlist_id,
                                    title ,
                                    Channel_Id,
                                    Channel_name,
                                    PublishedAt,
                                    Video_count
                                    )
                                    
                                    values(%s,%s,%s,%s,%s,%s)'''
        values=(row['playlist_id'],
                row['title'],
                row['Channel_Id'],
                row['Channel_name'],
                row['PublishedAt'],
                row['Video_count']) 

     
        cursor.execute(insert_query,values)  
        mydb.commit()       



# table creation for Video information using function

def videos_table():

    mydb=psycopg2.connect(
        host = "localhost",
        user = "postgres",
        password = "Qwerty@123",
        database = "youtube_data",
        port="5432"
    )

    cursor=mydb.cursor()
    #create a table playlist in Mysql
    drop_query='''drop table if exists videos'''
    cursor.execute(drop_query)
    mydb.commit()    

    create_query='''create table if not exists videos(Channel_name varchar(100),
                                                    Channel_ID varchar(100),
                                                    video_Id varchar(100) primary key,
                                                    title varchar(150),
                                                    Tags text,
                                                    Thumbnail varchar(150),
                                                    Descriptions text,
                                                    date_of_upload timestamp,
                                                    Duration interval,
                                                    views bigint,
                                                    Likes bigint,
                                                    comments int,
                                                    favcount int,
                                                    Definition  varchar(100),
                                                    caption_status varchar(100)
                                                    )'''
    cursor.execute(create_query)  
    mydb.commit()

    vi_list=[]
    db=client['youtube']
    collection1=db["channel_details"]
    for vi_data in collection1.find({},{"_id":0,"video_information":1}):
        for i in range(len(vi_data["video_information"])):
            vi_list.append(vi_data["video_information"][i])

    df2=pd.DataFrame(vi_list)


    for index,row in df2.iterrows():
            insert_query='''insert into videos(Channel_name,
                                                    Channel_ID,
                                                    video_Id,
                                                    title,
                                                    Tags,
                                                    Thumbnail,
                                                    Descriptions,
                                                    date_of_upload,
                                                    Duration,
                                                    views,
                                                    Likes,
                                                    comments,
                                                    favcount,
                                                    Definition,
                                                    caption_status
                                            )
                                        
                                        values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
            values=(row['Channel_name'],
                    row['Channel_ID'],
                    row['video_Id'],
                    row['title'],
                    row['Tags'],
                    row['Thumbnail'],
                    row['Descriptions'],
                    row['date_of_upload'],
                    row['Duration'],
                    row['views'],
                    row['Likes'],
                    row['comments'],
                    row['favcount'],
                    row['Definition'],
                    row['caption_status']) 

            cursor.execute(insert_query, values)  
            mydb.commit()



#table creation for comment details using function

def comments_table():

    mydb=psycopg2.connect(
        host = "localhost",
        user = "postgres",
        password = "Qwerty@123",
        database = "youtube_data",
        port="5432"
            )

    cursor=mydb.cursor()
    #create a table playlist in Mysql
    drop_query='''drop table if exists comments'''
    cursor.execute(drop_query)
    mydb.commit()    

    create_query='''create table if not exists comments(comment_Id varchar(100) primary key,
                            video_id varchar(100),
                            comment_Text text,
                            comment_author varchar(100) ,
                            comment_date timestamp
                            )'''


    cursor.execute(create_query)  
    mydb.commit()

    com_list=[]
    db=client['youtube']
    collection1=db["channel_details"]
    for com_data in collection1.find({},{"_id":0,"comment_information":1}):
        for i in range(len(com_data['comment_information'])):
            com_list.append(com_data["comment_information"][i])

    df3=pd.DataFrame(com_list)


    for index,row in df3.iterrows():
            insert_query='''insert into comments(comment_Id,
                                                    video_id,
                                                    comment_Text,
                                                    comment_author,
                                                    comment_date
                                                            )
                                        values(%s,%s,%s,%s,%s)'''
            values=(row['comment_Id'],
                    row['video_id'],
                    row['comment_Text'],
                    row['comment_author'],
                    row['comment_date'])

        
            cursor.execute(insert_query,values)  
            mydb.commit()

def tables():
    channels_table()
    playlist_table()
    videos_table()
    comments_table()

    return "Tables created Successfully"


#display output in Table format in streamlit

def show_channels_table():

    ch_list=[]
    db=client['youtube']
    collection1=db["channel_details"]
    for ch_data in collection1.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])

    df=st.dataframe(ch_list)

    return df

#playlist table

def show_playlist_table():

    pl_list=[]
    db=client['youtube']
    collection1=db["channel_details"]
    for pl_data in collection1.find({},{"_id":0,"playlist_information":1}):
        for i in range(len(pl_data["playlist_information"])):
            pl_list.append(pl_data["playlist_information"][i])


        df1=st.dataframe(pl_list)

        return df1
    
#video table 

def show_videos_table():

    vi_list=[]
    db=client['youtube']
    collection1=db["channel_details"]
    for vi_data in collection1.find({},{"_id":0,"video_information":1}):
        for i in range(len(vi_data["video_information"])):
            vi_list.append(vi_data["video_information"][i])

    df2=st.dataframe(vi_list)
    
    return df2

# comments table

def show_comments_table():

    com_list=[]
    db=client['youtube']
    collection1=db["channel_details"]
    for com_data in collection1.find({},{"_id":0,"comment_information":1}):
        for i in range(len(com_data['comment_information'])):
            com_list.append(com_data["comment_information"][i])

    df3=st.dataframe(com_list)


    return df3


#streamlit 

st.set_page_config(page_title= "Youtube Data Harvesting",
                   page_icon= 'im',
                   layout= "wide",)

def app_bg():
    st.markdown(f""" <style>.stApp {{
                        background: url("https://cutewallpaper.org/21/white-and-red-backgrounds/73+-Red-And-White-Backgrounds-on-WallpaperSafari.jpg");   
                        background-size: cover}}
                     </style>""",unsafe_allow_html=True)
app_bg()

with st.sidebar:
    st.title(":orange[YOUTUBE DATA HARVESTING AND WAREHOUSING]")
    st.header("Technologies Used")
    #img1 = Image.open("python.png")
    #new_image1 = img1.resize((300, 150))
    st.image("https://cdn.analyticsvidhya.com/wp-content/uploads/2023/07/1_m0H6-tUbW6grMlezlb52yw.png",width=250)
    #img2 = Image.open("mongo.png")
    #new_image2 = img2.resize((300, 150))
    st.image("https://wallpapercave.com/wp/wp8725088.jpg",width=250)
    #img3 = Image.open("pandas1.png")
    #new_image3 = img3.resize((300, 150))
    st.image("https://miro.medium.com/v2/resize:fit:798/1*93CVLqnQESmvfOhzvYUgQw.png",width=250)
    #img4 = Image.open("sql.png")
    #new_image4 = img4.resize((300, 150))
    st.image("https://thumb.tildacdn.com/tild6238-3035-4335-a333-306335373139/-/resize/824x/-/format/webp/IMG_3349.jpg",width=250)


st.title(":red[Welcome to my Youtube Project :)]")

Channel_id = st.text_input("Enter the channel ID")

if st.button("Collect & Store(Mongodb) "):
    ch_ids=[]
    db=client['youtube']
    collection1=db["channel_details"]

    for ch_data in collection1.find({},{"_id":0,"channel_information":1}):
        ch_ids.append(ch_data["channel_information"]['channel_ID'])


    if Channel_id in ch_ids:
        st.success("Channel details of given id already exists")
    else:
        insert=channel_details(Channel_id)
        st.success(insert)

if st.button("Data Migration to sql"):
    Table=tables()
    st.success(Table)


show_table=st.radio("SELECT the TABLE",("CHANNELS","PLAYLISTS", "VIDEOS","COMMENTS"))

if show_table=="CHANNELS":
    show_channels_table()

elif show_table=="PLAYLISTS":
    show_playlist_table()

elif show_table=="VIDEOS":
    show_videos_table()

elif show_table=="COMMENTS":
    show_comments_table()


#SQL connection

mydb=psycopg2.connect(
    host = "localhost",
    user = "postgres",
    password = "Qwerty@123",
    database = "youtube_data",
    port="5432"
        )

cursor=mydb.cursor()

Question=st.selectbox("Select your question",("1. What are the names of all the videos and their corresponding channels?",
                                              "2.Which channels have the most number of videos, and how many videos do they have?",
                                                "3.What are the top 10 most viewed videos and their respective channels?",
                                                "4.How many comments were made on each video, and what are their corresponding video names?",
                                                "5.Which videos have the highest number of likes, and what are their corresponding channel names?",
                                                "6.What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
                                                "7.What is the total number of views for each channel, and what are their corresponding channel names?",
                                                "8.What are the names of all the channels that have published videos in the year 2022?",
                                                "9.What is the average duration of all videos in each channel, and what are their corresponding channel names?",
                                                "10.Which videos have the highest number of comments, and what are their corresponding channel names?"))

if Question=="1. What are the names of all the videos and their corresponding channels?":
    que1='''select title as videos,channel_name as channelname from videos'''
    cursor.execute(que1)
    mydb.commit()

    tab1=cursor.fetchall()

    df=pd.DataFrame(tab1,columns=["Video title","Channel Name"])
    st.write(df)

elif Question=="2.Which channels have the most number of videos, and how many videos do they have?":

    que2='''select channel_name as channelname,total_videos as no_of_videos from channels 
              order by total_videos desc'''
    cursor.execute(que2)
    mydb.commit()

    tab2=cursor.fetchall()

    df1=pd.DataFrame(tab2,columns=["Channel Name"," No of Videos"])
    st.write(df1)

elif Question=="3.What are the top 10 most viewed videos and their respective channels?":

    que3='''select views as views,channel_name as channelname,title as videotitle from videos
                where views is not null order by views desc limit 10'''
    cursor.execute(que3)
    mydb.commit()

    tab3=cursor.fetchall()

    df2=pd.DataFrame(tab3,columns=["Views","Channel Name", "Video Title"])
    st.write(df2)

elif Question=="4.How many comments were made on each video, and what are their corresponding video names?":

    que4='''select comments as no_comments,title as videotitle from Videos where comments is not null'''
    cursor.execute(que4)
    mydb.commit()

    tab4=cursor.fetchall()

    df3=pd.DataFrame(tab4,columns=["Comments", "Video Title"])
    st.write(df3)

elif Question=="5.Which videos have the highest number of likes, and what are their corresponding channel names?":

    que5='''select title as videotitle,channel_name as channelname,likes as Likecount
            from videos where likes is not null order by likes desc'''
    cursor.execute(que5)
    mydb.commit()

    tab5=cursor.fetchall()

    df4=pd.DataFrame(tab5,columns=["Video Title", "Channel name","Like Count "])
    st.write(df4)


elif Question=="6.What is the total number of likes and dislikes for each video, and what are their corresponding video names?":

    que6='''select Likes as likecount,title as videotitle from videos'''
    cursor.execute(que6)
    mydb.commit()

    tab6=cursor.fetchall()

    df5=pd.DataFrame(tab6,columns=["Like Count ", "Video Title"])
    st.write(df5)

elif Question=="7.What is the total number of views for each channel, and what are their corresponding channel names?":

    que7='''select channel_name as channelname, view as totalviews from channels'''
    cursor.execute(que7)
    mydb.commit()

    tab7=cursor.fetchall()

    df6=pd.DataFrame(tab7,columns=["Channel Name", "Total Views"])
    st.write(df6)

elif Question=="8.What are the names of all the channels that have published videos in the year 2022?":

    que8='''select title as video_title,date_of_upload as Publisheddate, channel_name as channelname from videos
            where extract(year from date_of_upload)=2022'''
    cursor.execute(que8)
    mydb.commit()

    tab8=cursor.fetchall()

    df7=pd.DataFrame(tab8,columns=["Video Title","Published date"," Channel Name"])
    st.write(df7)


elif Question=="9.What is the average duration of all videos in each channel, and what are their corresponding channel names?":

    que9='''select channel_name as channelname, AVG(duration) as averageduration from videos group by channel_name'''
    cursor.execute(que9)
    mydb.commit()
    tab9=cursor.fetchall()
    df8=pd.DataFrame(tab9,columns=["channelname", "averageduration"])
    

    Tab9=[]
    for index,row in df8.iterrows():
        channel_title=row["channelname"]
        Average_duration=row["averageduration"]
        average_duration_str=str(Average_duration)
        Tab9.append(dict (ChannelTitle = channel_title,AvgDuration = average_duration_str))

    df9=pd.DataFrame(Tab9)
    st.write(df9)

elif Question=="10.Which videos have the highest number of comments, and what are their corresponding channel names?":

    que10='''select title as videotitle, channel_name as channelname, comments as comments from videos 
    where comments is not null order by comments desc '''
    cursor.execute(que10)
    mydb.commit()

    tab10=cursor.fetchall()

    df10=pd.DataFrame(tab10,columns=["Video Title","Channel Name ", "comments"])
    st.write(df10)
