# Top 5 DS/DA vs DE YouTube Channel Analysis


## Step 0 : Data Ingestion/Process/Storage
- Pull channel/playlist/video data using YouTube API
- Process/Clean Data
- Store data into RDS - MySQL

## Step 1 : Visualization using QuickSight
-  3 parameters
      -   users can select switch views by channel category (DE, DS/DA, All)
      -   users can select top youtube videos by commentcount/likecount/viewcount (using calculated field)
      -   users can choose top # of Topics to display on the wordcloud by Data Channel

Example:
<img width="1423" alt="Screen Shot 2022-05-09 at 17 47 33" src="https://user-images.githubusercontent.com/46492171/167520979-f535fb60-0ddd-4c62-8e4f-4edc38d6991c.png">




## Step 2: Set Trgigger to run the script weekly and retrieve new videos (Upsert in the database) With AWS Lambda


![image](https://user-images.githubusercontent.com/46492171/176592318-5666914c-ae5a-4db7-821a-5301a4927139.png)
