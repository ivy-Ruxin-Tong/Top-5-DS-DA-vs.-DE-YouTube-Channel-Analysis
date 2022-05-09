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

#<img width="889" alt="Screen Shot 2022-05-03 at 18 31 52" src="https://user-images.githubusercontent.com/46492171/166612178-4e46690a-e91a-4f39-9b13-324c712aa063.png">

## Step 2: Set Trgigger to run the script weekly and retrieve new videos (Upsert in the database) With AWS Lambda


