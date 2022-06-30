# Top 5 DS/DA vs DE YouTube Channel Analysis


## Step 0 : Data Ingestion/Process/Storage

- Tools
      - Databricks notebook (download relevant libraries onto the cluster)

- Details
      - Pull channel/playlist/video data using YouTube API
      - Process/Clean Data
      - Store data into RDS - MySQL



## Step 1 : Visualization using QuickSight (scheduled to refresh weekly)
-  3 parameters
      -   users can switch views by channel category (DE, DS/DA, All)
      -   users can change top youtube videos by commentcount/likecount/viewcount (using calculated field)
      -   users can choose top # of Topics to display on the wordcloud by Data Channel

Example:
<img width="1423" alt="Screen Shot 2022-05-09 at 17 47 33" src="https://user-images.githubusercontent.com/46492171/167520979-f535fb60-0ddd-4c62-8e4f-4edc38d6991c.png">




## Step 2: Schedule running the script weekly

retrieve new videos (Upsert in the database) With Databricks Job (cron schedule) 

<img width="767" alt="Screen Shot 2022-06-29 at 21 30 28" src="https://user-images.githubusercontent.com/46492171/176593103-4a3687cf-eb13-48dc-afd7-e11662b68159.png">
