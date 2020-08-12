# Introduction
A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.
Our task it to build an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to.

# design
1. Create staging_events table and staging_songs table to store events data and song data which in event.json and song.json.
2. copy data from json file to staging_events table and staging_songs table.
3. Create fact table songplays and dimension tables users,songs,artists and time.
4. Extract data from staging_events table and staging_songs table to songplays,users,songs,artists and time table.