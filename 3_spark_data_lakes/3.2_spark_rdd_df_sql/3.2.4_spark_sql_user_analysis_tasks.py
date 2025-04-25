# This and the script 3.3. use the same dataset and most questions. This script uses Spark SQL instead of Spark Data Frames.

# Import library.
from pyspark.sql import SparkSession

# Create Spark session.
spark = SparkSession.builder.appName("Data wrangling with Spark SQL").getOrCreate()

# Read JSON into DataFrame.
user_log = spark.read.json("data/sparkify_log_small.json")

# Create a view to use with SQL queries.
user_log.createOrReplaceTempView("user_log_table")


# Task 1: Which page(s) did userId = "" (empty string) NOT visit?
task_1_query = """
    SELECT DISTINCT page
    FROM user_log_table
    WHERE userId != ''

    EXCEPT

    SELECT DISTINCT page
    FROM user_log_table
    WHERE userId = ''
"""
task_1_result = spark.sql(task_1_query)
task_1_result.show()


# Task 2: Number of female users in the data set
task_2_query = """
    SELECT COUNT(DISTINCT userId) AS female_users
    FROM user_log_table
    WHERE gender = 'F'
"""
task_2_result = spark.sql(task_2_query)
task_2_result.show()


# Task 3: Total play count for the most frequently played artist
task_3_query = """
    SELECT artist, COUNT(*) AS play_count
    FROM user_log_table
    WHERE page = 'NextSong'
    GROUP BY artist
    ORDER BY play_count DESC
    LIMIT 1
"""
task_3_result = spark.sql(task_3_query)
task_3_result.show()


# Task 4: Average number (rounded) of songs users listen to between visiting our home page:

task_4_query = """
    WITH cusum AS (
        SELECT userID, page, ts,
            CASE WHEN page = 'Home' THEN 1 ELSE 0 END AS homevisit,
            SUM(CASE WHEN page = 'Home' THEN 1 ELSE 0 END) OVER (
                PARTITION BY userID
                ORDER BY ts DESC
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS period
        FROM user_log_table
        WHERE page IN ('NextSong', 'Home')
    )
    SELECT ROUND(AVG(song_count)) AS average_songs
    FROM (
        SELECT userID, period, COUNT(period) AS song_count
        FROM cusum
        WHERE page = 'NextSong'
        GROUP BY userID, period
    )
"""
task_4_result = spark.sql(task_4_query)
task_4_result.show()
