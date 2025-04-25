# Import libraries.
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import udf, desc, sum as Fsum
from pyspark.sql.types import IntegerType

import datetime

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

# Create Spark session.
spark = SparkSession.builder.appName("Data wrangling").getOrCreate()

# Read JSON into DataFrame.
path = "data/sparkify_log_small.json"
user_log_df = spark.read.json(path)


# Data Exploration

print("\nPrint Schema of the original DataFrame:")
user_log_df.printSchema()

print("\nDescribe the DataFrame:")
user_log_df.describe().show()

print("\nShow first row:")
user_log_df.show(n=1)

print("\nTake one row:")
print(user_log_df.take(1))

# Use .show() for visual inspection, .take() for programmatic access (e.g., extracting values).

print("\nShow summary statistics for the song length column:")
user_log_df.describe("length").show()

print("\nCount of rows in the DataFrame:")
print(user_log_df.count())

print("\nSelect the 'page' column, drop the duplicates from the DataFrame, then sort by 'page' column:")
user_log_df.select("page").dropDuplicates().sort("page").show()

print("\nSelect 'userId, firstname, page, song' where 'userId' == 1046:")
user_log_df.select(["userId", "firstname", "page", "song"]).where(user_log_df.userId == "1046").show()


# Calculate statistics by hour

get_hour = udf(lambda x: datetime.datetime.fromtimestamp(x / 1000.0). hour)

user_log_df = user_log_df.withColumn("hour", get_hour(user_log_df.ts))

print("\nUse .head() to access the first row:")
print(user_log_df.head(1))

# Filter rows where 'page' == 'NextSong'.
songs_in_hour_df = user_log_df.filter(user_log_df.page == "NextSong") \
    .groupby(user_log_df.hour) \
    .count() \
    .orderBy(user_log_df.hour.cast("float"))

print("\nShow the new DataFrame that includes 'hour' column, and filtered for rows where 'page' =='NextSong':")
songs_in_hour_df.show()

songs_in_hour_pd = songs_in_hour_df.toPandas()
songs_in_hour_pd.hour = pd.to_numeric(songs_in_hour_pd.hour)

# Display the results in a scatter plot.
plt.scatter(songs_in_hour_pd["hour"], songs_in_hour_pd["count"])
plt.xlim(-1, 24)
plt.ylim(0, 1.2 * max(songs_in_hour_pd["count"]))
plt.xlabel("Hour")
plt.ylabel("Songs played")
plt.show()


# Drop rows with missing values or empty strings

# Refresher: how='any' drops a row if it contains any nulls. how='all' drops a row only if all values are null.
user_log_valid_df = user_log_df.dropna(how="any", subset=["userId", "sessionId"])

print("\nCount of rows before dropping null values:")
print(user_log_df.count())

print("\nCount of rows after dropping null values:")
print(user_log_valid_df.count())

# This dataset has no nulls in userId or sessionId, but some userIds may be empty strings ("").
print("\nSelect all unique userIds and sort to check for empty strings:")
user_log_df.select("userId").dropDuplicates().sort("userId").show()

print("\nFilter out rows where 'userId' is an empty string. Note the drop in row count:")
user_log_valid_df = user_log_valid_df.filter(user_log_valid_df["userId"] != "")
print(user_log_valid_df.count())


# Find when users downgrade their accounts, then flag those log entries.

print("\nCount of rows where 'page' = 'Submit Downgrade':")
print(user_log_valid_df.filter("page = 'Submit Downgrade'").count())

print("\nFilter for rows where 'page' = 'Submit Downgrade':")
user_log_valid_df.filter("page = 'Submit Downgrade'").show()

print("\nSelect 'userId, firstname, page, level, song' where 'userId' == 1138:")
user_log_valid_df.select(["userId", "firstname", "page", "level", "song"]).where(user_log_df.userId == "1138").show()

# Create a udf() to return 1 if the record contains a downgrade.
flag_downgrade_event = udf(lambda x: 1 if x == "Submit Downgrade" else 0, IntegerType())

# Add a column called downgraded; apply the flag_downgrade_event UDF to it.
user_log_valid_df = user_log_valid_df.withColumn("downgraded", flag_downgrade_event("page"))

# Check if the downgraded column's added to the DataFrame and returning expected values.
# Expected values: if page column shows value Submit Downgrade, then downgraded column shows value 1, else 0.
print("\nCheck if the downgraded column's added to the DataFrame and returning expected values:")
print(user_log_valid_df.show(1))

# Partition by user id, then use a window function and cumulative sum to distinguish each user's data as pre or post downgrade events.
windowval = Window.partitionBy("userId").orderBy(desc("ts")).rangeBetween(Window.unboundedPreceding, 0)

# Fsum is a cumulative sum over a window - in this case a window showing all events for a user.
# Add a column called 'phase' with boolean values: 1 if the user has downgroaded, else 0.
user_log_valid_df = user_log_valid_df.withColumn("phase", Fsum("downgraded").over(windowval))

# Check if the phase column's added to the DataFrame and returning expected values.
print("\nCheck if the home column's added to the DataFrame and returning expected values:")
user_log_valid_df.show()

print("\nShow the 'phase' for user 1138:")
user_log_valid_df \
    .select(["userId", "firstname", "ts", "page", "downgraded", "phase"]) \
    .where(user_log_df.userId == "1138") \
    .sort("ts") \
    .show()


# Save DataFrame as CSV.

out_path = "data/sparkify_log_small_wrangled.csv"
user_log_valid_df.write.mode("overwrite").save(out_path, format="csv", header=True)
# Alternative: save without overwriting.
# user_log_valid_df.write.save(out_path, format="csv", header=True)

# Read the saved CSV back into a DataFrame.
user_log_df_2 = spark.read.csv(out_path, header=True)

# Show schema of reloaded DataFrame.
print("\nReloaded Schema:")
user_log_df_2.printSchema()
