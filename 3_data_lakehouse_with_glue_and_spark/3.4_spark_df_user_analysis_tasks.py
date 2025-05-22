# Import libraries.
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import countDistinct, udf, desc, sum as Fsum, avg, round as Fround
from pyspark.sql.types import IntegerType

# Create Spark session.
spark = SparkSession.builder.getOrCreate()

# Read JSON into DataFrame.
user_log_df = spark.read.json("data/sparkify_log_small.json")


# Task 1: Which page/s did user id ""(empty string) NOT visit?

# Pages visited by empty-string users
empty_user_pages = user_log_df.select("page").filter("userId == ''").dropDuplicates()

# Pages visited by valid users
valid_user_pages = user_log_df.select("page").filter("userId != ''").dropDuplicates()

print("\nPages NOT visited by the empty-string userId:")
valid_user_pages.subtract(empty_user_pages).show()


# Task 2 - Reflect: What type of user does the empty string user id most likely refer to?

"""
Select distinct values in each column for the empty string userId.
for col in user_log_df.columns:
    print(f"\nDistinct values in column '{col}':")
    user_log_df.select(col).filter(user_log_df.userId == "").distinct().show(truncate=False)

These are distinct values for the empty string userId:
  - artist, firstName, gender, lastName, length, location, registration, song, userAgent, userId = NULL
  - auth = Logged Out, Guest
  - itemInSession, sessionId, ts = various
  - level = free, paid
  - method = PUT, GET
  - page = Home, About, Login, Help
  - status = 307, 200

- No userAgent log: Maybe visits through an incognito/guest window.
- Pages visited [Home, About, Login, Help], registration=NULL: Maybe pre-login, non-registered, non-signed-up users.
- No [userId, artist, song], but there's [itemInSession, sessionId, level, status]: Maybe post-login behavior, between steps.
"""


# Task 3: Number of female users in the data set

print("\nNumber of female users in the data set:")
user_log_df.filter("gender = 'F'").select(countDistinct("userId").alias("female_users")).show()


# Task 4: Total play count for the most frequently played artist

print("\nTotal play count for the most frequently played artist:")
user_log_df.select("artist") \
    .filter("page == 'NextSong'") \
    .groupby("artist") \
    .count() \
    .orderBy("count", ascending=False) \
    .show(1)


# Task 5: Average number (rounded) of songs users listen to between visiting our home page:

# Create a UDF to return 1 if user is on Home page.
flag_home_visit = udf(lambda x: 1 if x == "home" else 0, IntegerType())

# Create a window partitioned by userId, ordered by timestamp.
windowval = Window.partitionBy("userId").orderBy("ts").rangeBetween(Window.unboundedPreceding, 0)

# Add [home, phase] columns.
user_log_df = user_log_df \
    .withColumn("home", flag_home_visit("page")) \
    .withColumn("phase", Fsum("home").over(windowval))

print("\nVerify addition of [home, phase] columns:")
user_log_df.show(1)

# Filter for actual songs played (NextSong page), group by user and phase, count songs per phase.
# Then compute average across all user-phases.
print("\nAverage number (rounded) of songs users listen to between visiting our home page:")
user_log_df.select("userId", "song", "phase") \
    .filter("page == 'NextSong'") \
    .groupby(["userId", "phase"]) \
    .count() \
    .agg(Fround(avg("count")).alias("avg_songs")) \
    .show()
