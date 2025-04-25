# Imports for Spark and Row structure.
from pyspark.sql import SparkSession, Row

# Create Spark session.
spark = SparkSession.builder.appName("Maps and Lazy Evaluation Example").getOrCreate()

# Define sample data.
log_of_songs = [
    "Despacito",
    "Nice for what",
    "No tears left to cry",
    "Despacito",
    "Havana",
    "In my feelings",
    "Nice for what",
    "despacito",
    "All the stars"
]

# Create an RDD from the list.
distributed_song_log_rdd = spark.sparkContext.parallelize(log_of_songs)

# Show the original input data to demonstrate immutability.
print("\nOriginal song list:")
distributed_song_log_rdd.foreach(print)


# Define a function that converts each song title to lowercase.
def convert_song_to_lowercase(song):
    return song.lower()

# Demonstrate the defined function on an example songtitle "Havana".
print("\nLowercase with named function and an example songtitle:")
print(convert_song_to_lowercase("Havana"))

# Apply the function using a map step.
print("\nLowercase with named function, .map() and .foreach():")
distributed_song_log_rdd.map(convert_song_to_lowercase).foreach(print)

# Show that the original input data is still mixed case.
print("\nThe original input data is still mixed case:")
distributed_song_log_rdd.foreach(print)

# Use lambda functions instead of named functions to do the same map operation.
print("\nLowercase with lambda and .foreach():")
distributed_song_log_rdd.map(lambda song: song.lower()).foreach(print)

# Use toDF() to convert from a simple RDD to a DataFrame.
print("\nLowercase with lambda and .toDF():")
lower_case_songs = distributed_song_log_rdd.map(lambda song: song.lower())
lower_case_df = lower_case_songs.map(lambda song: Row(song_title=song)).toDF()
lower_case_df.show()

"""
Notice the use of .foreach() in lieu of .collect() method.
.collect() forces collection of all the data from the entire RDD on all nodes. this kills productivity, and can crash
.foreach() allows the data to stay on each of the independent nodes.
"""
