from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
import pyspark.sql.functions as fn
from pyspark.sql import *
from pyspark.context import SparkContext
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.functions import regexp_replace
from pyspark.sql.functions import concat
from pyspark.sql.functions import col
from pyspark.sql.functions import lit, count
from pyspark.sql.functions import months_between
from pyspark.sql import Row
from pyspark.sql.functions import coalesce
from pyspark.sql import Window
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType,DoubleType
from pyspark.sql.functions import unix_timestamp, from_unixtime , to_date, col, count, avg ,countDistinct,udf, when, lit

spark = SparkSession.builder \
    .config("spark.executor.extraClassPath", "C:/Users/aditya/sqlite/sqlite-jdbc-3.8.6.jar") \
    .config('spark.driver.extraClassPath',
            "C:/Users/aditya/sqlite/sqlite-jdbc-3.8.6.jar") \
    .config("jars", "C:/Users/aditya/sqlite/sqlite-jdbc-3.8.6.jar")\
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .enableHiveSupport() \
    .getOrCreate()


df_ratings = spark.read.format('jdbc').options(url='jdbc:sqlite:C:/Users/aditya/sqlite/movies.db',dbtable='ratings',driver='org.sqlite.JDBC').load()
df_directors = spark.read.format('jdbc').options(url='jdbc:sqlite:C:/Users/aditya/sqlite/movies.db',dbtable='directors',driver='org.sqlite.JDBC').load()
df_movies = spark.read.format('jdbc').options(url='jdbc:sqlite:C:/Users/aditya/sqlite/movies.db',dbtable='movies',driver='org.sqlite.JDBC').load()
df_people = spark.read.format('jdbc').options(url='jdbc:sqlite:C:/Users/aditya/sqlite/movies.db',dbtable='people',driver='org.sqlite.JDBC').load()
df_stars = spark.read.format('jdbc').options(url='jdbc:sqlite:C:/Users/aditya/sqlite/movies.db',dbtable='stars',driver='org.sqlite.JDBC').load()

df_movies=df_movies.withColumn("year1",df_movies["year"].cast(IntegerType()))

df_ratings=df_ratings.withColumn("votes_int",df_ratings["votes"].cast(IntegerType()))
df_movies_count=df_movies.groupBy('year1').agg(fn.count('id').alias('NumberOfMovies'))
# To calculate maximum number of movies in a year
df_movies_max=df_movies_count.sort('NumberOfMovies',ascending=False).limit(1)
df_movies_max=df_movies_max.select(df_movies_max['year1']).withColumn("category", fn.lit("Most Number of Movies"))
df_movies_max=df_movies_max.select(df_movies_max['category'],df_movies_max['year1'].alias('year'))
#print(df_movies_max.show(1,truncate=False))
# To calculate minimum number of movies in a year
df_movies_min=df_movies_count.sort('NumberOfMovies',ascending=True).limit(1)
df_movies_min=df_movies_min.select(df_movies_min['year1']).withColumn("category", fn.lit("Least Number of Movies"))
df_movies_min=df_movies_min.select(df_movies_min['category'],df_movies_min['year1'].alias('year'))
#print(df_movies_min.show(1,truncate=False))

# To calculate average rating
df_movie_year_rating = df_movies.join(df_ratings,df_movies['id']==df_ratings['movie_id'],"inner")\
    .select(df_movies['id'],df_movies['year1'],df_ratings['rating'])
df_movie_year_rating_average=df_movie_year_rating.groupBy('year1').agg(fn.avg('rating').alias('YearlyAverageRating'))

# To calculate Highest Rated Year (year in which average rating of movies is the highest)
df_movies_highest_rated_year=df_movie_year_rating_average.sort('YearlyAverageRating',ascending=False).limit(1)
df_movies_highest_rated_year=df_movies_highest_rated_year.select(df_movies_highest_rated_year['year1']).withColumn("category", fn.lit("Highest Rated Year"))
df_movies_highest_rated_year=df_movies_highest_rated_year.select(df_movies_highest_rated_year['category'],df_movies_highest_rated_year['year1'].alias('year'))
# print(df_movies_highest_rated_year.show(1,truncate=False))

# Lowest Rated Year (year in which average rating of movies is the lowest)
df_movies_lowest_rated_year=df_movie_year_rating_average.sort('YearlyAverageRating',ascending=True).limit(1)
df_movies_lowest_rated_year=df_movies_lowest_rated_year.select(df_movies_lowest_rated_year['year1']).withColumn("category", fn.lit("Lowest Rated Year"))
df_movies_lowest_rated_year=df_movies_lowest_rated_year.select(df_movies_lowest_rated_year['category'],df_movies_lowest_rated_year['year1'].alias('year'))
# print(df_movies_lowest_rated_year.show(1,truncate=False))

# Doing Union all of all intermediate dataframes to create resultant Dataframe

df_final = df_movies_max.unionAll(df_movies_highest_rated_year).unionAll(df_movies_min).unionAll(df_movies_lowest_rated_year)
print(df_final.show(truncate=False))







