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
# to filter movies with less than 100 votes
df_ratings_1=df_ratings.filter(df_ratings["votes_int"] >= 100)

df_directors_movies = df_directors.join(df_ratings_1,df_directors["movie_id"] == df_ratings_1["movie_id"], "inner")\
    .select(df_directors["movie_id"],df_directors["person_id"].alias('Director_Id'),df_ratings_1["rating"],df_ratings_1["votes_int"])

# Filtering directors who directed less than 4 movies
df_directors_movies_agg = df_directors_movies.groupBy('Director_Id').agg(fn.avg('rating').alias('AverageRatingOfMoviesDirected'),fn.count('movie_id').alias('TotalNoOfMoviesDirected'))
df_directors_movies_agg=df_directors_movies_agg.filter(df_directors_movies_agg["TotalNoOfMoviesDirected"] >= 4)\
    .sort('AverageRatingOfMoviesDirected',ascending=False).limit(10)
df_directors_movies_agg = df_directors_movies_agg.alias('a').join(df_people.alias('b'),df_directors_movies_agg['Director_Id']==df_people['Id'], "inner" )\
    .select(df_directors_movies_agg['Director_Id'],df_people['name'].alias('DirectorName'),df_directors_movies_agg['AverageRatingOfMoviesDirected'],df_directors_movies_agg['TotalNoOfMoviesDirected'])
df_directors_movies_agg=df_directors_movies_agg.sort('AverageRatingOfMoviesDirected',ascending=False)
print(df_directors_movies_agg.show(10))




