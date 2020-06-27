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

df_stars_pair = df_stars.alias("a").join(df_stars.alias("b"), col('a.movie_id') == col('b.movie_id'),"inner")\
    .select(col('a.person_id').alias('ActorID1'),col('b.person_id').alias('ActorID2'),col('b.movie_id').alias('movie_id'))
df_stars_pair = df_stars_pair.where(col('ActorID1') != col('ActorID2')).where(col('ActorID1') < col('ActorID2'))
df_stars_pair_moviescount= df_stars_pair.groupBy('ActorID1','ActorID2').agg(fn.count('movie_id').alias('NoOfMoviesActedTogether'))
df_stars_pair_moviescount=df_stars_pair_moviescount.sort('NoOfMoviesActedTogether',ascending=False).limit(5)

# to get movie count for every actor
df_stars_movie_count = df_stars.groupBy('person_id').agg(fn.count('movie_id').alias('TotalNumberOfMovies'))
df_actors_name_movie_count = df_stars_movie_count.alias('a').join(df_people.alias('b'), col('a.person_id')==col('b.id'),"inner").\
    select(col('a.person_id'),col('b.name'),col('a.TotalNumberOfMovies'))

# final joining to get final dataset
df_actor_pair_details = df_stars_pair_moviescount.alias('a').join(df_actors_name_movie_count.alias('b'),col('a.ActorID1') == col('b.person_id'),"inner")\
    .select(col('a.ActorID1'),col('b.name').alias('ActorName1'),col('b.TotalNumberOfMovies').alias('TotalNumberOfMoviesForActor1')
            ,col('a.ActorID2'),col('a.NoOfMoviesActedTogether'))

df_actor_pair_details = df_actor_pair_details.alias('a').join(df_actors_name_movie_count.alias('b'),col('a.ActorID2') == col('b.person_id'),"inner")\
    .select(col('a.ActorID1'),col('a.ActorName1'),col('a.TotalNumberOfMoviesForActor1')
            ,col('a.ActorID2'),col('b.name').alias('ActorName2'),col('b.TotalNumberOfMovies').alias('TotalNumberOfMoviesForActor2'),col('a.NoOfMoviesActedTogether'))

print(df_actor_pair_details.show(20))

