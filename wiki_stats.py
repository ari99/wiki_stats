"""
This module creates csv files on s3 with the top 200 most visiting pages in wikipedia per hour, day, month, and year.

This module can use data from these and similar data sources:
  https://aws.amazon.com/datasets/wikipedia-traffic-statistics-v2/
  https://aws.amazon.com/datasets/wikipedia-page-traffic-statistic-v3/
Originally from:
  https://dumps.wikimedia.org/other/pagecounts-raw/

This file can be run locally.

To run on Amazon EMR:
  For spark-submit options enter:
    --packages org.apache.hadoop:hadoop-aws:2.7.2,com.databricks:spark-csv_2.10:1.0.3 --py-files /home/hadoop/operations.py
  For application location, enter where you put this file on the master node, for example:
    /home/hadoop/wiki_stats.py

"""
import os
from pyspark import SparkConf
from pyspark.sql import HiveContext
from pyspark.sql.functions import *
from pyspark import SparkContext


# These are created dynamically by _create_window_function or _create_function,
#  therefore your IDE may not be able to find them
try:
    from pyspark.sql.functions import lower
    from pyspark.sql.functions import dense_rank
except ImportError as e:
    print ("Can not import Spark Modules", e)
    sys.exit(1)
mode = "local"

if mode == "local":
    #Include the spark-csv and hadoop-aws packages
    SUBMIT_ARGS = "--packages com.databricks:spark-csv_2.11:1.2.0,org.apache.hadoop:hadoop-aws:2.7.2 pyspark-shell"
    os.environ["PYSPARK_SUBMIT_ARGS"] = SUBMIT_ARGS
    conf = SparkConf().setMaster("local").setAppName("Wiki Stats")
    sc = SparkContext(conf = conf)
    lines = sc.textFile("./testdata")
    sc.setLogLevel("ERROR")
    sc.addPyFile("./operations.py")

else:
    sc = SparkContext(appName="wikistats")
    lines = sc.textFile("s3n://my.wiki.bucket.com/wikidata")

sc._jsc.hadoopConfiguration().set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
sc._jsc.hadoopConfiguration().set("fs.s3n.awsAccessKeyId", "###")
sc._jsc.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey", "#####")


sqlContext = HiveContext(sc)


from operations import Operations

ops = Operations()
#Create the dataframe from the lines RDD
df = ops.create_dataframe(lines, sqlContext)
#Clean the 'pagename' column of encoded characters
df = ops.clean_string_column(df, 'pagename')
#Add columns for hour, day, month, year from the file name
df = ops.append_date_columns(df)

#Group by timeframes
hour_df, day_df, month_df, year_df = ops.aggregate_times(df)
#Create tokens from the pagename
hour_df = ops.append_tokens(hour_df)
#Add term frequency and inverse document frequency
hour_df = ops.append_tf_idf(hour_df)
#Create ranking
hour_df, day_df, month_df, year_df = ops.append_ranks(hour_df, day_df, month_df, year_df)

#Get the top 200 for each timeframe
top_hourly = hour_df.filter(hour_df['hour_rank']<201)
top_daily =  day_df.filter(day_df['day_rank']<201)
top_monthly =  month_df.filter(month_df['month_rank']<201)
top_yearly =  year_df.filter(year_df['year_rank']<201)

#Create files on s3 with the results
ops.make_plot_csv(top_hourly,"hourly")
ops.make_plot_csv(top_daily,"daily")
ops.make_plot_csv(top_monthly,"monthly")
ops.make_plot_csv(top_yearly,"yearly")
