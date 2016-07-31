"""
This module holds the various data transformations for the project.
"""
import urllib
from pyspark.sql.functions import *
import re

from pyspark.ml.feature import StopWordsRemover
from pyspark.sql.types import ArrayType,StringType
from nltk.stem.porter import PorterStemmer
from pyspark.ml.feature import HashingTF, IDF
from pyspark.sql import Row
from pyspark.sql import Window
from pyspark import StorageLevel

# These are created dynamically by _create_window_function or _create_function,
#  therefore your IDE may not be able to find them
try:
    from pyspark.sql.functions import lower
    from pyspark.sql.functions import dense_rank
except ImportError as e:
    print ("Can not import Spark Modules", e)
    sys.exit(1)

# These functions are outside of the class so spark doesn't
#  serialize the whole class when they are used.
def clean_string(str):
    """
    Clean and remove extra characters from pagename.

    Args:
        :param str: Original string to clean.
    Returns:
        :return: Cleaned string.
    """
    str = urllib.unquote_plus(str)
    str = str.replace('_', ' ')
    str = str.replace('  ', ' ')
    str = re.sub('\W', ' ', str)
    str = str.lower()
    str = str.replace('  ', ' ')
    str = str.strip()
    return str


def tokenize_porter(text):
    """
    Creates a list of stem words from parameter.
    For more information on stemming see: http://www.nltk.org/howto/stem.html

    Args:
        :param text: Text to stem and tokenize.
    Returns:
        :return: List of stem words
    """
    porter = PorterStemmer()
    stems = [porter.stem(word) for word in text.split() if len(word) > 1]
    return stems

class Operations():

    def create_dataframe(self, rdd, sqlContext):
        """
        Creates a dataframe from an RDD.
        Args:
            :param rdd: RDD to use to create dataframe.
            :param sqlContext: HiveContext used to create a dataframe.
        Returns:
            :return: dataframe
        """
        #Create an RDD of lists from an RDD of strings.
        parts = rdd.map(lambda l: l.split(" "))
        #Convert the lists to Row objects
        pages = parts.map(lambda p: Row(projectcode=p[0], pagename=p[1], pageviews=int(p[2]), bytes=int(p[3])))
        #Convert the RDD to a Dataframe
        df = sqlContext.createDataFrame(pages)
        return df


    def clean_string_column(self, df, column_name):
        """
        Cleans dataframe column using a user defined function.
        Args:
            :param df: Dataframe with column to clean.
            :param column_name: Name of the string column to clean.
        Returns:
            :return: Dataframe with column cleaned.
        """
        clean_udf = udf(clean_string)
        df = df.withColumn(column_name, clean_udf(df[column_name]))
        return df

    def append_date_columns(self, df):
        """
        Creates year, month, day and hour columns from the file name strings of the input.
        Args:
            :param df: Dataframe to add columns to.
        Returns:
            :return: Dataframe with columns added.
        """
        df = df.withColumn("year", substring(input_file_name(), -18, 4))
        df = df.withColumn("month", substring(input_file_name(), -14, 2))
        df = df.withColumn("day", substring(input_file_name(), -12, 2))
        df = df.withColumn("hour", substring(input_file_name(), -9, 2))
        return df

    def aggregate_times(self, df):
        """
        Creates GroupedData from dataframe parameter and sends the GroupedData to
        aggregate_rename_columns to do the aggregation on pageviews and bytes.
        Creates a dataframe for each time frame. Within an hour, this method is used to
        combine records that are now for equal pagenames, whereas before cleaning the pagename
        the records were separate.
        Args:
            :param df: Dataframe to do the aggregation on.
        Returns:
            :return: hour_df, day_df, month_df, year_day. Dataframes for each timeframe.
        """
        df.persist(StorageLevel.MEMORY_AND_DISK)
        hour_df = self.aggregate_rename_columns(
            df.groupBy(['pagename', 'projectcode', 'year', 'month', 'day', 'hour']))
        day_df = self.aggregate_rename_columns(
            df.groupBy(['pagename', 'projectcode', 'year', 'month', 'day']))
        month_df =self.aggregate_rename_columns(
            df.groupBy(['pagename', 'projectcode', 'year', 'month']))
        year_df = self.aggregate_rename_columns(
            df.groupBy(['pagename', 'projectcode', 'year']))
        return hour_df, day_df, month_df, year_df

    def aggregate_rename_columns(self,gdf):
        """
        Takes a GroupedData object. Sums pageviews and takes the max of bytes. Renames the columns back to
        pageviews and bytes after doing aggregation.
        Args:
            :param gdf: GroupedData object.
        Returns:
            :return: Dataframe with pageviews and bytes aggregated.
        """
        df = gdf.agg({'pageviews': 'sum', 'bytes': 'max'})
        df = df.withColumnRenamed('sum(pageviews)', 'pageviews').withColumnRenamed('max(bytes)', 'bytes')
        return df

    def append_tokens(self,df):
        """
        Creates tokens from the pagename column in the dataframe then removes
         stop-words from the tokens. Adds the tokens under the column rawTokens and tokens.
        Args:
            :param df: Dataframe to add token columns to.
        Returns:
            :return: Dataframe with new columns rawTokens and tokens.
        """
        #Tokenize pagename and convert tokens to their stem words.
        tokenize_udf = udf(tokenize_porter, returnType=ArrayType(StringType()))
        df = df.withColumn('rawTokens', tokenize_udf(df['pagename']))
        #Remove stop words.
        stop_words_remover = StopWordsRemover(inputCol="rawTokens", outputCol="tokens")
        df = stop_words_remover.transform(df)
        return df

    def append_tf_idf(self, df):
        """
        Calculate term frequency and inverse document frequency
         based on at least 1 visit hourly in this case. Compares how often the tokens appeared
         at least once per hour compared to other tokens. Not used for the main purpose of the project.
        Args:
            :param df: Dataframe parameter.
        Returns:
            :return:  Dataframe with term frequency and inverse document frequency added in the columns
                        'rawFeatures' and 'features' respectively.
        """
        #Create TF column.
        hashingTF = HashingTF(inputCol="tokens", outputCol="rawFeatures", numFeatures=100000)
        tf = hashingTF.transform(df)
        tf.persist(StorageLevel.MEMORY_AND_DISK)
        #Create IDF column.
        idf = IDF(inputCol="rawFeatures", outputCol="features")
        idfModel = idf.fit(tf)
        tfidf = idfModel.transform(tf)
        return tfidf

    def append_ranks(self, hour_df, day_df, month_df, year_df):
        """
        Ranks based on pageviews within categories. For hour_df the category is a single hour,
         for day_df the category is a single day, and so on.
        Args
            :param hour_df: Dataframe with hourly aggregated data.
            :param day_df: Dataframe with daily aggregated data.
            :param month_df: Dataframe with monthly aggregated data.
            :param year_df: Dataframe with yearly aggregated data.
        Returns:
            :return: hour_df, day_df, month_df, year_df with ranks added.
        """
        #Specify windows to rank within.
        hour_window = Window.partitionBy(['year', 'month', 'day', 'hour']).orderBy(hour_df['pageviews'].desc())
        day_window = Window.partitionBy(['year', 'month', 'day']).orderBy(day_df['pageviews'].desc())
        month_window = Window.partitionBy(['year', 'month']).orderBy(month_df['pageviews'].desc())
        year_window = Window.partitionBy(['year']).orderBy(year_df['pageviews'].desc())

        #Create ranking functions
        hour_rank = dense_rank().over(hour_window)
        day_rank = dense_rank().over(day_window)
        month_rank = dense_rank().over(month_window)
        year_rank = dense_rank().over(year_window)

        #Apply the ranking functions to make new columns
        hour_df = hour_df.withColumn("hour_rank", hour_rank)
        day_df = day_df.withColumn("day_rank", day_rank)
        month_df = month_df.withColumn("month_rank", month_rank)
        year_df = year_df.withColumn("year_rank", year_rank)
        return hour_df, day_df, month_df, year_df

    def make_plot_csv(self, df, dirname):
        """
        Creates a csv file on s3 from the dataframe. The result will be in 'dirname' with a file name
        like part-0001 because we gather the data onto a single partition using coalesce before exporting.

        Args:
            :param df: Dataframe to export.
            :param dirname: Name fo the resulting directory on s3.
        """
        df.coalesce(1).write.format('com.databricks.spark.csv').option("header", "true").save("s3n://com.my.output/"+dirname)

