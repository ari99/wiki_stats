"""
Testing module.
Usage:
py.test --capture=no
"""
import pytest
from operations import Operations
from pyspark import SparkConf
from pyspark.sql import HiveContext
from pyspark import SparkContext
import operations

def test_clean_string():
    """
     Test operations.clean_string(str).
    """
    str = "John_Person%27s_first_100_days"
    assert operations.clean_string(str) == "john person s first 100 days"

def test_tokenize_porter():
    """
    Test operations.tokenize_porter(str).
    """
    str="john person s first 100 days"
    assert operations.tokenize_porter(str) == ['john', 'person', 'first', '100', 'day']

@pytest.fixture(scope="session")
def spark_context(request):
    """
    Pytest fixture for creating a spark context.
    Args:
        :param request: pytest.FixtureRequest object
    """
    conf = (SparkConf().setMaster("local").setAppName("pyspark-local-testing"))
    sc = SparkContext(conf=conf)
    sc.setLogLevel("ERROR")
    request.addfinalizer(lambda: sc.stop())
    return sc

@pytest.fixture(scope="session")
def hive_context(spark_context):
    """
    Fixture for creative a hive context.
    Args:
        :param spark_context: SparkContext object from fixture.
    Returns:
        :return: HiveContext object.
    """
    return HiveContext(spark_context)

@pytest.fixture(scope="session")
def df(spark_context, hive_context):
    """
    Fixture for creative a test dataframe.
    Args:
        :param spark_context: SparkContext object from fixture.
        :param hive_context: HiveContext object from fixture.
    Returns:
        :return: DataFrame object.
    """
    input = ['ace Beubiri 10 12744',
             'ace Bhutan 20 31284',
             'ace Bireu%c3%abn 30 20356',
             'ace Bireuen 40 20347',
             'ace Bishkek 50 14665',
             'ace John_Person%27s_first_100_days 60 14576',
             'ace Bolivia 70 32058',
             'ace Bosnia_H%c3%a8rz%c3%a8govina 80 38777']
    rdd = spark_context.parallelize(input)
    ops = Operations()
    df = ops.create_dataframe(rdd, hive_context)
    return df

def test_create_dataframe(df):
    """
    Tests operations.create_dataframe .
    Args:
        :param df: DataFrame object from fixture.
    """
    assert df.count() == 8
    assert df.where(df['pagename']=='Bolivia').select("bytes").first()['bytes'] == 32058

def test_clean_string_column(df):
    """
    Tests operations.clean_string_column .
    Args:
        :param df: DataFrame object from fixture.
    """
    ops = Operations()
    df =ops.clean_string_column(df, 'pagename')
    assert df.where(df['pageviews']==60).select("pagename").first()['pagename'] == "john person s first 100 days"

def test_append_tokens(df):
    """
    Tests operations.append_tokens.
    Args:
        :param df: DataFrame object from fixture.
    """
    ops = Operations()
    df =ops.clean_string_column(df, 'pagename')
    df = ops.append_tokens(df)
    #The stop word 'first' is removed
    # see for a list of stop words: http://ir.dcs.gla.ac.uk/resources/linguistic_utils/stop_words
    assert df.where(df['pageviews']==60).select("tokens").first()['tokens'] == ['john', 'person', '100', 'day']
