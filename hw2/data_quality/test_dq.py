import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, IntegerType
from soda.scan import Scan


@pytest.fixture(scope='session')
def spark():
    return SparkSession.builder \
      .master("local") \
      .appName("Spoetry installparkAppTests") \
      .getOrCreate()

def build_scan(name, spark_session):
    scan = Scan()
    scan.disable_telemetry()
    scan.set_scan_definition_name("data_quality_test")
    scan.set_data_source_name("spark_df")
    scan.add_spark_session(spark_session)
    return scan


def test_videos_source(spark):
    videos_df = spark.read.option('header', 'true').option("inferSchema", "true").csv('datasets/USvideos.csv')
    videos_df.createOrReplaceTempView('videos')

    scan = build_scan("videos_source_data_quality_test", spark)
    scan.add_sodacl_yaml_file("data_quality/videos_checks.yml")

    scan.execute()

    scan.assert_no_checks_warn_or_fail()


def test_comments_source_corrupt_records(spark):
    comments_df = spark.read.option('header', 'true')\
        .option("mode", "PERMISSIVE") \
        .option("columnNameOfCorruptRecord", "corrupt_record") \
        .option("inferSchema", "true")\
        .csv('datasets/UScomments.csv')
    comments_df.createOrReplaceTempView('comments')

    scan = build_scan("comments_source_corrupt_records_test", spark)
    scan.add_sodacl_yaml_file("data_quality/comments_check_corrupt_records.yml")

    scan.execute()

    scan.assert_no_checks_warn_or_fail()


def test_comments_source_data_quality(spark):
    comments_schema = StructType([ \
        StructField("video_id", StringType(), True), \
        StructField("comment_text", StringType(), True), \
        StructField("likes", IntegerType(), True), \
        StructField("replies", IntegerType(), True)])

    comments_df = spark.read.option('header', 'true')\
        .option("mode", "DROPMALFORMED") \
        .schema(comments_schema)\
        .csv('datasets/UScomments.csv')
    comments_df.createOrReplaceTempView('comments')

    scan = build_scan("comments_source_data_quality_test", spark)
    scan.add_sodacl_yaml_file("data_quality/comments_checks_data_quality.yml")

    scan.execute()

    scan.assert_no_checks_warn_or_fail()
