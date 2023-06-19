from video_analytics.functions import split_tags

def test_split_tags_with_tags(spark):
    # Подготовка данных
    source_data = "tag1|tag2|tag3"
    source_df = spark.createDataFrame([(source_data,)], ["tags"])

    # Применение UDF
    actual_df = source_df.withColumn("split_tags", split_tags(source_df.tags))

    # Проверка результатов
    expected_result = ["tag1", "tag2", "tag3"]
    actual_result = actual_df.select("split_tags").first()[0]
    assert actual_result == expected_result


def test_split_tags_with_empty_tags(spark):
    # Подготовка данных
    source_data = ""
    source_df = spark.createDataFrame([(source_data,)], ["tags"])

    # Применение UDF
    actual_df = source_df.withColumn("split_tags", split_tags(source_df.tags))

    # Проверка результатов
    expected_result = []
    actual_result = actual_df.select("split_tags").first()[0]
    assert actual_result == expected_result


def test_split_tags_with_single_tag(spark):
    # Подготовка данных
    source_data = "tag1"
    source_df = spark.createDataFrame([(source_data,)], ["tags"])

    # Применение UDF
    actual_df = source_df.withColumn("split_tags", split_tags(source_df.tags))

    # Проверка результатов
    expected_result = ["tag1"]
    actual_result = actual_df.select("split_tags").first()[0]
    assert actual_result == expected_result


def test_split_tags_with_special_characters(spark):
    # Подготовка данных
    source_data = "tag1|tag3##tag4$tag5:|tag3"
    source_df = spark.createDataFrame([(source_data,)], ["tags"])

    # Применение UDF
    actual_df = source_df.withColumn("split_tags", split_tags(source_df.tags))

    # Проверка результатов
    expected_result = ["tag1", "tag3##tag4$tag5:", "tag3"]
    actual_result = actual_df.select("split_tags").first()[0]
    assert actual_result == expected_result




