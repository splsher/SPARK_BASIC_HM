import os
import tempfile
import pytest
from pyspark.sql import SparkSession
from src.main.python import main


@pytest.fixture
def spark_session():
    spark = SparkSession.builder \
        .appName('test') \
        .config('spark.jars', r'C:\Users\Tania\Desktop\SPARK_BASIC_HM\gcs-connector-hadoop3-2.2.18.jar') \
        .getOrCreate()
    yield spark
    spark.stop()


def test_main(spark_session):
    with tempfile.TemporaryDirectory() as temp_output_dir:
        main(spark_session, temp_output_dir)

        output_files = os.listdir(temp_output_dir)
        assert "joined_data.parquet" in output_files

        joined_data = spark_session.read.parquet(os.path.join(temp_output_dir, "joined_data.parquet"))
        assert joined_data.count() > 0


if __name__ == "__main__":
    pytest.main([__file__])
