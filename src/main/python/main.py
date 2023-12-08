import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import pandas as pd
import requests


def main():
    os.environ[
        "GOOGLE_APPLICATION_CREDENTIALS"] = r'C:\Users\Tania\Desktop\SPARK_BASIC_HM\service_account_key' \
                                            r'\application_default_credentials.json '

    gcs_jar_path = r"C:\Users\Tania\Desktop\SPARK_BASIC_HM\gcs-connector-hadoop3-2.2.18.jar"

    spark = SparkSession.builder \
        .appName('SPARK_BASIC_HM') \
        .config('spark.jars', gcs_jar_path) \
        .config('spark.hadoop.google.cloud.auth.service.account.json.keyfile',
                r"C:\Users\Tania\Desktop\SPARK_BASIC_HM\application_default_credentials.json") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    hotels_path = "gs://bucket_tan/m06sparkbasics/hotels"
    weather_path = "gs://bucket_tan/m06sparkbasics/weather"

    df_hotels = spark.read.option("inferSchema", "true").option("header", "true").csv(
        "gs://bucket_tan/m06sparkbasics/hotels")
    df_weather = spark.read.option("inferSchema", "true").option("header", "true").csv(
        "gs://bucket_tan/m06sparkbasics/weather")

    schema = ["Column_" + str(i) for i in range(len(df_weather.columns))]
    hotels_data = pd.DataFrame(columns=schema)

    api_key = '13ca66f530c14f04ba49ba4cf9773fd2'

    for row in df_hotels.rdd.collect():
        if pd.isnull(row["Latitude"]) or pd.isnull(row["Longitude"]):
            # Make API request to get correct latitude and longitude
            api_url = f'https://api.opencagedata.com/geocode/v1/json?key={api_key}&q={row["Name"]}'
            response = requests.get(api_url)

            if response.status_code == 200:
                data = response.json()

                if 'results' in data and data['results']:
                    correct_latitude = data['results'][0]['geometry']['lat']
                    correct_longitude = data['results'][0]['geometry']['lng']

                    hotels_data = hotels_data.append({
                        "Latitude": correct_latitude,
                        "Longitude": correct_longitude,
                        "Name": row["Name"]
                    }, ignore_index=True)

    hotels_spark = spark.createDataFrame(hotels_data)

    joined_data = hotels_spark.join(
        df_weather,
        (hotels_spark["Latitude"] == df_weather["Latitude"]) & (hotels_spark["Longitude"] == df_weather["Longitude"]),
        "left"
    )

    joined_data.write.parquet("gs://bucket_tan/data/joined_data.parquet", mode="overwrite")

    spark.stop()


if __name__ == "__main__":
    main()
