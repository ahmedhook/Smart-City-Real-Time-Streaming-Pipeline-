from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from pyspark.sql.functions import from_json, col
from config import configuration



vehicleSchema = StructType([
    StructField("id", StringType(), True),
    StructField("device_id", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("location", StringType(), True),
    StructField("speed", DoubleType(), True),
    StructField("direction", StringType(), True),
    StructField("make", StringType(), True),
    StructField("model", StringType(), True),
    StructField("year", IntegerType(), True),
    StructField("fueltype", StringType(), True)
])

gpsSchema = StructType([
    StructField("id", StringType(), True),
    StructField("device_id", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("speed", DoubleType(), True),
    StructField("direction", StringType(), True),
    StructField("vehicleType", StringType(), True)
])

trafficSchema = StructType([
    StructField("id", StringType(), True),
    StructField("device_id", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("location", StringType(), True),
    StructField("camera_id", StringType(), True),
    StructField("snapshot", StringType(), True)
])

weatherSchema = StructType([
    StructField("id", StringType(), True),
    StructField("device_id", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("location", StringType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("wind_speed", DoubleType(), True),
    StructField("weather_condition", StringType(), True),
    StructField("precipitation", DoubleType(), True),
    StructField("humidity", DoubleType(), True),
    StructField("air_quality_index", DoubleType(), True)
])

emergencySchema = StructType([
    StructField("id", StringType(), True),
    StructField("device_id", StringType(), True),
    StructField("incident_id", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("location", StringType(), True),
    StructField("emergency_type", StringType(), True),
    StructField("status", StringType(), True),
    StructField("description", StringType(), True)
])




def read_kafka_topic(spark: SparkSession, topic: str, schema: StructType) -> DataFrame:
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "broker:9092")
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .load()
        .selectExpr("CAST(value AS STRING)")
        .select(from_json(col("value"), schema).alias("data"))
        .select("data.*")  
        .withWatermark("timestamp", "2 minutes")
    )




def stream_writer(input: DataFrame, checkpointFolder: str, output: str):
    return input.writeStream \
        .format("parquet") \
        .option("path", output) \
        .option("checkpointLocation", checkpointFolder) \
        .outputMode("append") \
        .start()



def main():
    
    spark = (
        SparkSession.builder
        .appName("SmartCityStreaming")
        .config(
            "spark.jars.packages", 
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
            "org.apache.hadoop:hadoop-aws:3.3.1,"
            "com.amazonaws:aws-java-sdk:1.11.469"
        )
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.access.key", configuration.get('AWS_ACCESS_KEY_ID'))
        .config("spark.hadoop.fs.s3a.secret.key", configuration.get('AWS_SECRET_ACCESS_KEY'))
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", 
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    vehicleDF = read_kafka_topic(spark, "vehicle-data", vehicleSchema)
    gpsDF = read_kafka_topic(spark, "gps-data", gpsSchema)
    trafficDF = read_kafka_topic(spark, "traffic-data", trafficSchema)
    weatherDF = read_kafka_topic(spark, "weather-data", weatherSchema)
    emergencyDF = read_kafka_topic(spark, "emergency-data", emergencySchema)

    query1 = stream_writer(vehicleDF, 
                          checkpointFolder='s3a://spark-streaming-data-big-data/checkpoints/vehicle_data/',
                          output='s3a://spark-streaming-data-big-data/data/vehicle_data/')
    
    query2 = stream_writer(gpsDF, 
                          checkpointFolder='s3a://spark-streaming-data-big-data/checkpoints/gps_data/',
                          output='s3a://spark-streaming-data-big-data/data/gps_data/')
    
    query3 = stream_writer(trafficDF, 
                          checkpointFolder='s3a://spark-streaming-data-big-data/checkpoints/traffic_data/',
                          output='s3a://spark-streaming-data-big-data/data/traffic_data/')
    
    query4 = stream_writer(weatherDF, 
                          checkpointFolder='s3a://spark-streaming-data-big-data/checkpoints/weather_data/',
                          output='s3a://spark-streaming-data-big-data/data/weather_data/')
    
    query5 = stream_writer(emergencyDF, 
                          checkpointFolder='s3a://spark-streaming-data-big-data/checkpoints/emergency_data/',
                          output='s3a://spark-streaming-data-big-data/data/emergency_data/')

    
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()