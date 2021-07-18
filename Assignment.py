from pyspark.sql import SparkSession
from pyspark.sql.functions import substring
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, LongType

if __name__ == '__main__':

    spark = SparkSession.builder.appName("sairamm").master("local[*]").getOrCreate()

    flghtSchema2 = StructType([
        StructField("Year", IntegerType()),
        StructField("Month", IntegerType()),
        StructField("DayofMonth", IntegerType()),
        StructField("DayOfWeek", IntegerType()),
        StructField("DepTime", StringType()),
        StructField("CRSDepTime", IntegerType()),
        StructField("ArrTime", StringType()),
        StructField("CRSArrTime", IntegerType()),
        StructField("UniqueCarrier", StringType()),

        StructField("FlightNum", IntegerType()),
        StructField("TailNum", StringType()),
        StructField("ActualElapsedTime", StringType()),

        StructField("CRSElapsedTime", IntegerType()),
        StructField("AirTime", StringType()),
        StructField("ArrDelay", StringType()),
        StructField("DepDelay", StringType()),
        StructField("Origin", StringType()),

        StructField("Dest", StringType()),
        StructField("Distance", IntegerType()),
        StructField("TaxiIn", StringType()),
        StructField("TaxiOut", StringType()),

        StructField("Cancelled", IntegerType()),
        StructField("CancellationCode", StringType()),
        StructField("Diverted", IntegerType()),
        StructField("CarrierDelay", StringType()),

        StructField("WeatherDelay", StringType()),
        StructField("NASDelay", StringType()),
        StructField("SecurityDelay", StringType()),
        StructField("LateAircraftDelay", StringType())])


    df = spark.read\
        .format("csv")\
        .option("header", False)\
        .option("mode", "DROPMALFORMED")\
        .schema(flghtSchema2)\
        .option("path", "C:\\Users\\nithe\\workspace\\week16\\src\\interview\\dataset_flight_raw.csv")\
        .load()

    df.show(10)

    df2 = df.withColumn('LateAircraftDelay', substring('LateAircraftDelay', 1, 2)) #removing ; at the end

    df2.show(10)

    df2.printSchema()