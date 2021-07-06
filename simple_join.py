from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.conf import SparkConf
import time

if __name__ == "__main__":
    spark = SparkSession\
    .builder.appName("RandomStreamJoin").getOrCreate()


    spark.sparkContext.setLogLevel("ERROR")
    #spark.sparkContext.setLogLevel("TRACE")

    spark.conf.set("spark.sql.session.timeZone", "UTC")

    schema = StructType([StructField("Time", LongType(), True),\
        StructField("Payload", IntegerType(), True)])

    Df1 = spark\
        .readStream\
        .format("csv")\
        .option("header", True)\
        .option("path", "./data/spark_stream_dir1")\
        .schema(schema)\
        .load()

    Df2 = spark\
        .readStream\
        .format("csv")\
        .option("header", True)\
        .option("path", "./data/spark_stream_dir2")\
        .schema(schema)\
        .load()
    

 
    result = Df1.join(Df2, [Df1.Time == Df2.Time, Df1.Payload > Df2.Payload])




    startTime = time.time()
    query = result.writeStream.format("console").trigger(once=True).start().awaitTermination() 
    #query = wordCount.writeStream.format("console").start().awaitTermination() 
    endTime = time.time()
    print(endTime - startTime) 
 
    