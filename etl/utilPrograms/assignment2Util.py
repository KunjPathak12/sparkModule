from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
import warnings
warnings.simplefilter("ignore")

spark = SparkSession.builder.appName("assignment-2").config("spark.some.config.option","some-value").getOrCreate()

schema = StructType([StructField("Logging Level",StringType(),True),\
                     StructField("timestamp",DateType(),True),\
                     StructField("downloader_id",StringType(),True)])
spark.sparkContext.setLogLevel("ERROR")

filePath = input("input a proper absolute reference to a csv file:\n ")


def loadDataToRDD(filePath):
  myRdd = spark.sparkContext.textFile(filePath)
  return myRdd

def totalLinesRdd(loadDataToRDD):
    tline = loadDataToRDD(filePath)
    return ("Ans2: Total lines in rdd are : {}".format(tline.count()))

def totalWarnRdd(loadDataToRDD):
    twline = loadDataToRDD(filePath)
    return ("Ans3: Total Warnings present in the logFile are : {}".format(twline.filter(lambda x: "WARN" in x).count()))

def totalRepoProcessed(loadDataToRDD):
    trepo = loadDataToRDD(filePath)
    return("Ans4: Total Repositories processed are : {}".format(trepo.filter(lambda x: "api_client" in x).count()))



def makeDf(spark,schema):
    # global  dfA2
    dfA2 = spark.read.csv("/home/kp03/sparkModule2/datasets/ghtorrent-logs.txt", inferSchema=True, schema=schema)
    return dfA2

def articulatedDf(makeDf):
    dfA2 = makeDf(spark,schema)
    finalDF = dfA2.withColumn('id', split(col("downloader_id"), "--").getItem(0)) \
        .withColumn('clientInter', split(col("downloader_id"), "--").getItem(1)) \
        .withColumn('client', split(col("clientInter"), ":").getItem(0)) \
        .withColumn('jobInfo', split(col("clientInter"), ":").getItem(1))
    finalDF = finalDF.drop("clientInter", "downloader_id")
    return finalDF


def mostHttp(articulatedDf):
    finaldf = articulatedDf(makeDf)
    dfMostReq = finaldf.filter(finaldf.jobInfo.contains("request")).groupBy("id").count()
    df = dfMostReq.groupBy("id").max("count").orderBy(col("max(count)").desc())
    df = df.withColumn("sr_no",row_number().over(Window.orderBy(monotonically_increasing_id())))
    ans = df.filter(col("sr_no") == 1)
    ans = ans.withColumnRenamed("max(count)","maxHttpRequest")\
        .drop("sr_no")
    print("Ans5: Max http requests done by user is mentioned below with details: " )
    ans.show()

def failedRequest(articulatedDf):
    finaldf = articulatedDf(makeDf)
    dfFailed = finaldf.filter(finaldf.jobInfo.contains("Failed")).groupBy("id").count().orderBy(col("count").desc())
    ans1 = dfFailed.withColumn("sr_no",row_number().over(Window.orderBy(monotonically_increasing_id())))
    ans1 = ans1.groupBy("id","sr_no").max("count").filter(ans1.sr_no == 1).select("id",col("max(count)").alias("maxFailedRequest"))
    print("Ans6: Max Failed requests done by user is mentioned below with details: ")
    ans1.show()

def activeHourOfTheDay(articulatedDf):
    finaldf = articulatedDf(makeDf)
    df = finaldf.groupBy("timestamp").count()
    df = df.withColumn("sr_no",row_number().over(Window.orderBy(monotonically_increasing_id()))).na.drop()
    df = df.groupBy("sr_no","timestamp").max("count").filter(df.sr_no == 1).select(col("timestamp").alias("mostRepeatedDate"),col("max(count)").alias("noOfReps"))
    print("Ans7: Most repeated date is: ")
    df.show()


def mostActiveRepo(articulatedDf):
    finaldf = articulatedDf(makeDf)
    df = finaldf.withColumn("mostActiveRepo",split(col("jobInfo")," ").getItem(2))
    df = df.filter(df.jobInfo.contains("Repo")).filter(df.client.contains("ghtorrent.rb")).groupBy("id","mostActiveRepo").agg(count("mostActiveRepo").alias("timesUsed")).sort(col("timesUsed").desc())\
        .withColumn("sr_no",row_number().over(Window.orderBy(monotonically_increasing_id())))
    df = df.select("mostActiveRepo","timesUsed").where(df.sr_no == 1)
    print("Ans8: The most Active Repo with highest times used is:")
    df.show()
