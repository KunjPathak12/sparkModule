from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from etl.utilPrograms.pysparkAssignmentHelpingFunctions import *
import warnings
warnings.simplefilter("ignore")

spark = SparkSession.builder.appName("PySpark-Assignment").config("spark.some.config.option","some-value").getOrCreate()

Schema = StructType([StructField("Product Name",StringType(),True),\
                     StructField("Issue Date",StringType(),True),\
                     StructField("Price",LongType(),True),\
                     StructField("Brand",StringType(),True),\
                     StructField("Country",StringType(),True),\
                     StructField("ProductNumber",StringType(),False)
                     ])

Data = [("Washing Machine","1648770933000",20000,"Samsung","India","0001"),\
        ("Refrigerator","1648770999000",35000," LG ","","0002"),\
        ("Air Cooler","1648770948000",45000," Voltas ","","0003")]

Schema1 = StructType([StructField("SourceId",LongType(),True),\
                     StructField("TransactionNumber",LongType(),True),\
                     StructField("Language",StringType(),True),\
                     StructField("ModelNumber",LongType(),True),\
                     StructField("StartTime",StringType(),True),\
                     StructField("ProductNumber",StringType(),False)
                     ])

Data1 = [(150711,123456,"EN",456789,"2021-12-27T08:20:29.842+0000","0001"),\
        (150439,234567,"UK",345678,"2021-12-27T08:21:14.645+0000","0002"),\
        (150647,345678,"ES",234567,"2021-12-27T08:22:42.445+0000","0003")]
spark.sparkContext.setLogLevel("ERROR")
def createDataframeTable1(Data,Schema):
    df = spark.createDataFrame(data=Data, schema=Schema)
    return df

def createDataframeTable2(Data1,Schema1):
    df1 = spark.createDataFrame(data=Data1, schema=Schema1)
    return df1

# Q1A
def convertDate(createDataframeTable1,convertMiliSecondsToTimeStamp):
    df = createDataframeTable1(Data,Schema)
    df = df.rdd.map(lambda x:(x[0],x[1],convertMiliSecondsToTimeStamp(x[1]),x[2],x[3],x[4],x[5])).\
        toDF(schema = ["ProductName","IssueDate","newTimeStamp","Price","Brand","Country","ProductNumber"])
    df.show()
    return df

# Q1B Convert to date type
def converttoDateType(convertDate,timeStampToDateType):
    df =  convertDate(createDataframeTable1,convertMiliSecondsToTimeStamp)
    df = df.withColumn("newTimeStamp",df.newTimeStamp.cast(TimestampType()))
    df = df.rdd.map(lambda x:(x[0],x[1],timeStampToDateType(x[2]),x[3],x[4],x[5],x[6])).\
        toDF(schema = ["ProductName","IssueDate","DateType","Price","Brand","Country","ProductNumber"])
    df.show()

# Q1C
def rmWhiteSpaces(createDataframeTable1,rmWhiteSpaceUtils):
    df = createDataframeTable1(Data, Schema)
    df = df.rdd.map(lambda x:(x[0],x[1],x[2],rmWhiteSpaceUtils(x[3]),x[4],x[5])).\
        toDF(schema = ["ProductName","IssueDate","Price","Brand","Country","ProductNumber"])
    df.show()

# Q1D replace null values with Empty Values
def changeNUllValues(createDataframeTable1):
    df = createDataframeTable1(Data,Schema)
    df = df.select("*",when(df.Country == "","Empty Value").otherwise(df.Country).alias("nullHandlingForCountry")).drop("Country")
    df.show()

# Q2A Change the camel case columns to snake case
def convertCase(convertCaseUtils,createDataframeTable2):
    df = createDataframeTable2(Data1, Schema1)
    columns = df.columns
    updatedSchema = list(map(convertCaseUtils,columns))
    df = df.rdd.toDF(updatedSchema)
    df.show()

# Q2B Add another column as start_time_ms and convert the values of StartTime to
# milliseconds.
def convertTimeStampToMiliSeconds(createDataframeTable2,convertSecondstoTimeStamp):

    df = createDataframeTable2(Data1,Schema1)
    dfNew = df.rdd.map(lambda x: (x[0],x[1],x[2],x[3],x[4],convertSecondstoTimeStamp(x[4]),x[5])).\
        toDF(schema=["SourceID","TransactionNumber","Language","ModelNumber","unchangedTime","changedTime","ProductNumber",])
    dfNew = dfNew.withColumn("changedTime",col("changedTime").cast("long"))
    dfNew.show()

# Q3 Combine both the tables based on the Product Number and get all the fields in return.
# And get the country as EN
def combineTheTables(createDataframeTable1, createDataframeTable2):
    df1 = createDataframeTable1(Data,Schema)
    df2 = createDataframeTable2(Data1,Schema1)
    dfJoin = df1.join(df2,df1.ProductNumber == df2.ProductNumber,"inner")
    dfJoin.show()
    dfJoin = dfJoin.filter(dfJoin.Language.contains("EN")).groupBy("Country","Language").count()\
        .select("Country","Language", col("count").alias("countryFound"))
    dfJoin.show()

# combineTheTables(createDataframeTable1, createDataframeTable2)