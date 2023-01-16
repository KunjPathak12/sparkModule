from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
import warnings
warnings.simplefilter("ignore")
spark = SparkSession.builder.appName("assignment-1").config("spark.some.config.option","some-value").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

filePathT = "/home/kp03/sparkModule2/datasets/transaction.csv"
filePathU = "/home/kp03/sparkModule2/datasets/user.csv"


def makeDfTransaction(filePathT):
    dfA1T = spark.read.option("header", "true").option("mode", "FAILFAST").csv(filePathT, inferSchema=True)
    return dfA1T

def makeDfUser(filePathU):
    dfA1U = spark.read.option("header", "true").option("mode", "FAILFAST").csv(filePathU, inferSchema=True)
    return dfA1U

def joinColumn(makeDfTransaction,makeDfUser):
    dfA1T = makeDfTransaction(filePathT)
    dfA1U = makeDfUser(filePathU)
    dfJoin = dfA1T.join(dfA1U, dfA1T.userid == dfA1U.user_id, "inner")
    return  dfJoin
def uniqueLocation(joinColumn):
    # Answer1
    dfJoin = joinColumn(makeDfTransaction,makeDfUser)
    dfJoin = dfJoin.withColumnRenamed("location ", "location")
    dfJoin = dfJoin.withColumnRenamed("product_description", "pdesc")
    dfunique = dfJoin.dropDuplicates(["location"])
    n = dfunique.count()
    return ("Answer 1\nThe count of unique locations where each product is sold is: {}\n".format(n))

def productByEachUser(joinColumn):
    # Answer2
    dfJoin = joinColumn(makeDfTransaction, makeDfUser)
    dfJoin = dfJoin.withColumnRenamed("location ", "location")
    dfJoin = dfJoin.withColumnRenamed("product_description", "pdesc")
    print("Answer 2\nThe products bought by each user are as follows:\n")
    dfJoin = dfJoin.groupBy("userid", "pdesc").count().sort("userid")
    dfJoin.show()
    return dfJoin

def spendingByUSer(joinColumn):
    # Answer3
    dfJoin = joinColumn(makeDfTransaction, makeDfUser)
    dfJoin = dfJoin.withColumnRenamed("location ", "location")
    dfJoin = dfJoin.withColumnRenamed("product_description", "pdesc")
    print("Answer 3a\nThe spending done by each user on each product:\n")
    dfJoin = dfJoin.groupBy("userid","pdesc").sum("price").sort("userid")
    dfJoin.show()
    return dfJoin

def totalSpendingByUser(joinColumn):
    # Total Spending
    dfJoin = joinColumn(makeDfTransaction, makeDfUser)
    dfJoin = dfJoin.withColumnRenamed("location ", "location")
    dfJoin = dfJoin.withColumnRenamed("product_description", "pdesc")
    print("Answer 3b\nTotal spending done by each user:\n")
    dfJoin = dfJoin.groupBy("userid").sum("price").sort("userid").alias("totalSpent")
    dfJoin.show()
    return dfJoin
