# Questionnaire
# -> By making use of Spark Core (i.e., without using Spark SQL) find out:
# 1) Count of unique locations where each product is sold.
# 2) Find out products bought by each user.
# 3) Total spending done by each user on each product.

from pyspark.sql import *
import warnings
warnings.simplefilter("ignore")
spark = SparkSession.builder.appName("Pycharm's Spark").config("spark.some.config.option","some-value")\
        .getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("OFF")
# --> Assignment 1---
dfA1T = spark.read.option("header","true").option("mode","FAILFAST").csv("/home/kp03/Downloads/transaction.csv", inferSchema = True)
dfA1U = spark.read.option("header","true").option("mode","FAILFAST").csv("/home/kp03/Downloads/user.csv", inferSchema = True)
# dfA1T.show()
# dfA1U.show()

#Answer1
dfJoin = dfA1T.join(dfA1U,dfA1T.userid==dfA1U.user_id,"inner")
dfJoin = dfJoin.withColumnRenamed("location ", "location")
dfJoin = dfJoin.withColumnRenamed("product_description", "pdesc")
dfunique=dfJoin.dropDuplicates(["location"])
# dfunique.show()
n = dfunique.count()
print("Answer 1\nThe count of unique locations where each product is sold is: {}\n".format(n))
# Answer2
print("Answer 2\nThe products bought by each user are as follows:\n")
dfJoin.groupBy("userid","pdesc").count().sort("userid").show()
# Answer3
print("Answer 3a\nThe spending done by each user on each product:\n")
dfJoin.groupBy("userid","pdesc").sum("price").sort("userid").show()
# Total Spending
print("Answer 3b\nTotal spending done by each user:\n")
dfJoin.groupBy("userid").sum("price").sort("userid").alias("totalSpent").show()