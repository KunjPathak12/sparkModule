from etl.utilPrograms.assignment1Util import *
import unittest


class Test(unittest.TestCase):

    # TestCase for the first Answer's Function
    def testUniqueLocation(self):
        self.assertTrue(uniqueLocation(joinColumn), 3)

    # TestCase for the second Answer's Function
    def testProductByEachUser(self):
        def compareDF(spark):
            schema = StructType([StructField("userid", IntegerType(), True), \
                                 StructField("pdesc", StringType(), True), \
                                 StructField("count", LongType(), False)])
            data = [(101, "speaker", 1), \
                    (101, "mouse", 1), \
                    (101, "firdge", 1), \
                    (102, "chair", 1), \
                    (102, "keyboard", 1), \
                    (103, "tv", 1), \
                    (105, "sofa", 1), \
                    (105, "laptop", 1), \
                    (105, "bed", 1), \
                    (105, "phone", 1)]
            df = spark.createDataFrame(data=data, schema=schema)
            return df

        self.assertTrue(productByEachUser(joinColumn), compareDF(spark))

    # TestCase for the third Answer's Function
    def testSpendingByUSer(self):
        def checkDF(spark):
            schema = StructType([StructField("userid", IntegerType(), True), \
                                 StructField("pdesc", StringType(), True), \
                                 StructField("sum(price)", LongType(), False)])

            data = [(101, "speaker", 500), \
                    (101, "mouse", 700), \
                    (101, "firdge", 35000), \
                    (102, "chair", 1000), \
                    (102, "keyboard", 900), \
                    (103, "tv", 34000), \
                    (105, "sofa", 55000), \
                    (105, "laptop", 60000), \
                    (105, "bed", 100), \
                    (105, "phone", 20000)]
            df = spark.createDataFrame(data=data, schema=schema)
            return df

        self.assertTrue(checkDF(spark), spendingByUSer(joinColumn))


    # TestCase for the third Answer's "b" Function
    def testTotalSpendingByUSer(self):
        def checkDF(spark):
            schema = StructType([StructField("userid", IntegerType(), True), \
                                 StructField("sum(price)", LongType(), False)])

            data = [(101, 36200), \
                    (102, 1900), \
                    (103, 34000), \
                    (105, 121000), \
                    (106, 100), \
                    (108, 20000)]
            df = spark.createDataFrame(data=data, schema=schema)
            return df

        self.assertTrue(checkDF(spark), totalSpendingByUser(joinColumn))


if __name__ == "main":
    unittest.main()
