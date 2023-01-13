from etl.utilPrograms.assignment2Util import *
import unittest
fileapth = "/home/kp03/sparkModule2/datasets/ghtorrent-logs.txt"
class test(unittest.TestCase):

    def testCountRdd(self):
        self.assertTrue(totalLinesRdd(loadDataToRDD),281234)
    def testWarnings(self):
        self.assert_(totalWarnRdd(loadDataToRDD),3811)
    def testTotalRepoProcessed(self):
        self.assert_(totalRepoProcessed(loadDataToRDD), 37596)
    def testMaxHttpRequest(self):
        data = [("ghtorrent-13",2515)]
        schema =StructType([StructField("id",StringType(),True),\
                            StructField("maxHttpRequest",LongType(),True)])
        myDf = spark.createDataFrame(data=data,schema=schema)
        self.assert_(mostHttp(articulatedDf),myDf)
    def testMaxFailedRequests(self):
        data=[("ghtorrent-13",2321)]
        schema = StructType([StructField("id", StringType(), True),\
                             StructField("maxHttpRequest", LongType(), True)])
        myDf = spark.createDataFrame(data=data, schema=schema)
        self.assert_(mostHttp(articulatedDf), myDf)
    def testmostRepeatedDate(self):
        data = [("mostRepeatedDate", 30963)]
        schema = StructType([StructField("id", StringType(), True),\
                             StructField("maxHttpRequest", LongType(), True)])
        myDf = spark.createDataFrame(data=data, schema=schema)
        self.assert_(mostHttp(articulatedDf), myDf)

    def testmostRepeatedDate(self):
        data = [("mostRepeatedDate", 30963)]
        schema = StructType([StructField("id", StringType(), True),\
                             StructField("maxHttpRequest", LongType(), True)])
        myDf = spark.createDataFrame(data=data, schema=schema)
        self.assert_(mostHttp(articulatedDf), myDf)
    def testMostActiveRepo(self):
        data = [("mostActiveRepo", 657)]
        schema = StructType([StructField("id", StringType(), True),\
                             StructField("maxHttpRequest", LongType(), True)])
        myDf = spark.createDataFrame(data=data, schema=schema)
        self.assert_(mostHttp(articulatedDf), myDf)

if __name__ == '__main__':
    unittest.main()