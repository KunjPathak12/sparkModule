from etl.utilPrograms.pysparkAssignmentUtil import *
import unittest

class MyTestCase(unittest.TestCase):
    def testConvertDate(self):
        def checkDF(spark):
            schema = StructType([StructField('ProductName', StringType(), True),\
                                 StructField('IssueDate', StringType(), True), \
                                 StructField('newTimeStamp', StringType(), True),\
                                 StructField('Price', LongType(), True),\
                                 StructField('Brand', StringType(), True),\
                                 StructField('Country', StringType(), True),\
                                 StructField('ProductNumber', StringType(), True)])
            data = [("Washing Machine","1648770933000","2022-03-31T23:55:33.000000",20000,"Samsung","India","0001"), \
                    ("Refrigerator","1648770999000","2022-03-31T23:56:39.000000",35000," LG ","","0002"), \
                    ("Air Cooler","1648770948000","2022-03-31T23:55:48.000000",45000," Voltas ","","0003")]

            df = spark.createDataFrame(schema=schema, data=data)
            return df

        self.assertEqual(convertDate(createDataframeTable1,convertMiliSecondsToTimeStamp).collect(),checkDF(spark).collect())

    def testConvertToDataType(self):
        def checkDF(spark):
            schema = StructType([StructField('ProductName', StringType(), True), \
                                 StructField('IssueDate', StringType(), True), \
                                 StructField('DateType', StringType(), True), \
                                 StructField('Price', LongType(), True), \
                                 StructField('Brand', StringType(), True), \
                                 StructField('Country', StringType(), True), \
                                 StructField('ProductNumber', StringType(), True)])
            data = [("Washing Machine", "1648770933000", "2022-03-31", 20000, "Samsung", "India", "0001"), \
                ("Refrigerator", "1648770999000", "2022-03-31", 35000, " LG ", "", "0002"), \
                ("Air Cooler", "1648770948000", "2022-03-31", 45000, " Voltas ", "", "0003")]

            df = spark.createDataFrame(schema=schema, data=data)
            return df

        self.assertEqual(converttoDateType(convertDate,timeStampToDateType).collect(),checkDF(spark).collect())

    def testRmwhiteSpace(self):
        def checkDF(spark):
            schema = StructType([StructField('ProductName', StringType(), True), \
                                 StructField('IssueDate', StringType(), True), \
                                 StructField('Price', LongType(), True), \
                                 StructField('Brand', StringType(), True), \
                                 StructField('Country', StringType(), True), \
                                 StructField('ProductNumber', StringType(), True)])
            data = [("Washing Machine", "1648770933000", 20000, "Samsung", "India", "0001"), \
                ("Refrigerator", "1648770999000", 35000, "LG", "", "0002"), \
                ("Air Cooler", "1648770948000", 45000, "Voltas", "", "0003")]

            df = spark.createDataFrame(schema=schema, data=data)
            return df

        self.assertEqual(rmWhiteSpaces(createDataframeTable1,rmWhiteSpaceUtils).collect(),checkDF(spark).collect())
    def testChangeNullValues(self):
        def checkDF(spark):
            schema = StructType([StructField('ProductName', StringType(), True), \
                                 StructField('IssueDate', StringType(), True), \
                                 StructField('Price', LongType(), True), \
                                 StructField('Brand', StringType(), True), \
                                 StructField('ProductNumber', StringType(), True),\
                                 StructField('nullHandingForCountry', StringType(), True)])
            data = [("Washing Machine", "1648770933000", 20000, "Samsung", "0001", "India"), \
                ("Refrigerator", "1648770999000", 35000, " LG ", "0002", "Empty Value"), \
                ("Air Cooler", "1648770948000", 45000, " Voltas ",  "0003", "Empty Value")]

            df = spark.createDataFrame(schema=schema, data=data)
            return df

        self.assertEqual(changeNUllValues(createDataframeTable1).collect(),checkDF(spark).collect())

    def testConvertCase(self):
        def checkDF(saprk):
            schema = StructType([StructField("source_id",LongType(),True),\
                     StructField("transaction_number",LongType(),True),\
                     StructField("language",StringType(),True),\
                     StructField("model_number",LongType(),True),\
                     StructField("start_time",StringType(),True),\
                     StructField("product_number",StringType(),False)
                     ])
            data =  [(150711,123456,"EN",456789,"2021-12-27T08:20:29.842+0000","0001"),\
                    (150439,234567,"UK",345678,"2021-12-27T08:21:14.645+0000","0002"),\
                    (150647,345678,"ES",234567,"2021-12-27T08:22:42.445+0000","0003")]
            df = spark.createDataFrame(schema=schema, data=data)
            return df

        self.assertEqual(convertCase(convertCaseUtils,createDataframeTable2).collect(), checkDF(spark).collect())

    def testConvertTimeStampToMiliSeconds(self):
        def checkDF(saprk):
            schema = StructType([StructField('SourceID', LongType(), True),\
                                 StructField('TransactionNumber', LongType(), True),\
                                 StructField('Language', StringType(), True),\
                                 StructField('ModelNumber', LongType(), True),\
                                 StructField('unchangedTime', StringType(), True),\
                                 StructField('changedTime', LongType(), True),\
                                 StructField('ProductNumber', StringType(), True)])
            data = [(150711,123456,"EN",456789,"2021-12-27T08:20:29.842+0000",1640593229842,"0001"),\
                    (150439,234567,"UK",345678,"2021-12-27T08:21:14.645+0000",1640593274645,"0002"),\
                    (150647,345678,"ES",234567,"2021-12-27T08:22:42.445+0000",1640593362445,"0003")]

            df = spark.createDataFrame(schema=schema, data=data)
            return df

        self.assertEqual(convertTimeStampToMiliSeconds(createDataframeTable2,convertSecondstoTimeStamp).collect(), checkDF(spark).collect())

    def testCombineTheTables(self):
        def checkDF(spark):
            schema = StructType([StructField('ProductName', StringType(), True), \
                                 StructField('IssueDate', StringType(), True), \
                                 StructField('Price', LongType(), True), \
                                 StructField('Brand', StringType(), True), \
                                 StructField('Country', StringType(), True), \
                                 StructField('ProductNumber', StringType(), True), \
                                 StructField('SourceID', LongType(), True), \
                                 StructField('TransactionNumber', LongType(), True), \
                                 StructField('Language', StringType(), True), \
                                 StructField('ModelNumber', LongType(), True), \
                                 StructField('unchangedTime', StringType(), True), \
                                 StructField('ProductNumber', StringType(), True)])
            data = [("Washing Machine", "1648770933000", 20000, "Samsung", "India", "0001", 150711, 123456, "EN", 456789, "2021-12-27T08:20:29.842+0000", "0001"), \
                ("Refrigerator", "1648770999000", 35000, " LG ", "", "0002", 150439, 234567, "UK", 345678, "2021-12-27T08:21:14.645+0000", "0002"), \
                ("Air Cooler", "1648770948000", 45000, " Voltas ", "", "0003", 150647, 345678, "ES", 234567, "2021-12-27T08:22:42.445+0000","0003")]

            df = spark.createDataFrame(schema=schema, data=data)
            return df

        self.assertEqual(combineTheTables(createDataframeTable1,createDataframeTable2).collect(),checkDF(spark).collect())

    def testGetCountrybyValue(self):
        def checkDF(spark):
            schema=  StructType([StructField("Country",StringType(),True),\
                                 StructField("Language",StringType(),True),\
                                 StructField("countryFound",LongType(),False)])
            data = [("India","EN",1)]
            df = spark.createDataFrame(schema=schema, data=data)
            return df

        self.assertEqual(getCountrybyValue(combineTheTables).collect(),checkDF(spark).collect())

if __name__ == "main":
    unittest.main()
