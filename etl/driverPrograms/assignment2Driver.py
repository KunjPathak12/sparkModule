from etl.utilPrograms.assignment2Util import *
print(totalLinesRdd(loadDataToRDD))
print(totalWarnRdd(loadDataToRDD))
print(totalRepoProcessed(loadDataToRDD))
mostHttp(articulatedDf)
failedRequest(articulatedDf)
activeHourOfTheDay(articulatedDf)
mostActiveRepo(articulatedDf)
