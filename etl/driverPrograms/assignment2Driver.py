from etl.utilPrograms.assignment2Util import *
print(totalLinesRdd(loadDataToRDD))
print(totalWarnRdd(loadDataToRDD))
print(totalRepoProcessed(loadDataToRDD))
mostHttp(articulatedDf)
failedRequest(articulatedDf)
activeHourOfTheDay(articulatedDf)
mostActiveRepo(articulatedDf)

# filePath = /home/kp03/sparkModule2/datasets/ghtorrent-logs.txt