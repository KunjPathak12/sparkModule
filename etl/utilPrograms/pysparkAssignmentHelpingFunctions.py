import datetime
def convertMiliSecondsToTimeStamp(seconds):
    timeStamp = datetime.datetime.utcfromtimestamp(int(seconds)//1000).strftime("%Y-%m-%dT%H:%M:%S.%f%z")
    return timeStamp
# This is just a helping function for applying the asked transform for Q1A
def timeStampToDateType(string):
    timeStamp = datetime.datetime.strftime(string,"%Y-%m-%d")
    return timeStamp
# This os just a helping function for answer 1B
def rmWhiteSpaceUtils(string):
    return string.strip()
# This is just a helping function for the answer 1C
def convertCaseUtils(string):
    Alphabets = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    res = string[0].lower()
    for i in string[1:]:
        if i in Alphabets:
            res+="_"
            res+=i.lower()
        else:
            res+=i
    return res
# This is just a helping function for the answer 2A
def convertSecondstoTimeStamp(s):
    t = datetime.datetime.strptime(s, "%Y-%m-%dT%H:%M:%S.%f%z").replace(tzinfo=datetime.timezone.utc)
    t = t.timestamp()
    return t*1000
# This is just a helping function for the answer 2B