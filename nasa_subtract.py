from pyspark import SparkContext, SparkConf

def isNotHeader(line: str):
    return not (line.startswith("host") and "bytes" in line)
    
if __name__ == "__main__":
    conf = SparkConf().setAppName("sameHosts").setMaster("local[1]")
    sc = SparkContext(conf = conf)

    julyFirstLogs = sc.textFile("in/nasa_19950701.tsv")
    augustFirstLogs = sc.textFile("in/nasa_19950801.tsv")
    
    julyFirstUrls = julyFirstLogs.map(lambda line: line.split("\t")[4])
    augustFirstUrls = augustFirstLogs.map(lambda line: line.split("\t")[4])
    
    subtract = augustFirstUrls.subtract(julyFirstUrls)
    
    newNasaUrl = subtract.filter(lambda url: url != "url").distinct()

    newNasaUrl.saveAsTextFile("out/nasa_subtract_new_url.csv")
