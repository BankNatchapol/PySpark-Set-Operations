from pyspark import SparkContext, SparkConf

def isNotHeader(line: str):
    return not (line.startswith("host") and "bytes" in line)
    
if __name__ == "__main__":
    conf = SparkConf().setAppName("sameHosts").setMaster("local[*]")
    sc = SparkContext(conf = conf)

    julyFirstLogs = sc.textFile("in/nasa_19950701.tsv")
    augustFirstLogs = sc.textFile("in/nasa_19950801.tsv")
    
    union_nasa = julyFirstLogs.union(augustFirstLogs)
    
    union_no_header = union_nasa.filter(isNotHeader)
    union_csv = union_no_header.map(lambda line : line.split("\t")[0]).distinct()
    union_csv.saveAsTextFile("out/nasa_distinct_host.csv")
