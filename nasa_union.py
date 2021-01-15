from pyspark import SparkContext, SparkConf

def toCSVLine(data):
  return ','.join(str(d) for d in data)

def isNotHeader(line: str):
    return not (line.startswith("host") and "bytes" in line)
    
if __name__ == "__main__":
    conf = SparkConf().setAppName("sameHosts").setMaster("local[*]")
    sc = SparkContext(conf = conf)

    julyFirstLogs = sc.textFile("in/nasa_19950701.tsv")
    augustFirstLogs = sc.textFile("in/nasa_19950801.tsv")
    
    union_nasa = julyFirstLogs.union(augustFirstLogs)
    
    union_no_header = union_nasa.filter(isNotHeader)
    union_csv = union_no_header.map(lambda line : toCSVLine(line.split("\t")))
    union_csv.saveAsTextFile("out/nasa_union_line.csv")
