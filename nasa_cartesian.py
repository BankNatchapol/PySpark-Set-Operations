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

    method = union_no_header.map(lambda line : line.split("\t")[3]).distinct()
    response = union_no_header.map(lambda line : line.split("\t")[5]).distinct()
    
    cartesian = method.cartesian(response)
    
    cartesianCSV = cartesian.map(toCSVLine)
    
    cartesianCSV.saveAsTextFile("out/nasa_cartesian_method_response.csv")
