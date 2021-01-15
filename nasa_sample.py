from pyspark import SparkContext, SparkConf

def toCSVLine(data):
  return ','.join(str(d) for d in data)

def isNotHeader(line: str):
    return not (line.startswith("host") and "bytes" in line)
    
if __name__ == "__main__":
    conf = SparkConf().setAppName("sample line").setMaster("local[*]")
    sc = SparkContext(conf = conf)

    nasa = sc.textFile("in/nasa_19950701.tsv")
    sample = nasa.sample(withReplacement=False, fraction=0.1, seed=99)
    sampleNoHeader = sample.filter(isNotHeader)
    sampleCSV = sampleNoHeader.map(lambda line : toCSVLine(line.split('\t')[:-2]))
    sampleCSV.saveAsTextFile("out/nasa_sample.csv")
