#For each year available, plot the size of the set of words.
#word year no_of_occurences no_of_docs

from __future__ import print_function
import sys
from pyspark import SparkContext

if __name__ == "__main__":
  if len(sys.argv) < 2:
    print("enter a filename")
    sys.exit(1)
   
  sc = SparkContext(appName = "set_of_words")
  
  year_word = sc.textFile(sys.argv[1],)
  list_results=year_word.flatMap(lambda line:[(line.split('\t')[1],1)])
  count = list_results.reduceByKey(lambda x,y:x+y).collect()
  output_set = sorted(count, key=lambda k: k[0])
  with open("output.txt", 'w') as f:
        for line in output_set:
            f.write(line[0] + '\t' + str(line[1]) + '\n')
  
  sc.stop()
