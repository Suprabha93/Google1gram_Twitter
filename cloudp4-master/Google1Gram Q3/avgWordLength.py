#Google 1 gram data-question 3
#Plot the average word length for all unique words for all years available.  Year on x-axis, average word-length on y-axis.

#1 gram data format
#word year noofoccurences noofdocshaving the word

from __future__ import print_function
from __future__ import division
import sys
from pyspark import SparkContext

#determining the length of each word in a line
#<key,value> pair is the <year,word length> for each line in 1gram data
def get_WLength(line):
  l = line.strip().lower().split()
  year = int(l[1])
  wordLength = len(l[0])
  return (year , wordLength)

#calculating total no of words occuring in each year
#<key-value> pair is the <year,no of occurences> for each line in 1gram data
def get_Occurences(line):
  l = line.strip().lower().split()
  year = int(l[1])
  occurence = int(l[2])
  return (year , occurence)
  
if __name__ == "__main__":
  if len(sys.argv) < 2:
    print("enter a filename")
    sys.exit(1)
   
  sc = SparkContext(appName = "avgWordLength")
  
  oneGramData = sc.textFile(sys.argv[1],)
 
  #RDD storing the pairs(year,word length)
  wordLength = oneGramData.map(get_WLength)
  
  #sum of word lengths for each year
  wordLengthList=wordLength.reduceByKey(lambda x,y: x+y).collect()
  
  #RDD storing the pairs (year,no. of occurences)
  wordCount = oneGramData.map(get_Occurences)
  
  #total no of words for each year
  wordCountList = wordCount.reduceByKey(lambda x,y: x+y).collect()
  
  #storing as key-value pairs in dict
  #key=year
  #value=no of words in each year
  wordCountList=dict(wordCountList)
  
  #to store avg word length for each year
  avgLength=set()
  
  #calculating the avg length of each word in a year
  #dividing sum of all word lengths in a year by no. of words in a year
  for(year,word_length) in wordLengthList:
    avgLength.add((year,float(word_length)/wordCountList[year]))
  
  sc.stop()
  
  with open("q3result_cluster",'w') as f:
    for element in avgLength:
      f.write(str(element[0]) + '\t' + str(element[1]) + '\n')
