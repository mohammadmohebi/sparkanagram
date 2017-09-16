#!/usr/bin/env python
#-*- coding:utf-8 -*-
# author : Mohammad
from pyspark import SparkContext, SparkConf

import sys
import re
import time

class AnalyseWords:

	def __init__(self, iDirectory):
		self.m_sparkContext = SparkContext(appName="IndexInverse")
		self.m_rdd = self.m_sparkContext.wholeTextFiles(iDirectory)
		
	def writeFilesNames(self, outFile):
		outFile.write("[\n\t")
		rddOrdered = self.m_rdd.sortByKey()
		rddkeys = rddOrdered.keys()
		files = rddkeys.collect()
		outFile.writelines("\n\t".join(files))
		outFile.write("\n]\n")
		
	def writeWords(self, outFile):
		self.replaceFileNameByIndex()
		self.explodeFiles()
		self.mapReduce()
		listMots = self.m_rdd.collect()
		outFile.write("{")
		for terme in listMots:
			outFile.write("\n\t{0} [".format(terme[0]))
			listIndex = terme[1]
			
			for i in listIndex:
				string = ""
				string = "\n\t\t(" + str(i[0]) + " " + str(i[1]) + ")"
				outFile.write(string)
			outFile.write("\n\t]")
		outFile.write("\n}")
		
	def replaceFileNameByIndex(self):
		rddSortedByKey = self.m_rdd.sortByKey()
		rddKeys = rddSortedByKey.keys()
		rddKeysWithIndex = rddKeys.zipWithIndex()
		rddCombinaison = rddKeysWithIndex.leftOuterJoin(rddSortedByKey)
		rddIndexWithContent = rddCombinaison.map(AnalyseWords.staticReplaceIndex)
		rddIndexSorted = rddIndexWithContent.sortByKey()
		self.m_rdd = rddIndexSorted
		
	def explodeFiles(self):
		rddExplodedFiles = self.m_rdd.flatMap(AnalyseWords.staticExplode)
		rddKeySwaping = rddExplodedFiles.map(AnalyseWords.staticSwapKey)
		self.m_rdd = rddKeySwaping
		
	def mapReduce(self):
		rddReducedResult = self.m_rdd.reduceByKey(AnalyseWords.staticReducer)
		rddByKeyOrder = rddReducedResult.sortByKey()
		self.m_rdd = rddByKeyOrder

	@staticmethod
	def staticReplaceIndex(x):
		return (x[1][0], x[1][1])
		
	@staticmethod
	def staticExplode(x):
		list = re.findall("[\w']+", x[1])
		list2 = []
		offset = 0
		
		for i in list:
			offset = x[1].find(i, offset)
			list2.append((x[0], i, offset))
			
		return list2
	
	@staticmethod
	def staticSwapKey(x):
		return (x[1], [(x[0], x[2])])
		
	@staticmethod
	def staticReducer(a, b):
		list = []
		for i in a:
			list.append(i)
		for j in b:
			list.append(j)
		return list
   
if __name__ == "__main__":
    if len(sys.argv) != 3:
        print >> sys.stderr, "Usage: inverted_index <input directory> <output file> "
        exit(-1)

    start_time = time.clock()
    directory = sys.argv[1]
    outputFilePath = sys.argv[2]
    outputFile = open(outputFilePath, "w")
    wordAnalyser = AnalyseWords(directory)
    wordAnalyser.writeFilesNames(outputFile)
    wordAnalyser.writeWords(outputFile)
    outputFile.close()
    print (time.clock() - start_time, "seconds")
