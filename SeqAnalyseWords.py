#!/usr/bin/env python
#-*- coding:utf-8 -*-
# author : Mohammad

import sys
import re
import os
import collections
import time

class SeqAnalyseWords:
    def __init__(self, iDirectory):
        self.directory = iDirectory
        self.files_list = []
        self.words_dictionary = {}

    def writeFilesNames(self, outFile):
        outFile.write("[\n\t")
        outFile.writelines("\n\t".join(self.files_list))
        outFile.write("\n]\n")

    def writeWords(self, outFile):
        words_dictionary_sorted = collections.OrderedDict(sorted(self.words_dictionary.items()))
        outFile.write("{")
        for word, value in words_dictionary_sorted.iteritems():
            outFile.write("\n\t{0} [".format(word))
            for file_index, offset in value:
                outFile.write(("\n\t\t({0} {1})".format(str(file_index), str(offset))))
            outFile.write("\n\t]")
        outFile.write("\n}")

    def getAllWordsFromFiles(self):
        for root, folder_names, files in os.walk(self.directory):
            for index, filename in enumerate(files):
                file_path = os.path.join(root, filename)
                self.files_list.append(file_path)
                with open(file_path, 'r') as myfile:
                    data=myfile.read()
                    explodedFiles = self.staticExplode(index, data)
                    for word, value in explodedFiles.iteritems():
                        self.words_dictionary.setdefault(word, []).extend(value)

    def staticExplode(self, file_index, file_data):
        list = re.findall("[\w']+", file_data)
        dict = {}
        offset = 0
        for word in list:
            offset = file_data.find(word, offset)
            dict.setdefault(word,[]).append((file_index, offset))

        return dict

	
if __name__ == "__main__":
    if len(sys.argv) != 3:
        print >> sys.stderr, "Usage: InverseIndexSequential <input directory> <output file> "
        exit(-1)

    start_time = time.clock()
    directory = sys.argv[1]
    outputFilePath = sys.argv[2]
    outputFile = open(outputFilePath, "w")
    wordAnalyser = SeqAnalyseWords(directory)
    wordAnalyser.getAllWordsFromFiles()
    wordAnalyser.writeFilesNames(outputFile)
    wordAnalyser.writeWords(outputFile)
    outputFile.close()
    print (time.clock() - start_time, "seconds")

