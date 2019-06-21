#! /usr/bin/env python
from multiprocessing import Pool
import os,sys
import time
import glob
import shutil
import logging
import ConfigParser

configFile = "CascadeHammer.conf"
config = ConfigParser.ConfigParser()
config.read(configFile)
poolSize = config.get('CascadeHammer','PoolSize')
startPath = config.get('CascadeHammer','StartPath')
outFolder = config.get('CascadeHammer','OutputFolder')

logging.basicConfig(filename="CascadeHammer.log", level=logging.DEBUG, datefmt='%m/%d/%y %I:%M:%S %p', 
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(name='CascadeHammer')

fileDict = "files.txt"
folderDict = "folders.txt"
files = open(fileDict, "a")
outPath = ""


def Gatherer(dire):
	global outPath
	fo = []
	print "Directory: " + str(dire).strip()
	try:
		fileStem = os.getpid()
		fileName = os.path.join(outPath, "file" + str(fileStem) + ".txt")
		try:
			a = os.listdir(dire.strip())
			for f in a:
				fq = os.path.join(dire.strip(), f)
				if os.path.isdir(fq):
					fo.append(fq)
				else:
					files = open(fileName, "a")
					files.write(fq + "\n")
		except:
			pass
		try:
			files.close()
		except:
			pass
		return fo
	except Exception, ex:
		logger.error("Error scanning directory %s: %s" % (dirPath, ex))
		return fo

def Iterator(startPath):
	global outPath
	global files
	global folderDict
	global poolSize
	
	subset = 0
	end = int(poolSize)		
	folders = open(folderDict, "w")
	if len(startPath)>0:
		totalDirs = len(startPath)
		p = Pool(int(poolSize))
		while subset < totalDirs:
			section = startPath[subset:end]
			fo = p.map(Gatherer, section)
			subset = end
			end = end + int(poolSize)
			try:
				files.close()
			except:
				pass
			for litem in fo:
				if len(litem) > 0:
					for i in litem:
						folders.write(i + "\n")
	return

def Processor():
	global folderDict
	global outPath
	folders = open(folderDict, "r")
	startPath = folders.readlines()
	folders.close()
	folderLen = int(len(startPath))
	while folderLen > 0:
		Iterator(startPath)
		folders = open(folderDict, "r")
		startPath = folders.readlines()
		folders.close()
		folderLen = int(len(startPath))
	else:
		print "No folders to scan. Processing collected results."
		tempFiles = os.listdir(outPath)
		for t in tempFiles:
			with open(os.path.join(outPath, t)) as infile:
				files = open(fileDict, "a")
				files.write(infile.read())
				os.remove(os.path.join(outPath, t))
				files.close()
		shutil.move(fileDict, outPath)
		logger.info("Completing cascade at " + str(time.strftime("%d %b %Y, %H:%M:%S")))
		os.remove(folderDict)
		sys.exit()

def main():
	global fileDict
	global folderDict
	global outPath
	global startPath
	global outFolder
	global files

	logger.info("Starting cascade at " + str(time.strftime("%d %b %Y, %H:%M:%S")))
	outPath = os.path.join(os.getcwd(), "FILELIST_" + str(outFolder) + "_" + str(time.strftime("%Y_%m_%d")))
	if not os.path.exists(outPath):
		os.makedirs(outPath)
	if startPath == "":
		Processor()
	else:
		folders = open(folderDict, "w")
		dirContent = glob.glob(startPath + "/*")
		for d in dirContent:
			if os.path.isdir(d):
				folders.write(d + "\n")
			else:
				files.write(d + "\n")
		try:
			folders.close()
		except:
			pass
		Processor()

if __name__ == "__main__":
	main()
			
		
