"""
- Parse logs and forms a distribution of visits from different cities belonging to different IP blocks
- Updates corresponding data in database
"""

import csv
import os
import re
import json
import MySQLdb
import operator
from time import mktime
from datetime import datetime
import gzip
import glob
from ConfigParser import RawConfigParser

class Parser:

	def __init__(self):
		self.config = RawConfigParser()
		self.config.read("config")
		self.baseDir = self.config.get("paths", "base_directory")
		self.citiesDict = {}
		self.db = MySQLdb.connect(self.config.get("database", "server"), self.config.get("database", "username"), self.config.get("database", "password"), self.config.get("database", "dbname"))
		cursor = self.db.cursor(MySQLdb.cursors.DictCursor)
		cursor.execute("SELECT timestamp FROM latest_timestamp LIMIT 1")
		self.latestTimestamp = cursor.fetchone()['timestamp']
		cursor.close()
		self.newTimestamp = self.latestTimestamp
		self.batchSize = 300

	def loadCities(self):
		with open(os.path.join(self.baseDir, self.config.get("paths", "relative_citiesCSV"))) as f:
			creader = csv.reader(f)
			for row in creader:
				self.citiesDict[row[2][:-1]] = row[0]

	def parseLog(self, fname):
		DateRegEx = re.compile(r'\[(\d{1,2}\/[a-zA-Z]{3}\/\d{4}:\d{2}:\d{2}:\d{2})\s[+-]\d{4}\]')
		IPRegEx = re.compile(r'\s((\d{0,3}\.){3}\d{0,3})\s')
		URLRegEx = re.compile(r'(GET|POST)\s/(.*?)/')
		tc = 0
		vc = 0
		self.dist = {}
		previousDate = ""
		f = gzip.open(fname, "r")
		for line in f:
			tc += 1
			# if tc > 10000:
			# 	break
			try:
				ip = IPRegEx.search(line).group(0).strip() # get IP
				url = URLRegEx.search(line).group(2).strip() # get URL
				dt = DateRegEx.search(line).group(1).strip() # get date
			except Exception as e:
				continue
			ipBlock = ip[:ip.rfind(".")] # get first 3 bytes
			if ipBlock == "127.0.0":
				continue
			if url in self.citiesDict:
				self.newTimestamp = int(mktime(datetime.strptime(dt, "%d/%b/%Y:%H:%M:%S").timetuple()))

				if self.newTimestamp <= self.latestTimestamp:
					continue

				# print str(vc) + ". " + ipBlock + " ==> " + url + " (" + self.citiesDict[url] +")"
				vc += 1
				city = self.citiesDict[url]
				if ipBlock not in self.dist:
					self.dist[ipBlock] = {}
				if city not in self.dist[ipBlock]:
					self.dist[ipBlock][city] = 1
				else:
					self.dist[ipBlock][city] += 1
		f.close()

	def updateDB(self):
		insertData = []
		updateData = []
		count = 0
		cursor = self.db.cursor(MySQLdb.cursors.DictCursor)

		for ipBlock in self.dist:
			cursor.execute("SELECT * FROM geoip_mapping WHERE ip_block='%s'" % ipBlock)
			if cursor.rowcount == 0:
				topCities = self.getTopCities(self.dist[ipBlock], 3)
				insertData.append((ipBlock, topCities[0], topCities[1], topCities[2], json.dumps(self.dist[ipBlock])))
			else:
				rows = cursor.fetchall()
				row = rows[0]
				newJSON = self.mergeJSON(json.loads(row['distribution_json']), self.dist[ipBlock])
				topCities = self.getTopCities(newJSON, 3)
				updateData.append((topCities[0], topCities[1], topCities[2], json.dumps(newJSON), ipBlock))
			count += 1
			if count > self.batchSize:
				# Execute in batches
				self.executeBatch(insertData, updateData)
				count = 0
				insertData = []
				updateData = []

		# Execute residual batch (last batch < threshold)
		if count > 0: 
			self.executeBatch(insertData, updateData)

		if self.newTimestamp is not None:
			# print "updating timestamp .. " + str(self.newTimestamp)
			cursor.execute("UPDATE latest_timestamp SET timestamp=%s", (self.newTimestamp,))
			self.db.commit()
		cursor.close()

	def executeBatch(self, insertData, updateData):
		if len(insertData) == 0 and len(updateData) == 0:
			return
		# print "Executing batch.."
		cursor = self.db.cursor(MySQLdb.cursors.DictCursor)
		insertQuery = "INSERT INTO geoip_mapping(ip_block, city_1, city_2, city_3, distribution_json) VALUES(%s, %s, %s, %s, %s)"
		cursor.executemany(insertQuery, insertData)
		updateQuery = "UPDATE geoip_mapping SET city_1=%s, city_2=%s, city_3=%s, distribution_json=%s WHERE ip_block=%s"
		cursor.executemany(updateQuery, updateData)
		self.db.commit()
		cursor.close()

	def mergeJSON(self, existingJSON, newJSON):
		returnJSON = existingJSON
		for city in newJSON:
			if city in existingJSON:
				returnJSON[city] += newJSON[city]
			else:
				returnJSON[city] = newJSON[city]
		return returnJSON

	def getTopCities(self, d, n):
		topCities = sorted(d.items(), key=operator.itemgetter(1))[(-1*n):]
		topCitiesKeys = [int(x[0]) for x in topCities]
		topCitiesKeys.reverse()
		i = len(topCitiesKeys)
		while i < n:
			topCitiesKeys.append(None)
			i += 1
		return topCitiesKeys

	def processAllLogs(self):
		files = filter(os.path.isfile, glob.glob(self.baseDir + self.config.get("paths", "relative_configFilesFormat")));
		files.sort(key=lambda x: os.path.getmtime(x))
		for f in files:
			print "Parsing log " + f
			self.parseLog(f)
			print "Updating DB"
			self.updateDB()
		self.db.close()

if __name__ == "__main__":
	p = Parser()
	print "Loading cities"
	p.loadCities()
	p.processAllLogs()