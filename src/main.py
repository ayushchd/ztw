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

class Parser:

	def __init__(self):
		self.baseDir = "/Users/zomato/tw/"
		self.citiesDict = {}

	def loadCities(self):
		with open(os.path.join(self.baseDir, "cities.csv")) as f:
			creader = csv.reader(f)
			for row in creader:
				self.citiesDict[row[2][:-1]] = row[0]

	def parseLog(self, fname):
		with open(os.path.join(self.baseDir, fname)) as f:
			logs = f.readlines()

		IPRegEx = re.compile(r'\s((\d{0,3}\.){3}\d{0,3})\s')
		URLRegEx = re.compile(r'(GET|POST)\s/(.*?)/')
		tc = 0
		vc = 0
		self.dist = {}
		for line in logs:
			try:
				ip = IPRegEx.search(line).group(0).strip() # get IP
				url = URLRegEx.search(line).group(2).strip() # get URL
			except Exception:
				continue
			ipBlock = ip[:ip.rfind(".")] # get first 3 bytes
			if url in self.citiesDict:
				print str(vc) + ". " + ipBlock + " ==> " + url + " (" + self.citiesDict[url] + ")"
				vc += 1
				city = self.citiesDict[url]
				if ipBlock not in self.dist:
					self.dist[ipBlock] = {}

				if city not in self.dist[ipBlock]:
					self.dist[ipBlock][city] = 1
				else:
					self.dist[ipBlock][city] += 1
			tc += 1

		print tc
		# print json.dumps(dist)

	def updateDB(self):
		self.db = MySQLdb.connect("localhost", "zomato", "zomato", "zomato_geoip")
		self.cursor = self.db.cursor(MySQLdb.cursors.DictCursor)
		for ipBlock in self.dist:
			
			# Get an exclusive lock on the row
			self.cursor.execute("SELECT * FROM geoip_mapping WHERE ip_block=%s FOR UPDATE", (ipBlock,))
			doUpdate = 1
			keyerror = 0
			if self.cursor.rowcount == 0:
				topCities = self.getTopCities(self.dist[ipBlock], 3)
				try:
					doUpdate = 0
					# Insert record
					self.cursor.execute("INSERT INTO geoip_mapping(ip_block, city_1, city_2, city_3, distribution_json) VALUES(%s, %s, %s, %s, %s)", (ipBlock, topCities[0], topCities[1], topCities[2], json.dumps(self.dist[ipBlock])))
					self.db.commit()
				except Exception:
					# The most likely exception to be raised is if some other instance inserts the same IP in the meanwhile, so we rollback
					self.db.rollback()
					doUpdate = 1
					keyerror = 1
			if doUpdate:
				# keyerror=1 only if a duplicate was inserted by some other instance. so we get the latest record
				if keyerror: 
					self.cursor.execute("SELECT * FROM geoip_mapping WHERE ip_block=%s FOR UPDATE", (ipBlock,))
				rows = self.cursor.fetchall()
				row = rows[0]
				newJSON = self.mergeJSON(json.loads(row['distribution_json']), self.dist[ipBlock])
				topCities = self.getTopCities(newJSON, 3)
				self.cursor.execute("UPDATE geoip_mapping SET city_1=%s, city_2=%s, city_3=%s, distribution_json=%s WHERE ip_block=%s", (topCities[0], topCities[1], topCities[2], json.dumps(newJSON), ipBlock))
				self.db.commit()

		self.db.close()

	def mergeJSON(self, existingJSON, newJSON):
		returnJSON = existingJSON
		for city in newJSON:
			if city in existingJSON:
				returnJSON[city] += newJSON[city]
			else:
				returnJSON[city] = newJSON[city]
		return returnJSON

	def getTopCities(self, d, n):
		# print d
		topCities = sorted(d.items(), key=operator.itemgetter(1))[(-1*n):]
		topCitiesKeys = [int(x[0]) for x in topCities]
		topCitiesKeys.reverse()
		i = len(topCitiesKeys)
		while i < n:
			topCitiesKeys.append(None)
			i += 1
		return topCitiesKeys

if __name__ == "__main__":
	p = Parser()
	print "Loading cities"
	p.loadCities()
	print "Parsing log"
	p.parseLog("web/access.log")
	print "Updating DB"
	p.updateDB()