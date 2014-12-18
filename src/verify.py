"""
Verify accuracy
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

class Verifier:

	def __init__(self):
		self.baseDir = "/Users/zomato/tw/"
		self.citiesDict = {}
		self.db = MySQLdb.connect("localhost", "zomato", "zomato", "zomato_geoip")
		self.cursor = self.db.cursor(MySQLdb.cursors.DictCursor)
		self.cursor.execute("SELECT timestamp FROM latest_timestamp LIMIT 1")
		self.latestTimestamp = self.cursor.fetchone()['timestamp']
		self.newTimestamp = self.latestTimestamp
		self.batchSize = 300

	def loadCities(self):
		with open(os.path.join(self.baseDir, "cities.csv")) as f:
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
		fl = gzip.open(fname, "r")
		t = 0
		f = 0
		nf = 0
		memo = {}
		for line in fl:
			tc += 1
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
				if ipBlock not in memo:
					self.cursor.execute("SELECT city_1, city_2, city_3 FROM geoip_mapping WHERE ip_block=%s", (ipBlock,))
					rows = self.cursor.fetchall()
					if len(rows) > 0:
						ct = rows[0]['city_1']
						memo[ipBlock] = ct
				vc += 1
				# print ipBlock
				if ipBlock in memo:
					print self.citiesDict[url]
					print memo[ipBlock]
					if int(self.citiesDict[url]) == memo[ipBlock]:
						t += 1
					else:
						f += 1
				else:
					nf += 1
					print "fallback"
				print "======",t,f,nf,vc
				self.db.commit()
		fl.close()
		

if __name__ == "__main__":
	v = Verifier()
	v.loadCities()
	v.parseLog(v.baseDir + "logs/access.log.13.gz")