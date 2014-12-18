<?php

class Parser {
	public function __construct() {
		$this->baseDir = "/Users/zomato/tw/";
		$this->citiesDict = array();
	}

	public function loadCities() {
		$handle = fopen($this->baseDir . "cities.csv", "r");
		$data = fgetcsv($handle, 1000, ",");
		while( ($data = fgetcsv($handle, 1000, ",")) !== FALSE) {
			$this->citiesDict[substr($data[2], 0, strlen($data[2])-1)] = $data[0];
		}
	}

	public function parseLog($fname) {
		$handle = fopen($this->baseDir . $fname, "r");
		$logs = array();
		$tc = $vc = 0;
		$this->dist = array();
		// var_dump($this->citiesDict);
		while( ($data = fgets($handle)) !== FALSE) {
			$line = $data;
			$ipReg = preg_match("/\s((\d{0,3}\.){3}\d{0,3})\s/", $line, $ipMatches);
			$urlReg = preg_match("/(GET|POST)\s\/(.*?)\//", $line, $urlMatches);
			if (!isset($ipMatches[1]) || !isset($urlMatches[2])) {
				continue;
			}
			$ip = $ipMatches[1];
			$url = $urlMatches[2];
			$ipBlock = substr($ip, 0, strlen($ip)-strpos(strrev($ip), ".")-1);
			// echo "\n" . $tc;
			// $tc += 1;
			// continue;
			if (array_key_exists($url, $this->citiesDict)) {
				echo "\n" .$vc . ". " . $ipBlock . " ==> " . $url . " (" . $this->citiesDict[$url] . ")";
				$vc += 1;
				$city = $this->citiesDict[$url];
				if (!array_key_exists($ipBlock, $this->dist)) {
					$this->dist[$ipBlock] = array();
				}
				if (!array_key_exists($city, $this->dist[$ipBlock])) {
					$this->dist[$ipBlock][$city] = 1;
				} else {
					$this->dist[$ipBlock][$city] += 1;
				}
			}
			$tc += 1;
			// if ($tc > 50) {
			// 	break;
			// }
		}
	}

	public function updateDB() {
		$this->db = new mysqli("127.0.0.1", "zomato", "zomato", "zomato_geoip");
		$this->db->autocommit(false);
		foreach ($this->dist as $ipBlock => $dist) {
			$result = $this->db->query("SELECT * FROM geoip_mapping WHERE ip_block='$ipBlock' FOR UPDATE");
			if ($result->num_rows > 0) {
				$row = $result->fetch_assoc();
				$newJSON = $this->mergeJSON(json_decode($row['distribution_json'], true), $this->dist[$ipBlock]);
				$topCities = $this->getTopCities($newJSON, 3);
				$this->db->query("UPDATE geoip_mapping SET city_1=$topCities[0], city_2=$topCities[1], city_3=$topCities[2], distribution_json='" . json_encode($newJSON) ."' WHERE ip_block='$ipBlock'");
				$this->db->commit();
			} else {
				$topCities = $this->getTopCities($this->dist[$ipBlock], 3);
				$this->db->query("INSERT INTO geoip_mapping(ip_block, city_1, city_2, city_3, distribution_json) VALUES('$ipBlock', $topCities[0], $topCities[1], $topCities[2], '" . json_encode($this->dist[$ipBlock]) ."')");
				$this->db->commit();
			}
		}
		$this->db->close();
	}

	public function mergeJSON($existingJSON, $newJSON) {
		$returnJSON = $existingJSON;
		foreach ($newJSON as $city => $count) {
			if (array_key_exists($city, $existingJSON)) {
				$returnJSON[$city] += $newJSON[$city];
			} else {
				$returnJSON[$city] = $newJSON[$city];
			}
		}
		return $returnJSON;
	}

	public function getTopCities($d, $n) {
		arsort($d);
		$topCitiesKeys = array();
		foreach ($d as $city => $count) {
			$topCitiesKeys[] = $city;
		}
		$i = count($topCitiesKeys);
		while ($i < $n) {
			$topCitiesKeys[$i++] = "NULL";
		}
		return $topCitiesKeys;
	}
}

$p = new Parser();
echo "\nLoading cities";
$p->loadCities();
echo "\nParsing log";
$p->parseLog("web/access.log");
echo "\n Updating DB";
$p->updateDB();

?>