<?php
date_default_timezone_set("UTC");
class Parser {
	public function __construct() {
		$this->config = parse_ini_file("config", true);
		$this->baseDir = $this->config["paths"]["base_directory"];
		$this->citiesDict = array();
		$this->db = new mysqli($this->config["database"]["server"], $this->config["database"]["username"], $this->config["database"]["password"], $this->config["database"]["dbname"]);
		$this->db->autocommit(true);
		$result = $this->db->query("SELECT timestamp FROM latest_timestamp LIMIT 1");
		$row = $result->fetch_assoc();
		$this->latestTimestamp = $row['timestamp'];
		$this->newTimestamp = $this->latestTimestamp;
		$this->batchSize = 300;
	}

	public function loadCities() {
		$handle = fopen($this->baseDir . $this->config["paths"]["relative_citiesCSV"], "r");
		$data = fgetcsv($handle, 1000, ",");
		while( ($data = fgetcsv($handle, 1000, ",")) !== FALSE) {
			$this->citiesDict[substr($data[2], 0, strlen($data[2])-1)] = $data[0];
		}
	}

	public function parseLog($fname) {
		$handle = gzopen($fname, "r");
		$logs = array();
		$tc = $vc = 0;
		$this->dist = array();
		while( ($data = gzgets($handle)) !== FALSE) {
			$tc += 1;
			// if ($tc > 10000) {
			// 	break;
			// }
			$line = $data;
			$dateReg = preg_match('/\[(\d{1,2}\/[a-zA-Z]{3}\/\d{4}:\d{2}:\d{2}:\d{2})\s[+-]\d{4}\]/', $line, $dateMatches);
			$ipReg = preg_match("/\s((\d{0,3}\.){3}\d{0,3})\s/", $line, $ipMatches);
			$urlReg = preg_match("/(GET|POST)\s\/(.*?)\//", $line, $urlMatches);
			if (!isset($ipMatches[1]) || !isset($urlMatches[2]) || !isset($dateMatches[1])) {
				continue;
			}
			$ip = $ipMatches[1];
			$url = $urlMatches[2];
			$date = $dateMatches[1];

			$ipBlock = substr($ip, 0, strlen($ip)-strpos(strrev($ip), ".")-1);

			if ($ipBlock == "127.0.0") {
				continue;
			}
			if (array_key_exists($url, $this->citiesDict)) {

				$date_components = explode("/", $date);
				$date_components[1] = $this->getMonthInt($date_components[1]);
				$time_components = $this->castArrayToInt(explode(":", $date_components[2]));
				$date_components = $this->castArrayToInt($date_components);
				$this->newTimestamp = mktime($time_components[1], $time_components[2], $time_components[3], $date_components[1], $date_components[0], $time_components[0]);
				if ($this->newTimestamp <= $this->latestTimestamp) {
					continue;
				}
				// echo "\n" .$vc . ". " . $ipBlock . " ==> " . $url . " (" . $date . ")";
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
			
		}
	}

	public function updateDB() {
		$queries = array();
		foreach ($this->dist as $ipBlock => $dist) {
			$result = $this->db->query("SELECT * FROM geoip_mapping WHERE ip_block='$ipBlock'");
			if ($result->num_rows > 0) {
				$row = $result->fetch_assoc();
				$newJSON = $this->mergeJSON(json_decode($row['distribution_json'], true), $this->dist[$ipBlock]);
				$topCities = $this->getTopCities($newJSON, 3);
				$queries[] = "UPDATE geoip_mapping SET city_1=$topCities[0], city_2=$topCities[1], city_3=$topCities[2], distribution_json='" . json_encode($newJSON) ."' WHERE ip_block='$ipBlock'";
			} else {
				$topCities = $this->getTopCities($this->dist[$ipBlock], 3);
				$queries[] = "INSERT INTO geoip_mapping(ip_block, city_1, city_2, city_3, distribution_json) VALUES('$ipBlock', $topCities[0], $topCities[1], $topCities[2], '" . json_encode($this->dist[$ipBlock]) ."')";
			}
			if (count($queries) > $this->batchSize) {
				if (!$this->db->multi_query(implode(";", $queries))) {
					echo $this->db->error;
				}
				while ($this->db->next_result()) {;}
				$queries = array();
			}
		}
		
		if (count($queries) > 0) {
			if(!$this->db->multi_query(implode(";", $queries))) {
				echo $this->db->error;
			}
			while ($this->db->next_result()) {;}
		}
		$this->db->query("UPDATE latest_timestamp SET timestamp=" . $this->newTimestamp);
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

	public function castArrayToInt($a) {
		foreach ($a as $k => $v) {
			$a[$k] = (int) $v;
		}
		return $a;
	}

	public function getMonthInt($month) {
		$map = array();
		$map['Jan'] = 0;
		$map['Feb'] = 1;
		$map['Mar'] = 2;
		$map['Apr'] = 3;
		$map['May'] = 4;
		$map['Jun'] = 5;
		$map['Jul'] = 6;
		$map['Aug'] = 7;
		$map['Sep'] = 8;
		$map['Oct'] = 9;
		$map['Nov'] = 10;
		$map['Dec'] = 11;
		return $map[$month];
	}

	public function processAllLogs() {
		$files = glob($this->baseDir . $this->config["paths"]["relative_configFilesFormat"]);
		$files_times = array();
		foreach ($files as $f) {
			$files_times[$f] = filemtime($f);
		}
		asort($files_times);
		foreach ($files_times as $f => $t) {
			echo "\nParsing log " . $f;
			$this->parseLog($f);
			echo "\nUpdating DB ";
			$this->updateDB();
		}
		$this->db->close();
	}
}

$p = new Parser();
echo "\nLoading cities";
$p->loadCities();
$p->processAllLogs();

?>