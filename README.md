# GeoIP Database Creation From Logs

## Config

Relevant configurations can be specified in the `config` file. The paths to logs to be parsed, and the cities database must be specified relative to the `base directory`

## How to run

* `cd` into the root directory (This must be done as config file is expected to be present in the same location where the script is executed from)
* For PHP, run `php src/main.php`
* For Python, run `python src/main.py`
* To run predictions against a specific log file, run `python src/verify.py {relative_path_to_log}`

## Requirements

* MySQLdb Python module should be installed

