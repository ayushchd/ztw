-- phpMyAdmin SQL Dump
-- version 4.3.2
-- http://www.phpmyadmin.net
--
-- Host: 127.0.0.1:3306
-- Generation Time: Dec 19, 2014 at 07:06 AM
-- Server version: 5.6.22
-- PHP Version: 5.4.24

SET SQL_MODE = "NO_AUTO_VALUE_ON_ZERO";
SET time_zone = "+00:00";


/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8 */;

--
-- Database: `zomato_geoip`
--

-- --------------------------------------------------------

--
-- Table structure for table `geoip_mapping`
--

CREATE TABLE IF NOT EXISTS `geoip_mapping` (
  `id` int(11) NOT NULL,
  `ip_block` varchar(11) NOT NULL,
  `city_1` int(11) NOT NULL,
  `city_2` int(11) DEFAULT NULL,
  `city_3` int(11) DEFAULT NULL,
  `distribution_json` text NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- --------------------------------------------------------

--
-- Table structure for table `latest_timestamp`
--

CREATE TABLE IF NOT EXISTS `latest_timestamp` (
  `timestamp` int(14) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

--
-- Dumping data for table `latest_timestamp`
--

INSERT INTO `latest_timestamp` (`timestamp`) VALUES
(0);

--
-- Indexes for dumped tables
--

--
-- Indexes for table `geoip_mapping`
--
ALTER TABLE `geoip_mapping`
  ADD PRIMARY KEY (`id`), ADD UNIQUE KEY `ip` (`ip_block`);

--
-- AUTO_INCREMENT for dumped tables
--

--
-- AUTO_INCREMENT for table `geoip_mapping`
--
ALTER TABLE `geoip_mapping`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
