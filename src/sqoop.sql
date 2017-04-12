-- MySQL dump 10.13  Distrib 5.1.71, for redhat-linux-gnu (x86_64)
--
-- Host: localhost    Database: sqoop
-- ------------------------------------------------------
-- Server version	5.1.71

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Table structure for table `database_info`
--

DROP TABLE IF EXISTS `database_info`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `database_info` (
  `database_id` int(11) NOT NULL AUTO_INCREMENT,
  `database_link` varchar(200) NOT NULL,
  `database_username` varchar(20) NOT NULL,
  `database_pwd` varchar(50) NOT NULL,
  PRIMARY KEY (`database_id`)
) ENGINE=MyISAM AUTO_INCREMENT=2 DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `database_info`
--

LOCK TABLES `database_info` WRITE;
/*!40000 ALTER TABLE `database_info` DISABLE KEYS */;
INSERT INTO `database_info` VALUES (1,'jdbc:oracle:thin:@192.168.1.28:1521:xe','YUNCHEN','root');
/*!40000 ALTER TABLE `database_info` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `sqoop_info`
--

DROP TABLE IF EXISTS `sqoop_info`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `sqoop_info` (
  `sqoop_id` int(11) NOT NULL,
  `table_name` varchar(60) NOT NULL,
  `sqoop_timestamp` varchar(50) NOT NULL,
  `sqoop_delta_full` varchar(20) NOT NULL,
  `sqoop_compress_format` varchar(20) NOT NULL,
  `sqoop_storage_format` varchar(20) NOT NULL,
  `sqoop_map_count` varchar(20) NOT NULL,
  `sqoop_pri_key` varchar(20) NOT NULL,
  `sqoop_time_varchar` varchar(20) NOT NULL,
  `sqoop_map_column_java` varchar(20) DEFAULT NULL
) ENGINE=MyISAM DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `sqoop_info`
--

LOCK TABLES `sqoop_info` WRITE;
/*!40000 ALTER TABLE `sqoop_info` DISABLE KEYS */;
INSERT INTO `sqoop_info` VALUES (1,'YUNCHEN.MYTABLE','INC_DATETIME','full','no','no','5','ID','no',NULL);
/*!40000 ALTER TABLE `sqoop_info` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `table_timestamp`
--

DROP TABLE IF EXISTS `table_timestamp`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `table_timestamp` (
  `table_name` varchar(200) NOT NULL,
  `mintime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `maxtime` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00'
) ENGINE=MyISAM DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `table_timestamp`
--

LOCK TABLES `table_timestamp` WRITE;
/*!40000 ALTER TABLE `table_timestamp` DISABLE KEYS */;
INSERT INTO `table_timestamp` VALUES ('MYTABLE','0000-00-00 00:00:00','2016-12-29 03:33:02'),('MYTABLE','0000-00-00 00:00:00','2016-12-29 03:33:02');
/*!40000 ALTER TABLE `table_timestamp` ENABLE KEYS */;
UNLOCK TABLES;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2017-01-09 21:27:07


CREATE TABLE `hdfs_export_info` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `database_link` varchar(100) DEFAULT NULL,
  `database_username` varchar(100) DEFAULT NULL,
  `database_password` varchar(100) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=MyISAM AUTO_INCREMENT=2 DEFAULT CHARSET=latin1;


CREATE TABLE `hdfs_export_opts` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `source_id` int(11) DEFAULT NULL,
  `table_name` varchar(100) DEFAULT NULL,
  `table_columns` varchar(1000) DEFAULT NULL,
  `update_key` varchar(100) DEFAULT NULL,
  `export_dir` varchar(100) DEFAULT NULL,
  `map_column_java` varchar(300) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=MyISAM AUTO_INCREMENT=2 DEFAULT CHARSET=latin1;











