-- adding temperory function 
-- it work till current session ends
-- log into hive and run below command
ADD JAR /home/kalikant/utilities/sha-256.jar;
DROP FUNCTION IF EXISTS hive_db.sha_256;
CREATE TEMPORARY FUNCTION sha_256 AS 'og.package.HiveUDF';

-- can test function using below command
select sha_256('Kalikant');


-- adding permanent function
-- it can be used acrros sessions
-- it can be used across database through database name

-- from unix shel copy jar to HDFS partition
hdfs dfs -put 'hdfs://SERVICENAME/user/hive/sha-256.jar'

-- now login to hive and run below command

DROP FUNCTION IF EXISTS hive_db.sha_256;
create function hive_db.sha_256 AS 'og.package.HiveUDF' using jar 'hdfs://SERVICENAME/user/hive/sha-256.jar';
RELOAD function hive_db.sha_256;

-- can test function using below command
select hive_db.sha_256('Kalikant');
