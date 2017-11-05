-- passing param value to hive using -hivevar
hive -hivevar STAGE_DB=${STAGING_DB} -hivevar HDFS_BASE_PATH=${HDFS_BASE_PATH} -f $ENV_PATH/appl/hql/create_metadata.hql

-- data loading into hive steps
-- loading data into staging table
-- this table is truncate and load
load data local inpath '/UNIX_PATH/data/*.txt' overwrite into table hive_ab.hive_stg_tbl;

-- verify row count
select count(*) from hive_ab.hive_stg_tbl;

-- load data into actual landing table
insert overwrite table hive_ab.hive_lnd_tbl partition(date) select * from hive_ab.hive_stg_tbl;

-- if data file coming with different partition column name and warehouse loading partition column is different
insert overwrite table hive_ab.hive_lnd_tbl partition(date=business_date) select * from hive_ab.hive_stg_tbl;

-- verify row count in actual landing table
select date,count(*) from hive_ab.hive_lnd_tbl group by date;  
select date,count(*) from hive_ab.hive_lnd_tbl where date between '2017_08_01' and '2017_08_31' group by date;  
