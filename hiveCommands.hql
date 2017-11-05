-- passing param value to hive using -hivevar
hive -hivevar STAGE_DB=${STAGING_DB} -hivevar HDFS_BASE_PATH=${HDFS_BASE_PATH} -f $ENV_PATH/appl/hql/create_metadata.hql

