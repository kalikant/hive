# calling hive for file load
HDFS_FILEPATH="$HDFS_BASE_PATH/"
BATCH_DATE=`date -u +'%Y_%m_%d'`
hadoop fs -test -d ${HDFS_FILEPATH}/batch_date=${BATCH_DATE}/
if [ `echo $?` -eq 0 ]
then
	hadoop fs -appendToFile $BASE_PATH/appl/oracle_query_output.txt ${HDFS_FILEPATH}/batch_date=${BATCH_DATE}/oracle_query_output.txt
	
else
	hadoop fs -mkdir -p ${HDFS_FILEPATH}/batch_date=${BATCH_DATE}/
	hadoop fs -put $BASE_PATH/appl/oracle_query_output.txt  ${HDFS_FILEPATH}/batch_date=${BATCH_DATE}/
	location="${HDFS_FILEPATH}/batch_date=${BATCH_DATE}/"
	echo "use $HIVE_DB;ALTER TABLE hive_table ADD if not exists PARTITION (batch_date='${BATCH_DATE}') location '$location';" > ${ENV_PATH}/appl/partition.hql
	hive -f "${ENV_PATH}/appl/partition.hql"
fi
