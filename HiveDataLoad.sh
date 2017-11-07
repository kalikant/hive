#!/bin/bash
#~~~~~~~~##################################################~~~~~~~~~~~~~~~~~~~~~~~~~~~#
# Name      : staging_tbl_load.sh
# Version   : 1.0
# Date      : 2017-11-06
# Author    : Kalikant Jha
#
# Description
# -------------------------------------------------------------------------------------#
# Parameters 1. <start date>  2. <end date> 
# sh staging_tbl_load.sh 20170101 20170102
# -------------------------------------------------------------------------------------#
# Date          Changed by      Ver     Change Desc W
# -------------------------------------------------------------------------------------#
#
#~~~~~~~~##################################################~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
set -x

  if [ $# -lt 2 ] || [ $# -gt 2 ]
  then
      echo "Expecting 2 parameters, Received "$#
      echo "USAGE :<start date> <end date>  "
      echo "[`date +%c`] ERROR: Unexpected number of parameters! Expected = 2, Received = $#"
      exit 1
  fi
SCRIPT_START_TIME=$(date +%s)

  start_dt=`date -d "${$1}" +'%Y-%m-%d'`
  end_dt=`date -d "${$1}" +'%Y-%m-%d'`
  
  hdfs_base_path="/sit/edm/hadoop/edmmantas/hdata/"
  hive_db="sit_aml_sri_open"
  logFile="$AML_SAIL_BASE/MAS/processing/log/staging_tbl_load.log"
  queryExecAnalyzer="$AML_SAIL_BASE/MAS/processing/log/query_exec_time.txt"
  hive_output_path="$AML_SAIL_BASE/MAS/processing/log/"
  script_path="$AML_SAIL_BASE/MAS/appl/scripts/"
 
>${queryExecAnalyzer} 
  START_TIME=$(date +%s)
  
	hadoop fs -rm -skipTrash ${hdfs_base_path}/${hive_db}/abc_temp1/*
	hadoop fs -rm -skipTrash ${hdfs_base_path}/${hive_db}/abc_temp2/*
	hadoop fs -rm -skipTrash ${hdfs_base_path}/${hive_db}/abc_temp4/*
	
	echo "============ HDFS CLEANUP IS DONE SUCCESSFULLY ============"
	END_TIME=$(date +%s)
	DIFF_TIME=$(( $END_TIME - $START_TIME ))
	echo " HDFS clean up time  ${DIFF_TIME} " >> ${queryExecAnalyzer}

START_TIME=$(date +%s)	
hive -S -e "
set hive.execution.engine=mr;
set hive.auto.convert.join=false;
set mapreduce.job.reduces=25;
msck repair table ${hive_db}.abc_temp1;
msck repair table ${hive_db}.abc_temp2;
msck repair table ${hive_db}.abc_temp3;"
hive_exec_status_code=$?
if [ ${hive_exec_status_code} -gt 0 ]
then
	echo "msck repair failed .. hive error code is ${hive_exec_status_code} " | tee -a ${logFile}
	echo ${hive_exec_status_code} 
else
	echo "============ MSCK REAPIR IS DONE SUCCESSFULLY ============"
	END_TIME=$(date +%s)
	DIFF_TIME=$(( $END_TIME - $START_TIME ))
	echo " MSCK REAPIR execution time  ${DIFF_TIME} " >> ${queryExecAnalyzer}
fi

START_TIME=$(date +%s)	
hive -S -e "
set hive.execution.engine=mr;
set hive.auto.convert.join=false;
set mapreduce.job.reduces=25;
analyze table ${hive_db}.abc_temp1 compute statistics;
analyze table ${hive_db}.abc_temp2 compute statistics;
analyze table ${hive_db}.abc_temp3 compute statistics;" 
hive_exec_status_code=$?
if [ ${hive_exec_status_code} -gt 0 ]
then
	echo "analyze table failed .. hive error code is ${hive_exec_status_code}  " | tee -a ${logFile}
	echo ${hive_exec_status_code} 
else 
	echo "============ ANALYZE COMPUTE IS DONE SUCCESSFULLY ============"
	END_TIME=$(date +%s)
	DIFF_TIME=$(( $END_TIME - $START_TIME ))
	echo " ANALYZE COMPUTE execution time  ${DIFF_TIME} " >> ${queryExecAnalyzer}
fi
  
START_TIME=$(date +%s)  
> ${hive_output_path}/row_count.out
hive -S -e "
set hive.execution.engine=mr;
set hive.auto.convert.join=false;
set mapreduce.job.reduces=25;
select count(*) from ${hive_db}.abc_temp1;
select count(*) from ${hive_db}.abc_temp2;
select count(*) from ${hive_db}.abc_temp3;" >> ${hive_output_path}/row_count.out 	
hive_exec_status_code=$?
if [ ${hive_exec_status_code} -gt 0 ]
then
	echo "count check failed for tmp tables .. hive error code is ${hive_exec_status_code}  " | tee -a ${logFile}
	echo $?
else
	zero_count=`grep -w '0' ${hive_output_path}/row_count.out | wc -l`
	if [[ ${zero_count} -eq "1" ]]
	then 
		echo "tmp table cleanup is successful .. " | tee -a ${logFile}
		END_TIME=$(date +%s)
		DIFF_TIME=$(( $END_TIME - $START_TIME ))
		echo " ROW COUNT check execution time  ${DIFF_TIME} " >> ${queryExecAnalyzer}
	else
		echo "count check failed for tables .. hive error code is ${hive_exec_status_code}  " | tee -a ${logFile}
		exit 1;
	fi
fi

END_TIME=$(date +%s)
DIFF_TIME=$(( $END_TIME - $SCRIPT_START_TIME ))
echo " complete cleanup time  ${DIFF_TIME} " >> ${queryExecAnalyzer}

	echo "======== calling data loading script =========="
	
echo " ===== DATE RANGE ${start_dt} and ${end_dt} ====== " 	
hive --hivevar start_dt=${start_dt} --hivevar end_dt=${end_dt} -f ${script_path}/staging_tbl_loading_query.hql >> ${hive_output_path}/loading_query.out
	
END_TIME=$(date +%s)
DIFF_TIME=$(( $END_TIME - $SCRIPT_START_TIME ))
echo " total data loading time ..  ${DIFF_TIME} "  >> ${queryExecAnalyzer}		 
