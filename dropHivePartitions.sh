echo "Calling the environment variable script"

. $ENV_PATH/appl/common/bin/env_variables.env

table_list=`echo $4 | tr '[:upper:]' '[:lower:]'`
db_name="hive_db";
part_col_name="date"
part_val="20170901"
hdfs_bas_path="/basepath/"


hql_filename=$ENV_PATH/appl/common/bin/drop_partition.hql
hdfs_filename=$ENV_PATH/appl/common/bin/drop_partition.sh
rm -f $hql_filename 
rm -f $hdfs_filename 
 
# preparing hive sql file and hdfs file
for each_table in "${$table_list}"; 
do 
	"alter table ${db_name}.${each_table} drop if exists partition(${part_col_name}='${part_val}');$" >> ${hql_filename}  
	"hadoop fs -rm -r -skipTrash $hdfs_bas_path/$each_table"="$part_col_name"  >> ${hdfs_filename}
done

echo "Running the Drop Partition Scripts for Partition ::: $PARTITION"
hive -S -f ${hql_filename}
sh -x ${hdfs_filename}

status=$?
echo "Drop Partition Status ::: $status" 
if [[ $status -gt 0 ]]
then
    echo "Hive Drop partition failed: $status"
    exit 1
fi
