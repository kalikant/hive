-- find duplicate in table based on key columns
select brnchcd,count(brnchcd) as brnchcd_cnt from database.branch_table
group by brnchcd
having brnchcd_cnt > 1

-- find top 10 count
select brnchcd,count(*) as cnt from adatabase.branch_table
group by brnchcd
order by cnt desc
limit 10;

-- find date wise top 5 entry
select * from 
(
select a.brnchcd,a.lasttrndt,a.cnt,
rank() over (partition by brnchcd order by cnt desc) as rnk
from (select brnchcd,lasttrndt,count(*) as cnt from database.branch_table  group by brnchcd,lasttrndt)a
group by a.brnchcd,a.lasttrndt,a.cnt)b
where b.rnk=1

--other than hive : 
select grouping_col1,grouping_col2,count(*) as cnt, 
rank() over (partition by grouping_col1 order by cnt desc) as rnk
from database.branch_table
where between rnk 1  and 10 -- for top 10 records
group by grouping_col1,grouping_col2,cnt

-- this to replacement of oracle minus query
SELECT * FROM database_name.table_a a 
    LEFT OUTER JOIN database_name.table_b b ON 
( 
NVL(TRIM(LOWER(a.currencycode)),'') = NVL(TRIM(LOWER(b.currencycode)),'') AND 
NVL(TRIM(LOWER(a.accountno)),'') = NVL(TRIM(LOWER(b.accountno)),'') 
) 
    WHERE b.CURRENCYCODE IS NULL and b.ACCOUNTNO IS NULL ;
