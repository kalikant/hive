create table emp(id int,name string,dept int,salary double);

insert into emp values(101,'kalikant',01,1234.56);
insert into emp values(102,'anil',02,11234.56);

create table emp_seq(id int,name string,dept int,salary double)
stored as sequencefile;

insert into emp_seq select * from emp; -- text to seq

create table emp_rc(id int,name string,dept int,salary double)
stored as rcfile;

insert into emp_rc select * from emp_seq; -- seq to rc
insert into emp_rc select * from emp; -- text to rc

create table emp_orc(id int,name string,dept int,salary double)
stored as orcfile;

insert into emp_orc select * from emp_rc;  -- rc to orc
insert into emp_orc select * from emp_seq; -- seq to orc
insert into emp_orc select * from emp; -- text to orc

create table emp_avro(id int,name string,dept int,salary double)
stored as avro;

insert into emp_avro select * from emp;  -- text to avro
insert into emp_avro select * from emp_seq;  -- seq to avro
insert into emp_avro select * from emp_rc;  -- rc to avro
insert into emp_avro select * from emp_orc;  -- orc to avro

create table emp_parquet(id int,name string,dept int,salary double)
stored as parquet;

insert into emp_parquet select * from emp;  -- text to parquet
insert into emp_parquet select * from emp_seq;  -- seq to parquet
insert into emp_parquet select * from emp_rc;  -- rc to parquet
insert into emp_parquet select * from emp_orc;  -- orc to parquet
insert into emp_parquet select * from emp_avro;  -- avro to parquet
