SLF4J: Class path contains multiple SLF4J bindings.
SLF4J: Found binding in [jar:file:/usr/local/apache-hive-3.1.2-bin/lib/log4j-slf4j-impl-2.10.0.jar!/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: Found binding in [jar:file:/usr/local/hadoop-3.2.1/share/hadoop/common/lib/slf4j-log4j12-1.7.25.jar!/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: See http://www.slf4j.org/codes.html#multiple_bindings for an explanation.
SLF4J: Actual binding is of type [org.apache.logging.slf4j.Log4jLoggerFactory]
Hive Session ID = 2e46b8ec-20f8-47d5-9f03-1b5f069a1b51

Logging initialized using configuration in jar:file:/usr/local/apache-hive-3.1.2-bin/lib/hive-common-3.1.2.jar!/hive-log4j2.properties Async: true
Hive-on-MR is deprecated in Hive 2 and may not be available in the future versions. Consider using a different execution engine (i.e. spark, tez) or using Hive 1.X releases.
Hive Session ID = 5e7616ab-7229-47a6-9a70-695a5d59d2c0
hive> show databases;
OK
default
Time taken: 4.185 seconds, Fetched: 1 row(s)
hive> create table sales_order_data_csv_v1 ( ORDERNUMBER int, QUANTITYORDERED int, PRICEEACH float, ORDERLINENUMBER int, SALES float, STATUS string, QTR_ID int, MONTH_ID int, YEAR_ID int, PRODUCTLINE string, MSRP int, PRODUCTCODE string, PHONE string, CITY string, STATE string, POSTALCODE string, COUNTRY string, TERRITORY string, CONTACTLASTNAME string, CONTACTFIRSTNAME string, DEALSIZE string ) row format delimited fields terminated by ',' tblproperties("skip.header.line.count"="1") ;
OK
Time taken: 2.373 seconds
hive> load data local inpath 'file:///config/workspace/sales_order_data.csv' into table sales_order_data_csv_v1;
Loading data to table default.sales_order_data_csv_v1
OK
Time taken: 3.943 seconds
hive> create table sales_order_data_orc ( ORDERNUMBER int, QUANTITYORDERED int, PRICEEACH float, ORDERLINENUMBER int, SALES float, STATUS string, QTR_ID int, MONTH_ID int, YEAR_ID int, PRODUCTLINE string, MSRP int, PRODUCTCODE string, PHONE string, CITY string, STATE string, POSTALCODE string, COUNTRY string, TERRITORY string, CONTACTLASTNAME string, CONTACTFIRSTNAME string, DEALSIZE string ) stored as orc;
OK
Time taken: 0.398 seconds
hive> from sales_order_data_csv_v1 insert overwrite table sales_order_data_orc select *;
Query ID = abc_20230821110935_bd1f09fd-2d16-4738-904f-fada335c9958
Total jobs = 1
Launching Job 1 out of 1
Number of reduce tasks determined at compile time: 1
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Starting Job = job_1692595766947_0001, Tracking URL = http://61e3608cadc8:8088/proxy/application_1692595766947_0001/
Kill Command = /usr/local/hadoop/bin/mapred job  -kill job_1692595766947_0001
Hadoop job information for Stage-1: number of mappers: 1; number of reducers: 1
2023-08-21 11:10:14,423 Stage-1 map = 0%,  reduce = 0%
2023-08-21 11:10:35,602 Stage-1 map = 100%,  reduce = 0%, Cumulative CPU 22.65 sec
2023-08-21 11:10:49,481 Stage-1 map = 100%,  reduce = 100%, Cumulative CPU 25.61 sec
MapReduce Total cumulative CPU time: 25 seconds 610 msec
Ended Job = job_1692595766947_0001
Stage-4 is selected by condition resolver.
Stage-3 is filtered out by condition resolver.
Stage-5 is filtered out by condition resolver.
Moving data to directory hdfs://localhost/user/hive/warehouse/sales_order_data_orc/.hive-staging_hive_2023-08-21_11-09-35_259_8571698466713928725-1/-ext-10000
Loading data to table default.sales_order_data_orc
MapReduce Jobs Launched: 
Stage-Stage-1: Map: 1  Reduce: 1   Cumulative CPU: 25.61 sec   HDFS Read: 400690 HDFS Write: 50366 SUCCESS
Total MapReduce CPU Time Spent: 25 seconds 610 msec
OK
Time taken: 78.879 seconds
hive> select * from sales_order_data_orc limit 5;
OK
10107   30      95.7    2       2871.0  Shipped 1       2       2003    Motorcycles     95      S10_1678        2125557818      NYC     NY      10022   USA     NA      Yu      Kwai    Small
10121   34      81.35   5       2765.9  Shipped 2       5       2003    Motorcycles     95      S10_1678        26.47.1555      Reims           51100   France  EMEA    Henriot Paul    Small
10134   41      94.74   2       3884.34 Shipped 3       7       2003    Motorcycles     95      S10_1678        +33 1 46 62 7555        Paris           75508   France  EMEA    Da Cunha        Daniel  Medium
10145   45      83.26   6       3746.7  Shipped 3       8       2003    Motorcycles     95      S10_1678        6265557265      Pasadena        CA      90003   USA     NA      Young   Julie   Medium
10159   49      100.0   14      5205.27 Shipped 4       10      2003    Motorcycles     95      S10_1678        6505551386      San Francisco   CA              USA     NA      Brown   Julie   Medium
Time taken: 1.229 seconds, Fetched: 5 row(s)
hive> add file file:////config/workspace/multiply_udf.py;
Added resources: [file:////config/workspace/multiply_udf.py]

hive> select transform(quantityordered) using 'python multiply_udf.py' as (quantityordered int) from sales_order_data_orc limit 5;
Query ID = abc_20230821113256_9be2a51e-89ac-4489-ad88-5b0b1d2c1977
Total jobs = 1
Launching Job 1 out of 1
Number of reduce tasks is set to 0 since there's no reduce operator
Starting Job = job_1692595766947_0002, Tracking URL = http://61e3608cadc8:8088/proxy/application_1692595766947_0002/
Kill Command = /usr/local/hadoop/bin/mapred job  -kill job_1692595766947_0002
Hadoop job information for Stage-1: number of mappers: 1; number of reducers: 0
2023-08-21 11:33:19,817 Stage-1 map = 0%,  reduce = 0%
2023-08-21 11:33:36,959 Stage-1 map = 100%,  reduce = 0%, Cumulative CPU 4.12 sec
MapReduce Total cumulative CPU time: 4 seconds 120 msec
Ended Job = job_1692595766947_0002
MapReduce Jobs Launched: 
Stage-Stage-1: Map: 1   Cumulative CPU: 4.12 sec   HDFS Read: 25714 HDFS Write: 167 SUCCESS
Total MapReduce CPU Time Spent: 4 seconds 120 msec
OK
300
340
410
450
490
Time taken: 43.722 seconds, Fetched: 5 row(s)
hive> 
