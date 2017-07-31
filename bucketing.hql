--
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=1000;


DROP TABLE IF EXISTS bucketed_user;
DROP TABLE IF EXISTS temp_user;
CREATE TABLE temp_user(
	       firstname VARCHAR(64),
       	lastname  VARCHAR(64),
       	address   STRING,
       	country   VARCHAR(64),
       	city      VARCHAR(64),
       	state     VARCHAR(64),
       	post      STRING,
       	phone1    VARCHAR(64),
       	phone2    STRING,
       	email     STRING,
       	web       STRING
       	)
       	ROW FORMAT DELIMITED 
       		FIELDS TERMINATED BY ','
       		LINES TERMINATED BY '\n'
       STORED AS TEXTFILE;
 
LOAD DATA LOCAL INPATH '/home/hemant/Desktop/hadoopaptron/user.txt' INTO TABLE temp_user;

CREATE TABLE bucketed_user(
       	firstname VARCHAR(64),
       	lastname  VARCHAR(64),
       	address   STRING,
       	city 	     VARCHAR(64),
       state     VARCHAR(64),
       	post      STRING,
       	phone1    VARCHAR(64),
       	phone2    STRING,
       	email     STRING,
       	web       STRING
       	)
       COMMENT 'A bucketed sorted user table'
       	PARTITIONED BY (country VARCHAR(64))
       CLUSTERED BY (state) SORTED BY (city) INTO 32 BUCKETS
       	STORED AS SEQUENCEFILE;


INSERT OVERWRITE TABLE bucketed_user PARTITION (country)
       SELECT  firstname ,
               		lastname  ,
               		address   ,
               city      ,
               state     ,
               		post      ,
               		phone1    ,
               		phone2    ,
               		email     ,
               		web       ,
               		country   
        	FROM temp_user;
