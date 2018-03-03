import pandas as pd
import numpy as np


#Create a DataFrame for the inbound trasactional feed
tradeDF=pd.read_csv('/home/veepee555/TRADE_TXN_FEED.csv')


##Sample the data to see if the file is read and the headers are lined correctly
tradeDF.head()

#Total Number of unique trade id (num of transactions in feed)
tradeDF.trade_id.count()

#The verification of the file layout, to help make sure all the attributes needed are captured by the dataFrame
tradeDF.columns

# All the datatypes of the inbound feed are described here
tradeDF.dtypes

#Filter Bad transactions with missing dates for trade_dt & trade_settlement dt
date_cleaned_tradeDF=tradeDF[tradeDF['trade_dt'].notnull() & tradeDF['trade_settlement_dt'].notnull()]

date_cleaned_tradeDF.count()

# Drop rows that have too many null values in the record ( say 5 or more)
date_cleaned_tradeDF.dropna(thresh=5)

date_cleaned_tradeDF.count()

#Selectively pick only BUY/SELL/ADJUST and filterout cancelled trades
date_cleaned_tradeDF=date_cleaned_tradeDF[date_cleaned_tradeDF['transaction_type'].isin(['BUY','SELL','ADJUST']) ]

#Look at the counts and shape of the frame, it is now rid of a lot of cancled data
date_cleaned_tradeDF.count()

#Sometimes the trade quantity may come in as null,
#one way to handle is to drop but another bussiness case is to apply a mean of the existing data
mean_quantity=date_cleaned_tradeDF['trade_quantity'].mean(axis=0)
date_cleaned_tradeDF.replace(np.nan,mean_quantity,inplace=True)

# We noticed some negative numbers on the file, this will ensure that is taken care of 
date_cleaned_tradeDF['trade_quantity']=date_cleaned_tradeDF['trade_quantity'].abs()

date_cleaned_tradeDF.tail()

# This will give us the number of rows(indexs) and columns(attributes)
date_cleaned_tradeDF.shape
date_cleaned_tradeDF.describe(include='all')
# Once cleaned please move to file to /tmp/venkat/clean/
date_cleaned_tradeDF.to_csv('/home/veepee555/trade_feed_clean.csv')
import os
os.system("hadoop fs -mkdir -p /tmp/usedcase")
os.system("hadoop fs -put /home/veepee555/trade_feed_clean.csv /tmp/usedcase/")



#Invoke HiveContext to execute SparkSQL commands
from pyspark.sql import HiveContext
ss = HiveContext(sc)
ss.sql('show databases').head(1000)
ss.sql('create database if not exists usedcase')

# Create Landing table if it does not exist
ss.sql('create table if not exists usedcase.land_trade_feed(rec_id int, trade_id int, trade_name string, trade_type string,trade_dt string,trade_settlement_dt string,transaction_type string,trade_quantity int,branch_name string,branch_location_id string,lob string,org_unit string,fa_num string,fa_split string,cusip string,start_dt string,end_dt string,symbols string,symbol_type string,quantity string,acct_num string,acct_name string,acct_type string,acct_sub_type string,product_id int,product_name string,product_desc string,product_category string) row format delimited fields terminated by "," stored as textfile')


#Load data Into the landing table, It can be the full file on day one or delta on subsequent days 
ss.sql("load data inpath '/tmp/usedcase/trade_feed_clean.csv' overwrite into table usedcase.land_trade_feed")


# Read the feed layout
for rec in ss.sql('select *  from usedcase.land_trade_feed').head(1):
    print rec



#Creating product dim. external textfile storage table 
ss.sql('create external table if not exists usedcase.dim_product_ext(product_id int,product_name string,product_desc string,product_category string)  stored as textfile')


#Creating account dim. external textfile storage table 
ss.sql('create external table if not exists usedcase.dim_acct_ext(acct_num int,acct_name string,acct_type string,acct_sub_type string)  stored as textfile')


#Creating branch dim. external textfile storage table 
ss.sql('create external table if not exists usedcase.dim_branch_ext(branch_id int,branch_name string,branch_location_id string,lob string, org_unit string, fa_num string, fa_split string)  stored as textfile')


#Creating security dim. external textfile storage table 
ss.sql('create external table if not exists usedcase.dim_security_ext(sec_id string,cusip string,start_dt string,end_dt string, symbl string, symbl_type string, quantity int, unit_price int)  stored as textfile')


#Creating trade dim. external textfile storage table 
ss.sql('create external table if not exists usedcase.dim_trade_ext(tarde_id int,trade_name string,trade_type string,trade_dt string, trade_settlement_dt string, transaction_type string, trade_quantity int)  stored as textfile')


#List out the external tables
for rec in ss.sql('show tables').head(100):
    print rec


# Loading account dimensions temporary external table (always load incremental into the delta and add that to the dimensions(external))
ss.sql('drop table if exists usedcase.dim_acct_delta')
ss.sql('create table if not exists usedcase.dim_acct_delta as select * from usedcase.dim_acct_ext where 1=0')
ss.sql('insert into table usedcase.dim_acct_delta  select cast(land_trade_feed.acct_num as int),max(land_trade_feed.acct_name), \
max(land_trade_feed.acct_type), max(land_trade_feed.acct_sub_type) from \
usedcase.land_trade_feed land_trade_feed \
left outer join usedcase.dim_acct_ext dim_acct_ext \
on dim_acct_ext.acct_num =cast(land_trade_feed.acct_num as int)\
where dim_acct_ext.acct_num is null and  land_trade_feed.rec_id is not null group by land_trade_feed.acct_num')
ss.sql('insert into table usedcase.dim_acct_ext select * from usedcase.dim_acct_delta')


# In[ ]:

# Loading product dimensions temporary external table (always load incremental into the delta and add that to the dimensions(external))
ss.sql('drop table if exists usedcase.dim_product_delta ')
ss.sql('create table if not exists usedcase.dim_product_delta as select * from usedcase.dim_product_ext where 1=0')
ss.sql('insert into  table  usedcase.dim_product_delta select max(land_trade_feed.product_id),land_trade_feed.product_name, max(land_trade_feed.product_desc),max(land_trade_feed.product_category) from usedcase.land_trade_feed land_trade_feed left outer join usedcase.dim_product_ext dim_product_ext on dim_product_ext.product_name=land_trade_feed.product_name where dim_product_ext.product_name is null and land_trade_feed.rec_id is not null group by land_trade_feed.product_name')
ss.sql('insert into table usedcase.dim_product_ext select * from usedcase.dim_product_delta')


# In[ ]:

# Loading branch dimensions temporary external table (always load incremental into the delta and add that to the dimensions(external))
ss.sql('drop table if exists usedcase.dim_branch_delta ')
ss.sql('create table usedcase.dim_branch_delta as select * from usedcase.dim_branch_ext where 1=0')
ss.sql('insert into table usedcase.dim_branch_delta  \
select max(land_trade_feed.rec_id),land_trade_feed.branch_name, \
max(land_trade_feed.branch_location_id),max(land_trade_feed.lob), \
max(land_trade_feed.org_unit),max(land_trade_feed.fa_num),max(land_trade_feed.fa_split) \
from usedcase.land_trade_feed land_trade_feed \
left outer join usedcase.dim_branch_ext dim_branch_ext \
on dim_branch_ext.branch_name=land_trade_feed.branch_name \
where dim_branch_ext.branch_name is null  and land_trade_feed.rec_id is not null \
group by land_trade_feed.branch_name')
ss.sql('insert into table usedcase.dim_branch_ext select * from usedcase.dim_branch_delta')


# In[ ]:

# Loading security master dimensions temporary external table (always load incremental into the delta and add that to the dimensions(external))
ss.sql('drop table if exists usedcase.dim_security_delta ')
ss.sql('create table usedcase.dim_security_delta as select * from usedcase.dim_security_ext where 1=0')
ss.sql('insert into table usedcase.dim_security_delta select concat(land_trade_feed.cusip,substr(land_trade_feed.start_dt,3,2),substr(land_trade_feed.start_dt,6,2),substr(land_trade_feed.start_dt,9,2)),land_trade_feed.cusip, land_trade_feed.start_dt,max(land_trade_feed.end_dt),max(land_trade_feed.symbols),max(land_trade_feed.symbol_type), sum(land_trade_feed.quantity),sum(land_trade_feed.quantity) from usedcase.land_trade_feed land_trade_feed left outer join usedcase.dim_security_ext dim_security_ext on (dim_security_ext.cusip=land_trade_feed.cusip and dim_security_ext.start_dt=land_trade_feed.start_dt) where land_trade_feed.rec_id is not null and dim_security_ext.cusip is null group by land_trade_feed.cusip,land_trade_feed.start_dt')
ss.sql('insert into table usedcase.dim_security_ext select * from usedcase.dim_security_delta')


# In[ ]:

# Loading trade  dimensions temporary external table(always load incremental into the delta and add that to the dimensions(external))
ss.sql('drop table if exists usedcase.dim_trade_delta ')
ss.sql('create table usedcase.dim_trade_delta as select * from usedcase.dim_trade_ext where 1=0')
ss.sql('insert into usedcase.dim_trade_delta \
select land_trade_feed.trade_id,land_trade_feed.trade_name, \
land_trade_feed.trade_type, \
land_trade_feed.trade_dt,land_trade_feed.trade_settlement_dt, \
land_trade_feed.transaction_type,land_trade_feed.trade_quantity \
from usedcase.land_trade_feed land_trade_feed \
left outer join usedcase.dim_trade_ext dim_trade_ext \
on land_trade_feed.trade_id=dim_trade_ext.tarde_id \
where dim_trade_ext.tarde_id is null and land_trade_feed.rec_id is not null')
ss.sql('insert into table usedcase.dim_trade_ext select * from usedcase.dim_trade_delta')


# In[ ]:

# Create all managed table for the dimensions (stored as ORC)
ss.sql('create table if not exists  usedcase.dim_product(product_id int,product_name string,product_desc string,product_category string)  stored as orc')
ss.sql('create  table if not exists usedcase.dim_acct(acct_num int,acct_name string,acct_type string,acct_sub_type string)  stored as orc')
ss.sql('create  table if not exists usedcase.dim_branch(branch_id int,branch_name string,branch_location_id string,lob string, org_unit string, fa_num string, fa_split string)  stored as orc')
ss.sql('create  table if not exists usedcase.dim_security(sec_id string,cusip string,start_dt string,end_dt string, symbl string, symbl_type string, quantity int, unit_price int)  stored as orc')
ss.sql('create  table if not exists usedcase.dim_trade(tarde_id int,trade_name string,trade_type string,trade_dt string, trade_settlement_dt string, transaction_type string, trade_quantity int)  stored as orc')


# In[ ]:

#Populate these tables with the data from the external tables (respective to each ORC there is a source external table)
ss.sql('insert overwrite table  usedcase.dim_product select *  from usedcase.dim_product_ext')
ss.sql('insert overwrite table  usedcase.dim_acct select *  from usedcase.dim_acct_ext')
ss.sql('insert overwrite table  usedcase.dim_branch select *  from usedcase.dim_branch_ext')
ss.sql('with temp as (select oq.sec_id,oq.cusip,oq.start_dt,min(sq.start_dt) min_start_dt,oq.end_dt,max(oq.symbl) symbl,max(oq.symbl_type) symbl_type,max(oq.quantity) quantity,max(oq.unit_price) unit_price from usedcase.dim_security_ext oq inner join  usedcase.dim_security_ext sq on oq.cusip=sq.cusip where to_date(oq.start_dt) <= to_date(sq.start_dt) group by oq.sec_id,oq.cusip,oq.start_dt, oq.end_dt) insert overwrite table  usedcase.dim_security select sec_id,cusip,start_dt,CASE WHEN start_dt = min_start_dt then end_dt else min_start_dt end,symbl,symbl_type,quantity,unit_price from temp')
ss.sql('insert overwrite table  usedcase.dim_trade select *  from usedcase.dim_trade_ext')


# In[ ]:

#Define and create a fact table for trade txn details 
#( this is a factless fact marrying all the dimensions to capture their many:many relationship)
ss.sql('create table if not exists usedcase.fact_trade_txn_details (branch_id int, acct_num int,sec_id string ,trade_id int,product_id int ) stored as orc')


# In[ ]:

# This Cross reference table will be rebuilt for every run 
#(This is a fact less fact table just hoding the relatioship between dimensions)
ss.sql('insert overwrite table usedcase.fact_trade_txn_details \
select distinct b.branch_id,a.acct_num, s.sec_id,t.tarde_id,p.product_id \
from usedcase.land_trade_feed src \
inner join usedcase.dim_product p on (src.product_name=p.product_name) \
inner join usedcase.dim_branch b on (src.branch_name=b.branch_name) \
inner join usedcase.dim_acct a on (src.acct_num=a.acct_num) \
inner join usedcase.dim_trade t on (src.trade_name=t.trade_name) \
inner join usedcase.dim_security s on (src.cusip=s.cusip and src.start_dt=s.start_dt)')

# Create View for location based transaction type trend

ss.sql('create view if not exists usedcase.loc_based_tran_trend as \
select  temp.branch_location_id,temp.transaction_type, count(distinct temp.tarde_id) tran_cnt \
from  (select b.branch_location_id,t.transaction_type,t.tarde_id \
from \
usedcase.fact_trade_txn_details f \
inner join usedcase.dim_branch b \
on (f.branch_id=b.branch_id) \
inner join usedcase.dim_trade t \
on (f.trade_id=t.tarde_id)) temp \
group by temp.branch_location_id,temp.transaction_type \
order by 1,2,3')





