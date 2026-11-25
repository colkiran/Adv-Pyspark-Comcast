1\. Performance Tuning using PySpark



2\. how to work on huge data and understand how the data is processed internally



\--------------------------------------------------------------

1\. Data Engineering

2\. PySpark in Data Engineering

3\. PySpark Architecture

4\. Drivers, Executors

5\. Jobs Stages and Tasks

6\. Cluster Concepts





Data Engineering

\----------------

what is data engineering?



Three methods of Data Ingestion

\--------------------------------

1\. Batch Ingestion

&#x20;

collect data at scheduled intervals



12:00 AM - extract orders

12:10 AM - Transfer Data

12:20 AM - Store it in Datalake



Apache Airflow

Azure Data Factory

AWS Glue

Informatica

Python Scripts









2\. Real time ingestion / Streaming



continuously collect data as event occurs



customer places order  ->  Order Application ->  Apache Kafka  ->  Streaming consumer   ->   Data Lake / Analytics



Typical Use

\------------

1\. Fraud Detection

2\. Live order tracking

3\. Real time stock updates

4\. Website clickstream analytics





3\. Change data capture ingestion



CDC captures changes made to operational database



Before

C001	Ravi	Bangalore



After

C001	Ravi	Chennai



Instead of extracting the entire table again CDC captures the inserted updated and deleted records



Debezium

Kafaka Connect

AWS DMS

Azure Data Factory CDC capabilities



1\. Moves data from multiple independent system into one platform



2\. Supports both historical and continuously generated data



3\. Reduce the need for repeated full database extraction



4\. Helps make data available for downstream analytics



5\. provides a mechanism to schedule monitor and recover the data transfer





Layer - Data Lake (Bronze Layer)

\---------------------------------

Data Lake is centralized storage that can hold large volumes of structured semi structured and unstructured data



Example

\-------

Amazon S3

Azure Data lake storage

Google Cloud Storage

Hadoop HDFS



The Bronze layer will store data in its original or near original form



for example

Order\_ID	Customer\_ID	Amount	Order\_Date

101		C001		65000	2026-11-20

102		C002		25000	2026-11-20

103		C003		NULL	2026-11-20



What is the use of bronze layer?



Preserves the original incoming data

Helps recovering data if a transformation has a problem

Supports auditing and trouble shooting

Allows us to reprocess historical data when business rules changes

Separate source data from business transformation logic





**Layer - 4 Data Processing and Transformation**

\--------------------------------------------

This is one important layer in the entire architecture



Data Processing means converting raw data into meaningful accurate consistent and usable data



after receiving 100000 orders the data may contain



a. missing values

b. duplicate records

c. incorrect data types

d. invalid order amount

e. Inconsistent city names

f. Incorrect dates

g. Invalid Cust ID's



Order\_ID	Customer\_ID	Amount	Order\_Date

101		C001		65000	2026-11-20

102		C002		"25000"	2026-11-20

103		C003		NULL	2026-11-20

101		C001		65000	2026-11-20

104		C004	       -25000	2026-11-20



identify the missing amount

Investigate the negative amounts

Reject or quarantine the invalid records

Correct values only when reliable business rule or source exists



B. Remove duplicates



C. Data type conversion



D. Data Standardization - City - Chennai, Madras, CHENNAI



E. Data Integration



Orders -> joining using Customer ID



Customers -> Joining using Order ID



Payments -> Combined order dataset



performs complex operations like



* joins
* Aggregations
* Filtering
* Sorting
* Deduplication
* Data enrichment
* Business Rule application
* Feature Engineering for ML





**Layer 5 - Silver Layer**

\----------------------

This layer stores the cleaned, standardized, deduplicated and validated data



Bronze Layer

\-------------

Order\_ID	Customer\_ID	Amount	City

101		C001		65000	Bangalore

101		C001		65000	Bangalore

102		C002		NULL	Mysore

103		C003		-5000	Bengaluru

104		C004		30000	bangalore



Silver Layer

\------------

Order\_ID	Customer\_ID	Amount	City

101		C001		65000	BANGALORE

104		C004		30000	BANGALORE



Silver Layer Contains

\---------------------

1. Cleaned Data
2. Standardized Data
3. Deduplicated Data
4. Validated Data
5. Integrated Data
6. Consistent Schemas



Need of Silver Layer

\--------------------

Imagine there five different departments using the same customer data



Sales

Finance

Marketing

Logistics

Customer Support



If each dept independently cleans the data they may produce different results



Silver creates a reusable consistent version of data



**Layer - 6 Gold Layer**

\--------------------

Gold layer converts detailed cleaned data into business oriented datasets



Silver contains detailed records. Gold contains data organized for business questions and reporting



Silver layer data

Order\_ID	City		Product	Amount

101		Bangalore	Laptop	65000

104		Bangalore	Mobile	30000

105		Mysore		Tablet	20000

106		Mysore		Laptop	60000



Revenue generated by each city



Gold Layer data



City		Total Revenue	Total Orders

Bangalore	   95000	     2

Mysore		   80000             2





*from pyspark.sql.functions import sum, count*



*gold\_df = silver\_df.groupBy("City").agg(*

&#x20;   *sum("Amount").alias("Total\_Revenue"),*

&#x20;   *count("Order\_ID").alias("Total\_Orders")*

*)*



*gold\_df.show()*





**Layer 7 - Data Serving Layer**

\-----------------------------

Gold Dataset



Date\_ID		Product\_ID	Customer\_ID	Revenue

20261120	P101		C001		65000

20261120	P102		C004		30000



1. Provides fast access to analytical dataset
2. Separates data storage from business consumption
3. Support controlled access for different dept
4. Provides structed tables and SQL interfaces
5. Reduces the complexity of querying raw data







**Layer 8 - Data Consumption Layer**

\--------------------------------

This is the layer where processed data is actually used



There are four major consumption areas



1. Business Intelligence and Reporting

&#x09;Tools

&#x09;-----

&#x09;a. Microsoft Power BI

&#x09;b. Tableau

&#x09;c. Looker

&#x09;d. Qlik





E-commerce Sales Dashboard

\--------------------------

Total revenue			Total orders

₹1.75 Cr			42,500





Average order value		Return rate

₹4,118				4.8%





2\. Machine Learning and AI



The Gold customer dataset might contain:



Customer\_ID	Orders	Days\_Since\_Last\_Order	Complaints	Churned

C001		 15	    5			  0		No

C002		 2	   120			  5		Yes

C003		 10	   15			  1		No

&#x09;



3\. Applications and API's



we can think of applications using REST API to extract necessary data from the dataset



4\. Ad hoc analysis



This layer converts engineering work into business value



Better business decisions

Faster Reporting

Customer behaviour analysis

Fraud detection

Sales forecasting

Inventory optimization

Personalized recommendation



**Layer 9 - Orchestration Layer**

\------------------------------



Orchestration is the process of coordinating and scheduling different data engineering tasks in the correct order



Consider our ecommerce pipeline that needs to perform these operations



1. Extract orders from the database
2. Load orders into Broze
3. Clean the data using pyspark
4. Store the cleaned data in Silver
5. Generate Business aggregates
6. Store it in Gold dataset
7. Refresh the power BI dataset



Example workflow



start 12:00 AM  ->   Extract Order data   ->   Load Bronze data    ->  Run PySpark



cleaning   ->   Data quality passed ?  -> (yes) Load Silver data -> (no) Send failure alert





(yes) -> Generate Gold aggregates   ->   Load Data Warehouse    ->   Refresh Power BI



\->   send the completion notification



Technologies used

\-----------------

1. Apache Airflow
2. Dagster
3. Perfect
4. AWS Step\_Functions
5. Azure Data Factory



Orchestration helps in



1. Automates repetitive pipelines
2. Control execution order
3. Manage task dependencies
4. Supports retries and failure handling
5. Provides execution history
6. Helps Schedule daily hourly or continuous workflows



PySpark processes the data, Airflow coordinates when and how the processing task run.







**Layer 10 - Data Governance, Security, Quality and Monitoring**

\------------------------------------------------------------

A. Data governance



Data Governance defines how data should be managed and used throughout the organization.



It establishes:

\- Who owns the data?

\- Who is allowed to access it?

\- What does each column mean?

\- Which data is confidential?

\- How long should the data be retained?

\- Which system is the authoritative source?



B. Data Quality



Common checks:

**Check		Example**

Completeness	Customer ID cannot be NULL

Uniqueness	Order ID must be unique

Validity	Amount must be positive for a normal sale

Consistency	City names must follow a standard

Accuracy	Order amount should match the source transaction

Timeliness	Daily orders should arrive before the reporting deadline



C. Data Security



D. Metadata Management and Data Catalog



E. Data Lineage





PySpark

\-------

What is pyspark?



Python ->   Pyspark  ->  Apache Spark



How does pyspark work

\--------------------

Each order contains:

Order ID	Customer	Product		City		Amount

1001		C101		Laptop		Bangalore	55000

1002		C102		Mobile		Chennai		25000

1003		C103		TV		Hyderabad	40000

1004		C101		Mobile		Bangalore	20000

1005		C104		Laptop		Mumbai		60000









## PySpark Architecture





Spark Architecture







Pyspark Architecture





























easewithdata/pyspark-jupyter-lab-old









setup Pyspark

\--------------



Docker desktop



PySpark 3.3.0 with Jupyter Lab, Python 3.10



docker pull easewithdata/pyspark-jupyter-lab:latest





Ports

\-----

4040 - spark UI



8888 - Jupyter Notebook



token=1ed750ef4f6cce5fb84e437bb4eeb055a3e43a1a9343efed

token=

