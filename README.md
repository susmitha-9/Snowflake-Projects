# Snowflake Scripts, SCD1 and SCD2 Implementation:-

**ETL SCD1 using Snowflake Tasks, Streams, and Stored Procedures**

**Slowly Changing Dimension(SCD) Type1:** Slowly Changing Dimensions are dimensions (like Customer, Product, Employee) whose attributes change slowly over time, not every day like transactions. 
Example:
1. A customer changes their email address.
2. A product gets a new description.
3. An employee updates their job title.
SCD Type 1 simply overwrites the old data with the new data. There is no history kept. Only the latest value is stored. So if something changes, the old value is lost forever.

**Scenario:** 
Source data (customer table)
Customer_id  Name  Email
1  John Doe  john@example.com

Target data (customer table)
Customer_id  Name  Email
1  John Doe  john@example.com

John updates his email to john.new@example.com.

Source data (customer table)
Customer_id  Name  Email
1  John Doe  john.new@example.com

Target data (customer table)
Customer_id  Name  Email
1  John Doe  john.new@example.com

**Architecture:**
<img width="1040" height="396" alt="image" src="https://github.com/user-attachments/assets/401723a6-96a2-4fec-8c61-3606dc858a87" />

**Working:**
Data Upload: A Python script (running in an Anaconda notebook) takes the downloaded full extracts and incremental change files from local machine. The script uploads these files to a designated S3 bucket.
AWS S3 and Snowflake are connected via Storage Integration: secure way for Snowflake to read files from S3.
External Stage: defines the S3 location inside Snowflake.
Automated Data Load with Snowpipe: Snowpipe continuously monitors the S3 stage. When new files arrive (full or CDC), Snowpipe automatically copies the data into a Customer Source table (CUSTOMER_SOURCE).

✅ Benefit: Fully automated, near real-time ingestion without manual loading.
Change Processing with Streams: A Stream is defined on the CUSTOMER_SOURCE table. The Stream tracks new or changed rows that were loaded by Snowpipe.

✅ Benefit: Snowflake knows exactly which rows are new or updated, making incremental processing simple.
Automated Processing with Tasks & Stored Procedures: A Task runs on a schedule (or continuously) and calls a Stored Procedure. The Stored Procedure reads the change data from the Stream. Merges it into the target CUSTOMER table using SCD Type 1 logic: If the customer ID exists → update the changed attributes (e.g., overwrite address or email). 
If the customer ID is new → insert it.
This is typically done using a MERGE statement.

✅ Benefit: The entire SCD1 flow is automated — no manual steps to merge data.
Automated full & incremental loads: Zero manual file handling — everything runs on Snowflake side once data lands in S3. No duplicate work — Stream ensures only new/changed rows are processed SCD1 logic guarantees the CUSTOMER table always has the latest values — old data is overwritten as required.

**Slowly Changing Dimension(SCD) Type2:** 
For example: source data customer details such as customer ID, name, address, phone, and email. To move this data from the source system to the target system, will move as it is present in the source, along with that, will insert three new columns inside the target table, which is effective start date, effective end date, and is active. Maintaining the history of all changes on a particular primary key record and identifying the active record with an active flag.
**Scenario:**  
Update the name of the customer ID 
When the source data is sent again to the target, the data warehouse logic is going to create one more entry for the same customer ID, closing the previous active record from 1 to 0, and then a new record is inserted in the target table making it active. For the customer ID, both customer names are stored with one record being inactive and another active.


**Architecture**
<img width="662" height="328" alt="image" src="https://github.com/user-attachments/assets/66b825b6-152f-4ae3-a637-ae6218cc1e08" />

Set up an S3 Bucket in AWS Console to upload product data, Python code running on Visual Studio Code IDE on the local machine, and integration with Snowflake using an External Stage. 
DBT along with Snowflake integration: DBT functionalities to move this data to different layers like bronze, silver, and gold. Macro inside A DBT, which is going to copy the data from your external stage to your copy table, which is nothing but your bronze layer. Once the data is in the bronze layer, a silver layer model, which is the transformation table to perform data movement from bronze to silver. And along with that, will perform the SCD Type 2 logic by using the DBT snapshot feature. Once the DBT snapshot is performed on the silver layer, will create a view which is going to be the gold layer.
Snapshot uses two functionalities: One is the timestamp functionality, another is the check functionality. In timestamp functionality, we need to provide the configuration at which date column we want to track the changes happening in the source table. To identify the changes into source data. The second option is the check columns. So will define a unique key as part of the configuration, and we will define all the non-primary key columns. So based on each unique key, it'll compare if there is any change happen on any of the non-primary key columns. Whatever we have given as part of check columns, if there is a change between from source and whatever already exists in the target table, then DBT will create a new record.


