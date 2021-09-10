# airflow_data_pipeline_project

### Introduction
A music streaming company, Sparkify, has decided using Airflow automate and monitor data warehouse ETL pipelines.

To create high grade data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills. It's also noted that the data quality plays a big part when analyses are executed on top the data warehouse and want to run tests against their datasets after the ETL steps have been executed to catch any discrepancies in the datasets.

In this project, source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.

### Project Goal

This project will demonstrate how to create custom operators to perform tasks such as staging the data, filling the data warehouse, and running checks on the data as the final step.

### ETL work flow

![dag lineage](image/dag_lineage.png)


### Data Structure 
Staging Tables <br>
![tables](image/staging_tables.png)

Data warehouse <br>
![tables](image/Erd_dwh.png)


