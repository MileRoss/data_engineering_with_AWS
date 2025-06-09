# Project: Data Pipelines with Airflow


## Purpose
* To demonstrate and practice my skills in Apache Airflow by building data pipelines to AWS platform.  
This included creating custom operators to execute essential functions like staging data, populating a data warehouse, and validating data through the pipeline.



## Implementation


### Tools
	- Local machine: MacBookAir, M1, 2020, Chip: Apple M1, Memory: 8 GB, macOS: Sequoia 15.4.1.
	- Google Chrome: Version 136.0.7103.93 (Official Build) (arm64)
	- Terminal: Version 2.14 (455.1)
	- Apache Airflow 2.8.1 via Docker
	- AWS console: IAM, Redshift Serverless



### Datasets
	- AWS S3: US West AWS Region
	- Log data: `s3://udacity-dend/log_data`
	- Song data: `s3://udacity-dend/song_data`

Some data engineering peers complained on the [Knowledge platform](knowledge.udacity.com) that they either had problems (or weren't able to complete) copying the datasets to their S3 buckets, so I decided to not copy the data but use it directly from the source.



### Steps

* Inspected the client-provided [project repository](https://github.com/udacity/cd12380-data-pipelines-with-airflow), which includes the following folders and files:

- [README](https://github.com/udacity/cd12380-data-pipelines-with-airflow?tab=readme-ov-file) – file with requirements and instructions  
- [create_tables.sql](https://github.com/udacity/cd12380-data-pipelines-with-airflow/blob/main/create_tables.sql)  
- [dags](https://github.com/udacity/cd12380-data-pipelines-with-airflow/tree/main/dags)  
  - [final_project.py](https://github.com/udacity/cd12380-data-pipelines-with-airflow/blob/main/dags/final_project.py) – script containing one DAG and nine tasks  
- [plugins](https://github.com/udacity/cd12380-data-pipelines-with-airflow/tree/main/plugins)  
  - [helpers](https://github.com/udacity/cd12380-data-pipelines-with-airflow/tree/main/plugins/helpers)  
    - [sql_queries.py](https://github.com/udacity/cd12380-data-pipelines-with-airflow/blob/main/plugins/helpers/sql_queries.py) – helper class with all necessary SQL transformations  
  - [operators](https://github.com/udacity/cd12380-data-pipelines-with-airflow/tree/main/plugins/operators)  
    - [data_quality.py](https://github.com/udacity/cd12380-data-pipelines-with-airflow/blob/main/plugins/operators/data_quality.py)  
    - [load_dimension.py](https://github.com/udacity/cd12380-data-pipelines-with-airflow/blob/main/plugins/operators/load_dimension.py)  
    - [load_fact.py](https://github.com/udacity/cd12380-data-pipelines-with-airflow/blob/main/plugins/operators/load_fact.py)  
    - [stage_redshift.py](https://github.com/udacity/cd12380-data-pipelines-with-airflow/blob/main/plugins/operators/stage_redshift.py)



#### Create tables
* Connected to my AWS Console with Udacity credentials. 
* Configured my Redshift Serverless data warehouse to be associated with my existing IAM role, and to be publicly available.
* In Query Editor v2 successfuly ran the `create_tables.sql` code, which created 7 tables.



#### Airflow UI
* Initiated the Airflow Web Server via Docker Desktop and Terminal.
* Created 2 Connections: AWS, Redshift.



#### DAG
* Ran the DAG as-is on the Airflow UI. The graph view looks like this:

![Initial DAG](https://github.com/udacity/cd12380-data-pipelines-with-airflow/blob/main/assets/final_project_dag_graph1.png)

* Added `default parameters` on client's request:
	- The DAG does not have dependencies on past runs
	- Catchup is turned off
	- On failure, the tasks are retried 3 times
	- Retries happen every 5 minutes
	- Do not email on retry

* Added `task dependencies` on client's request:

![Working DAG with correct task dependencies](https://github.com/udacity/cd12380-data-pipelines-with-airflow/blob/main/assets/final_project_dag_graph2.png)



#### Operators
* Built four different operators that will: 
	- access my Redshift Serverless credentials and target database from the Airflow UI Connections, 
	- run SQL statements against my Redshift database, 
	- stage the data from S3 bucket to the database, 
	- transform the data, and 
	- run checks on data quality.



##### StageToRedshiftOperator
The stage operator takes both my Redshift and AWS credentials from the Airflow UI and does a COPY of JSON formatted files FROM `udacity-dend` S3 bucket in `us-west-2` region to my Redshift SV database.  
The DAG code uses StageToRedshiftOperator for 2 tasks: `stage_events_to_redshift` and `stage_songs_to_redshift`, to load the staging data into 2 tables: `events` and `songs`.  
For reusability purpose, I set default values for key attributes, however you can override those values in the DAG code.  



##### LoadFactOperator, LoadDimensionOperator
These 2 operators utilize the provided `sql_queries.py` helper class to tranform the data from my 2 staging tables into fact and dimensions tables.  
Additionally, `load_dimension.py` allows switching between insert modes.  
`load_songplays_fact_table` task uses the LoadFactOperator, and requires you to provide the table name and the sql statement name from the `sql_queries.py` helper class.  
4 other tasks use the LoadDimensionOperator, requiring you to define the target table, sql statement, and insert mode.



##### DataQualityOperator
Data quality operator receives one or more SQL based test cases along with the expected results and executes the tests.  
If the test result/s don't match the expected result/s, the operator raises an exception, retries 3 times, and eventually fails.  
`run_data_quality_checks` task uses the DataQualityOperator to check if `songs.songid` column has null values, and it expects none.



## How to run

* clone repo  
git clone https://github.com/MileRoss/project_4_data_pipelines_with_airflow.git  
cd project_4_data_pipelines_with_airflow

* start airflow  
docker-compose up -d

* access Airflow UI  
http://localhost:8080

* login  
user: airflow  
pass: airflow



## Troubleshooting


### Where are clusters in Redshift Serverless
If you do this project as part of Udacity's Data Engineering with AWS Nanodegree, and your primary source of knowledge is their learning material, some lessons are outdated, or a mix of outdated and updated paragraphs, which makes them hard to follow.  
If you find yourself stuck in the lesson 3.12 "Configure AWS Redshift Serverless", struggling to find a cluster in Redshift Serverless that they're showing in the screenshot. The lesson seems to contain parts whic refer to the "old" Redshift, where you had to create a provisioned cluster, etc. In the "new" Redshift Serverless, we no longer do that. We don't create or use clusters.  
The content of the official AWS documentation may be overwhelming in volume and details, but it's your best friend in these situations.



### Redshift vs Redshift Serverless
Udacity gives conflicting advices regarding this choice. While the 3.12 lesson talks about the Redshift Serverless as "a tremendous cost savings. In fact, you should be able to stay within your Udacity credits.", mentors in the Knowledge platform advise against using it.  
As I used the classic Redshift cluster in 2 previous projects, and configuring Redshift Serverless felt simple, it was my choice for this project, and I can't recommend it enough. It is indeed cheaper than the traditional cluster, and at the time of completing this project, I only spent 8$ of my 25$ budget.  
Regardless of your choice, both will do the work. You can use the same Airflow UI Connection if your username and password are same for both Redshifts, but you'd have to swap the endpoint as it determines where your DAG will stage the data to. It's best to just create 2 separate Connections and name then in a distinguishing manner.  


#### Airflow UI: Connection: Redshift
Mind to have the password actually present in the connection! It seems that the password field is not mandatory, and that Connection can be saved with it empty.  
If your password is in the field, you’ll see dots, like ****, 1 dot per character of your password length.  
If the password field appears empty and with no dots, your DAG will fail to connect to Redshift.  



### DAG fails, you checked everything and you don't know why
Maybe some of the starter code is deprecated?  
At the time of this project, Airflow 3 was released, yet some of Udacity's code was deprecated even for the Airflow 2. 2 examples:  
1. One operator that you need for the project, it is deprecated, but may still work or fail:  
from airflow.operators.dummy import DummyOperator  
2. Before the project, in the training material for context templating exercises:  
prev_execution_date, next_execution_date  
These are removed from the airflow 3 and will cause dag errors if not replaced.


## Recommendation
### Do this project on your local machine.  
You’ll have more opportunities to make mistakes that way; those safe mistakes that we can learn from! This learning time is your chance to make mess that won't cost you or your company in € because you're working with sample data.  
If you choose to work in Udacity's integrated workspace, where all the settings and dependencies are handled for you, there may be less stress and "why is the code not working", but you're denying yourself one valuable source of learning.  

### Whatever you decide, good luck, and check my other GitHub repositories for more learning material.