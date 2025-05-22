# Airflow DAGs

## Building a DAG

### Decorators

#### `@dag` decorator
* is an annotation used to mark a function as the definition of a DAG. You can set DAG attributes like: name, description, start date, and interval.

```
import pendulum
import logging
from airflow.decorators import dag

@dag(description='Analyzes Divvy Bikeshare Data',
    start_date=pendulum.now(),
    schedule_interval='@daily')
def divvy_dag():
```

#### `@task` decorator
* is an annotation used to mark a function as a **custom operator**, that generates a task.
* is a modern style code; its legacy alternative are Operators.
```undefined
    @task()
    def hello_world_task():
      logging.info("Hello World")
```

### Operators
define atomic steps of work that make up a DAG. Airflow comes with many Operators that can perform common operations, like:  
* `PythonOperator`
* `PostgresOperator`
* `RedshiftToS3Operator`
* `S3ToRedshiftOperator`
* `BashOperator`
* `SimpleHttpOperator`
* `Sensor`

Instantiated operators are referred to as **Tasks**.
```
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

def hello_world():
    print(“Hello World”)

divvy_dag = DAG(...)
task = PythonOperator(
    task_id=’hello_world’,
    python_callable=hello_world,
    dag=divvy_dag)
```

### Task Dependencies

In Airflow DAGs:
* Nodes = Tasks
* Edges = Ordering and dependencies between tasks

Task dependencies can be described programmatically in Airflow using `>>` and `<<`
* a `>>` b means a comes before b
* a `<<` b means a comes after b

```
hello_world_task = PythonOperator(task_id=’hello_world’, ...)
goodbye_world_task = PythonOperator(task_id=’goodbye_world’, ...)
...
# Use >> to denote that goodbye_world_task depends on hello_world_task
hello_world_task >> goodbye_world_task
```

Tasks dependencies can also be set with “set_downstream” and “set_upstream”
* `a.set_downstream(b)` means a comes before b
* `a.set_upstream(b)` means a comes after b

```
hello_world_task = PythonOperator(task_id=’hello_world’, ...)
goodbye_world_task = PythonOperator(task_id=’goodbye_world’, ...)
...
hello_world_task.set_downstream(goodbye_world_task)
```

### Callables
* passing functions that can be included as arguments to other functions. Examples of callables are map, reduce, filter. This is a pretty powerful feature of python you can explore more on [Python documentation on callables](https://docs.python.org/3/library/functools.html).

### Airflow Hooks

#### Connection via Airflow Hooks

Connections can be accessed in code via hooks. Hooks provide a reusable interface to external systems and databases. With hooks, you don’t have to worry about how and where to store these connection strings and secrets in your code.

```
from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator

def load():
# Create a PostgresHook option using the `demo` connection
    db_hook = PostgresHook(‘demo’)
    df = db_hook.get_pandas_df('SELECT * FROM rides')
    print(f'Successfully used PostgresHook to return {len(df)} records')

load_task = PythonOperator(task_id=’load’, python_callable=hello_world, ...)
```

Airflow comes with many Hooks that can integrate with common systems, like:   
* `HttpHook`
* `PostgresHook` (works with RedShift)
* `MySqlHook`
* `SlackHook`
* `PrestoHook`

### Context Variables

Airflow leverages templating to allow users to “fill in the blank” with important runtime variables for tasks. We use the `**kwargs` parameter to accept the runtime variables in our task.

```
from airflow.decorators import dag, task

@dag(
  schedule_interval="@daily";
)
def template_dag(**kwargs):

  @task
  def hello_date():
    print(f“Hello {kwargs['ds']}}”)
```

(Here)[https://airflow.apache.org/docs/apache-airflow/stable/templates-ref.html] is the Apache Airflow  **Templates reference**

## Tips for Using Airflow's Web UI

* Use Google Chrome to view the Web UI. Airflow sometimes has issues rendering correctly in other browsers.
* Make sure you toggle the DAG to `On` before you try an run it. Otherwise you'll see your DAG running, but it won't ever finish.

### Add Airflow Connections

Here, we'll use Airflow's UI to configure your AWS credentials.

1. Click on the **Admin** tab and select **Connections**.
2. Under **Connections**, click the plus button.
3. On the create connection page, enter the following values:
   * **Connection Id**: Enter `aws_credentials`.
   * **Connection Type**: Enter `Amazon Web Services`.
   * **Login**: Enter your **Access key ID** from the IAM User credentials you downloaded earlier.
   * **Password**: Enter your **Secret access key** from the IAM User credentials you downloaded earlier.
   Once you've entered these values, select **Save**.

> **Note**: The **Access key ID** and **Secret access key** should be taken from the **csv** file you downloaded after creating an IAM User on the page **Create an IAM User in AWS**.  
>

This should connect Airflow to AWS. We will use this connection in the next few demos and exercises.

### Copy S3 Data

The CSV data for the next few exercises is stored in Udacity's S3 bucket. This bucket is in the US West AWS Region. To simplify things, we are going to copy the data to your own bucket, so Redshift can access the bucket.

<br data-md>

Using the AWS Cloudshell, create your own S3 bucket (buckets need to be unique across all AWS accounts): `aws s3 mb s3://sean-murdock/`

<br data-md>

Copy the data from Udacity's bucket to your own bucket: `aws s3 cp s3://udacity-dend/divvy/ s3://sean-murdock/divvy/ --recursive`

<br data-md>

Update  `/udacity/common/sql_statements.py` to use the new bucket. Copy `/udacity/common/sql_statements.py` to the dag directory.

List the data to be sure it copied over: `aws s3 ls s3://sean-murdock/divvy/`

### Connections and Hooks

* Open the Airflow UI and open Admin->Variables
* Click "Create"
* Set `Key` equal to `s3_bucket` and set `Value` equal to **your bucket name**
* Set `Key` equal to `s3_prefix` and set `Value` equal to `data-pipelines`
* Click save
* Run the DAG

### Build the S3 to Redshift DAG

# MetastoreBackend

The `MetastoreBackend` python class connects to the **Airflow Metastore Backend** to retrieve credentials and other data needed to connect to outside systems. 

The below code creates an `aws_connection` object:

* `aws_connection.login` contains the **Access Key ID**
* `aws_connection.password` contains the **Secret Access Key**

**MetastoreBackend Usage**

```
from airflow.decorators import dag
from airflow.secrets.metastore import MetastoreBackend


@dag(
    start_date=pendulum.now()
)
def load_data_to_redshift_dag():

    @task
    def load_task():    
        metastoreBackend = MetastoreBackend()
        aws_connection=metastoreBackend.get_connection("aws_credentials")
        logging.info(vars(aws_connection))

```

**Logging Output**

```
[2022-08-11, 16:16:20 UTC] {l2_e4_s3_to_redshift copy.py:21} INFO - {'_sa_instance_state': <sqlalchemy.orm.state.InstanceState object at 0x7f4342ca2e10>, 'id': 1, 'conn_type': 'aws', 'login': 'AKIA4QE4NTH3R7EBEANN', 'conn_id': 'aws_credentials', '_password': '***'}

```

# PostgresHook
* is a superclass of the Airflow `DbApiHook`. When instantiated, it creates an object containing all the connection details for the Postgres database. It retrieves the details from the Postgres connection you created earlier in the Airflow UI.

<br data-md>

Just pass the connection id that you created in the Airflow UI.

```undefined
from airflow.providers.postgres.operators.postgres import PostgresOperator
. . .
        redshift_hook = PostgresHook("redshift")
```

Call `.run()` on the returned `PostgresHook`object to execute SQL statements.

```
       redhisft_hook.run("SELECT * FROM trips")
```

# PostgresOperator
* class executes sql, and accepts the following parameters:
    * a `postgres_conn_id`
    * a `task_id`
    * the `sql` statement
    * optionally a `dag`

```undefined
from airflow.hooks.postgres_hook import PostgresHook
from airflow.decorators import dag

@dag(
    start_date=pendulum.now()
)
def load_data_to_redshift_dag():


. . .
    create_table_task=PostgresOperator(
        task_id="create_table",
        postgres_conn_id="redshift",
        sql=sql_statements.CREATE_TRIPS_TABLE_SQL
    )
. . .
    create_table_task >> copy_data
```

# Tip:

Notice the `PostgresOperator` **doesn't** have a `dag` parameter in the above example. That is because we used the `@dag` decorator on the dag function.

# Plugins

Airflow was built with the intention of allowing its users to extend and customize its functionality through plugins. The most common types of user-created plugins for Airflow are Operators and Hooks. These plugins make DAGs reusable and simpler to maintain.

To create custom operator, follow the steps:

1. Identify Operators that perform similar functions and can be consolidated
1. Define a new Operator in the plugins folder
1. Replace the original Operators with your new custom one, re-parameterize, and instantiate them.

<a href="https://airflow.apache.org/docs/apache-airflow/stable/howto/custom-operator.html" target="_blank">Here</a> is the Official Airflow Documentation for custom operators

# Task Boundaries
DAG tasks should be:
* Atomic and have a single purpose = perform **only one job.**
* Maximize parallelism
* Make failure states obvious

When revisiting a pipeline you wrote a while ago, it's much easier to understand it if the boundaries between tasks are clear and well defined.  
Tasks that do just one thing are often more easily parallelized. This parallelization can offer a significant speedup in the execution of our DAGs.

# SubDAGs
Commonly repeated series of tasks within DAGs can be captured as reusable SubDAGs. Benefits include:
* Decrease the amount of code we need to write and maintain to create a new DAG
* Easier to understand the high level goals of a DAG
* Bug fixes, speedups, and other enhancements can be made more quickly and distributed to all DAGs that use that SubDAG

## Drawbacks of Using SubDAGs

* Limit the visibility within the Airflow UI
* Abstraction makes understanding what the DAG is doing more difficult
* Encourages premature optimization

### Can Airflow nest subDAGs?
Yes, but have a really good reason to do so because such code is much harder to understand. Generally, subDAGs are not necessary at all, let alone subDAGs within subDAGs.
