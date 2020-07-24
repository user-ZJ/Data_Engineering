# DAGs（Directed Acyclic Graphs）

- **有向无环图（DAG）：** DAG是图的特殊子集，其中节点之间的边具有特定方向，并且不存在循环。当我们说“不存在周期”时，我们的意思是节点无法创建返回自己的路径。
- **节点：**数据管道流程中的一个步骤。
- **边缘：**节点之间的依赖关系或其他关系。

# Airflow

- airflow是一款开源的，分布式任务调度框架，它将一个具有上下级依赖关系的工作流，组装成一个有向无环图。
- 特点:
- 分布式任务调度：允许一个工作流的task在多台worker上同时执行
- 可构建任务依赖：以有向无环图的方式构建任务依赖关系
- task原子性：工作流上每个task都是原子可重试的，一个工作流某个环节的task失败可自动或手动进行重试，不必从头开始任务



## Airflow DAG的操作顺序

- Airflow Scheduler根据时间或外部触发器启动DAG。
- 启动DAG后，调度程序将查看DAG中的步骤，并通过查看其依赖关系来确定哪些步骤可以运行。
- 调度程序将可运行的步骤放入队列中。
- worker承担这些任务并运行它们。
- worker完成该步骤后，将记录任务的最终状态，并由调度程序放置其他任务，直到完成所有任务。
- 完成所有任务后，DAG将完成。

DAG 5大组件：scheduler, workers, web server, queue, and database.

Airflow UI是用户和维护者的控制界面，允许他们执行和监视DAG

```python
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
divvy_dag = DAG(
    'divvy',
    description='Analyzes Divvy Bikeshare Data',
    #start_date=datetime(2019, 2, 4),  #开始日期
    start_date=datetime.datetime.now() - datetime.timedelta(days=2),
    end_date=datetime(2019, 5, 4),
    schedule_interval='@daily',# 时间间隔
    max_active_runs=1    #最多同时执行1个flow
)  

def hello_world():
    print(“Hello World”)
    
def goodbye_world():
    print(“Goodbye”)

hello_world_task  = PythonOperator(
    task_id=’hello_world_task’,
    python_callable=hello_world,
    dag=divvy_dag)
goodbye_world_task  = PythonOperator(
    task_id=’goodbye_world_task’,
    python_callable=goodbye_world,
    dag=divvy_dag)
#hello_world_task >> goodbye_world_task
#或
hello_world_task.set_downstream(goodbye_world_task)
# 时间表是可选的，可以使用cron字符串或“ariflow预设”定义。气流提供以下预设：
# @once -一次运行DAG，然后不再运行
# @hourly -每小时运行DAG
# @daily -每天运行DAG
# @weekly -每周运行DAG
# @monthly -每月运行DAG
# @yearly-每年运行DAG
# None -仅在用户启动DAG时运行它
# 开始日期：如果开始日期是过去的日期，则Airflow将运行DAG的次数要多于该开始日期和当前日期之间的计划间隔。
# 结束日期：除非您指定可选的结束日期，否则Airflow将继续运行DAG，直到您禁用或删除DAG。
```

airflow支持的operator:

- `PythonOperator`
- `PostgresOperator`
- `RedshiftToS3Operator`
- `S3ToRedshiftOperator`
- `BashOperator`
- `SimpleHttpOperator`
- `Sensor`

task之前的依赖关系表示：

- a `>>`b表示a在b之前
- a `<<`b表示a在b之后
- `a.set_downstream(b)` 表示a在b之前
- `a.set_upstream(b)` 表示a在b之后

airflow支持的hook,使用hook，您不必担心如何以及在何处将这些连接字符串和机密存储在代码中。

- `HttpHook`
- `PostgresHook` (works with RedShift)
- `MySqlHook`
- `SlackHook`
- `PrestoHook`

```python
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

## [**context variables**](https://airflow.apache.org/macros.html) 

airflow提供了task运行上下文变量，可以在task中打印变量值，记录到日志中

```python
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

def hello_date(*args, **kwargs):
    print(f“Hello {kwargs[‘execution_date’]}”) #运行的时间点

divvy_dag = DAG(...)
task = PythonOperator(
    task_id=’hello_date’,
    python_callable=hello_date,
    provide_context=True,
    dag=divvy_dag)
```

## 使用hook连接到s3示例

```python
# We're going to create a connection and a variable.
# 1. Open your browser to localhost:8080 and open Admin->Variables
# 2. Click "Create"
# 3. Set "Key" equal to "s3_bucket" and set "Val" equal to "udacity-dend"
# 4. Set "Key" equal to "s3_prefix" and set "Val" equal to "data-pipelines"
# 5. Click save
# 6. Open Admin->Connections
# 7. Click "Create"
# 8. Set "Conn Id" to "aws_credentials", "Conn Type" to "Amazon Web Services"
# 9. Set "Login" to your aws_access_key_id and "Password" to your aws_secret_key
# 10. Click save
# 11. Run the DAG
import datetime
import logging
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.S3_hook import S3Hook

def list_keys():
    hook = S3Hook(aws_conn_id='aws_credentials')
    bucket = Variable.get('s3_bucket')
    prefix = Variable.get('s3_prefix')
    logging.info(f"Listing Keys from {bucket}/{prefix}")
    keys = hook.list_keys(bucket, prefix=prefix)
    for key in keys:
        logging.info(f"- s3://{bucket}/{key}")

dag = DAG(
        'lesson1.exercise4',
        start_date=datetime.datetime.now())

list_task = PythonOperator(
    task_id="list_keys",
    python_callable=list_keys,
    dag=dag
)
```



## s3到redshift示例

创建redshift连接；admin->connections->create

Conn Id: redshift

Conn Type:Postgres

Host:redshift的url

Schema:数据库名

Login:数据库用户名

Password:数据库密码

Port：数据库端口

```python
import datetime
import logging
from airflow import DAG
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
import sql_statements

def load_data_to_redshift(*args, **kwargs):
    aws_hook = AwsHook("aws_credentials")
    credentials = aws_hook.get_credentials()
    redshift_hook = PostgresHook("redshift")
    redshift_hook.run(sql_statements.COPY_ALL_TRIPS_SQL.format(credentials.access_key, credentials.secret_key))

dag = DAG(
    'lesson1.solution6',
    start_date=datetime.datetime.now()
)

create_table = PostgresOperator(
    task_id="create_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql=sql_statements.CREATE_TRIPS_TABLE_SQL
)

copy_task = PythonOperator(
    task_id='load_from_s3_to_redshift',
    dag=dag,
    python_callable=load_data_to_redshift
)

location_traffic_task = PostgresOperator(
    task_id="calculate_location_traffic",
    dag=dag,
    postgres_conn_id="redshift",
    sql=sql_statements.LOCATION_TRAFFIC_SQL
)

create_table >> copy_task
copy_task >> location_traffic_task
```

```python
# sql_statements.py
CREATE_TRIPS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS trips (
trip_id INTEGER NOT NULL,
start_time TIMESTAMP NOT NULL,
end_time TIMESTAMP NOT NULL,
bikeid INTEGER NOT NULL,
tripduration DECIMAL(16,2) NOT NULL,
from_station_id INTEGER NOT NULL,
from_station_name VARCHAR(100) NOT NULL,
to_station_id INTEGER NOT NULL,
to_station_name VARCHAR(100) NOT NULL,
usertype VARCHAR(20),
gender VARCHAR(6),
birthyear INTEGER,
PRIMARY KEY(trip_id))
DISTSTYLE ALL;
"""

CREATE_STATIONS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS stations (
id INTEGER NOT NULL,
name VARCHAR(250) NOT NULL,
city VARCHAR(100) NOT NULL,
latitude DECIMAL(9, 6) NOT NULL,
longitude DECIMAL(9, 6) NOT NULL,
dpcapacity INTEGER NOT NULL,
online_date TIMESTAMP NOT NULL,
PRIMARY KEY(id))
DISTSTYLE ALL;
"""

COPY_SQL = """
COPY {}
FROM '{}'
ACCESS_KEY_ID '{{}}'
SECRET_ACCESS_KEY '{{}}'
IGNOREHEADER 1
DELIMITER ','
"""

COPY_MONTHLY_TRIPS_SQL = COPY_SQL.format(
    "trips",
    "s3://udacity-dend/data-pipelines/divvy/partitioned/{year}/{month}/divvy_trips.csv"
)

COPY_ALL_TRIPS_SQL = COPY_SQL.format(
    "trips",
    "s3://udacity-dend/data-pipelines/divvy/unpartitioned/divvy_trips_2018.csv"
)

COPY_STATIONS_SQL = COPY_SQL.format(
    "stations",
    "s3://udacity-dend/data-pipelines/divvy/unpartitioned/divvy_stations_2017.csv"
)

LOCATION_TRAFFIC_SQL = """
BEGIN;
DROP TABLE IF EXISTS station_traffic;
CREATE TABLE station_traffic AS
SELECT
    DISTINCT(t.from_station_id) AS station_id,
    t.from_station_name AS station_name,
    num_departures,
    num_arrivals
FROM trips t
JOIN (
    SELECT
        from_station_id,
        COUNT(from_station_id) AS num_departures
    FROM trips
    GROUP BY from_station_id
) AS fs ON t.from_station_id = fs.from_station_id
JOIN (
    SELECT
        to_station_id,
        COUNT(to_station_id) AS num_arrivals
    FROM trips
    GROUP BY to_station_id
) AS ts ON t.from_station_id = ts.to_station_id
"""

```

## 配置AWS凭证

admin->connections->create

conn id:aws_credentials

conn type:Amazon Web Services

Login:公钥

Password:私钥



copy_task = PythonOperator(
    task_id='load_from_s3_to_redshift',
    dag=dag,
    python_callable=load_data_to_redshift

​	sla=

​	provide_context=True

​	param={

​		'table':'stations'

​	}

)



## 自定义插件

airflow当前支持的插件https://github.com/apache/airflow/tree/master/airflow/contrib

1. 创建继承BaseOperator的类
2. init中传入参数
3. execute方法中实现具体步骤
4. 在_ _init_ _.py中定义继承AirflowPlugin的类

```python
import logging
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class HasRowsOperator(BaseOperator):

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 *args, **kwargs):

        super(HasRowsOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {self.table}")
        if len(records) < 1 or len(records[0]) < 1:
            raise ValueError(f"Data quality check failed. {self.table} returned no results")
        num_records = records[0][0]
        if num_records < 1:
            raise ValueError(f"Data quality check failed. {self.table} contained 0 rows")
        logging.info(f"Data quality on table {self.table} check passed with {records[0][0]} records")
```

```python
from airflow.plugins_manager import AirflowPlugin
import operators

# Defining the plugin class
class MyPlugin(AirflowPlugin):
    name = "my_plugin"
    operators = [
        operators.FactsCalculatorOperator,
        operators.HasRowsOperator,
        operators.S3ToRedshiftOperator
    ]
```





## data Lineage(数据沿袭)

数据集的数据沿袭描述了该数据集的创建，移动和计算所涉及的离散步骤。

### 为什么数据沿袭很重要？

1. **灌输信心：**能够描述特定数据集或分析的数据沿袭将建立对数据消费者（工程师，分析师，数据科学家等）的信心，即我们的数据管道正在使用正确的数据集创建有意义的结果。如果不清楚数据沿袭，则数据使用者将不太可能信任或使用数据。
2. **定义度量标准：**显示数据沿袭的另一个主要好处是，它使组织中的每个人都可以就如何计算特定度量标准的定义达成共识。
3. **调试：**数据沿袭可帮助数据工程师在错误发生时追踪错误的根源。如果对数据移动和转换过程的每个步骤都进行了很好的描述，那么在出现问题时就很容易发现问题。