# Data_Engineering-
ETL: extract transform load, ETL通常是一个连续的，持续的过程，具有定义明确的工作流。ETL首先从同质或异类数据源中提取数据。然后，对数据进行清洗，丰富，转换和存储，然后再存储到湖泊或数据仓库中

ELT（提取，加载，转换）是ETL的一种变体，其中首先将提取的数据加载到目标系统中。在将数据加载到数据仓库中之后执行转换。当目标系统足够强大时，ELT通常可以很好地工作来处理转换。像Amazon Redshift和Google BigQ这样的分析数据库





hadoop

Hive

Pig

Impala

Spark

Flink

Beam



Cloudera

Hortonworks

AWS Athena

### MapReduce

MapReduce编程技术旨在分析整个集群中的海量数据集。 

MapReduce的灵感来源于函数式语言（比如[Lisp](https://baike.baidu.com/item/Lisp/22083)）中的内置函数map和reduce。函数式语言也算是阳春白雪了，离我们普通开发者总是很远。简单来说，在函数式语言里，map表示对一个列表（List）中的每个元素做计算，[reduce](https://baike.baidu.com/item/reduce)表示对一个列表中的每个元素做迭代计算。它们具体的计算是通过传入的函数来实现的，map和reduce提供的是计算的框架。

简单说来，一个映射函数就是对一些独立元素组成的概念上的列表（例如，一个测试成绩的列表）的每一个元素进行指定的操作（比如前面的例子里，有人发现所有学生的成绩都被高估了一分，它可以定义一个“减一”的映射函数，用来修正这个错误。）。事实上，每个元素都是被独立操作的，而原始列表没有被更改，因为这里创建了一个新的列表来保存新的答案。这就是说，Map操作是可以高度并行的，这对高性能要求的应用以及[并行计算](https://baike.baidu.com/item/并行计算)领域的需求非常有用。

而化简操作指的是对一个列表的元素进行适当的合并（继续看前面的例子，如果有人想知道班级的平均分该怎么做？它可以定义一个化简函数，通过让列表中的元素跟自己的相邻的元素相加的方式把列表减半，如此[递归](https://baike.baidu.com/item/递归)运算直到列表只剩下一个元素，然后用这个元素除以人数，就得到了平均分。）。虽然他不如映射函数那么并行，但是因为化简总是有一个简单的答案，大规模的运算相对独立，所以化简函数在高度并行环境下也很有用。

这样我们就可以把MapReduce理解为，把一堆杂乱无章的数据按照某种特征归纳起来，然后处理并得到最后的结果。Map面对的是杂乱无章的互不相关的数据，它解析每个数据，从中提取出key和value，也就是提取了数据的特征。经过MapReduce的Shuffle阶段之后，在Reduce阶段看到的都是已经归纳好的数据了，在此基础上我们可以做进一步的处理以便得到结果。

### hadoop

 Hadoop MapReduce是编程技术的特定实现。

### HDFS

HDFS（Hadoop Distributed File System）是Hadoop生态系统中的文件系统

### Spark

Hadoop生态系统是一种比Spark生态系统稍老的技术。通常，Hadoop MapReduce比Spark慢，因为Hadoop在中间步骤中将数据写出到磁盘。

#### RDD

RDD是数据的低层抽象。在Spark的第一个版本中，您直接使用RDD。您可以将RDD视为分布在各种计算机上的长列表。尽管数据框架和SQL更容易，但仍可以将RDD用作Spark代码的一部分

#### DAG

DAG，全称 Directed Acyclic Graph， 中文为：有向无环图。在 Spark 中， 使用 DAG 来描述我们的计算逻辑。

DAG 是一组顶点和边的组合。顶点代表了 RDD(弹性分布式数据集)， 边代表了对 RDD 的一系列操作。

DAG Scheduler 会根据 RDD 的 transformation 动作，将 DAG 分为不同的 stage，每个 stage 中分为多个 task，这些 task 可以并行运行。

DAG 的出现主要是为了解决 Hadoop MapReduce 框架的局限性。

主要有两个：

- 每个 MapReduce 操作都是相互独立的，HADOOP不知道接下来会有哪些Map Reduce。
- 每一步的输出结果，都会持久化到硬盘或者 HDFS 上。

在某些迭代的场景下，MapReduce 框架会对硬盘和 HDFS 的读写造成大量浪费。而且每一步都是堵塞在上一步中，所以当我们处理复杂计算时，会需要很长时间，但是数据量却不大

**DAG工作流程**：

1. 解释器是第一层。Spark 通过使用Scala解释器，来解释代码，并会对代码做一些修改。
2. 在Spark控制台中输入代码时，Spark会创建一个 operator graph， 来记录各个操作。
3. 当一个 RDD 的 Action 动作被调用时， Spark 就会把这个 operator graph 提交到 DAG scheduler 上。
4. DAG Scheduler 会把 operator graph 分为各个 stage。 一个 stage 包含基于输入数据分区的task。DAG scheduler 会把各个操作连接在一起。
5. 这些 Stage 将传递给 Task Scheduler。Task Scheduler 通过 cluster manager 启动任务。Stage 任务的依赖关系， task scheduler 是不知道的。
6. 在 slave 机器上的 Worker 们执行 task。

RDD 的 transformation 分为两种：窄依赖（如map、filter），宽依赖（如reduceByKey）。 窄依赖不需要对分区数据进行 shuffle ，而宽依赖需要。所以窄依赖都会在一个 stage 中， 而宽依赖会作为 stage 的交界处。每个 RDD 都维护一个 metadata 来指向一个或多个父节点的指针以及记录有关它与父节点的关系类型。

#### Maps和Lambda，关联数据处理函数

```python
import pyspark
sc = pyspark.SparkContext(appName="maps_and_lazy_evaluation_example")
# config = pyspark.SparkConf().setAppName("name").setMaster("local")
# sc = pyspark.SparkContext(conf=config)
log_of_songs = [
        "Despacito",
        "Nice for what",
        "No tears left to cry",
        "Despacito",
        "Havana",
        "In my feelings",
        "Nice for what",
        "despacito",
        "All the stars"
]
distributed_song_log = sc.parallelize(log_of_songs)
def convert_song_to_lowercase(song):
    return song.lower()
distributed_song_log.map(convert_song_to_lowercase).collect()
#or
distributed_song_log.map(lambda x: x.lower()).collect()
```

```python
import pyspark
from pyspark import SparkConf
from pyspark.sql import SparkSession
spark = SparkSession \
    .builder \
    .appName("Our first Python Spark SQL example") \
    .getOrCreate()
spark.sparkContext.getConf().getAll()
path = "data/sparkify_log_small.json"
user_log = spark.read.json(path)
user_log.printSchema()
user_log.describe()
user_log.show(n=1)
user_log.take(5)
out_path = "data/sparkify_log_small.csv"
user_log.write.save(out_path, format="csv", header=True)
user_log_2 = spark.read.csv(out_path, header=True)
user_log_2.printSchema()
user_log_2.take(2)
user_log_2.select("userID").show()
user_log_2.take(1)
```

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import desc
from pyspark.sql.functions import asc
from pyspark.sql.functions import sum as Fsum
import datetime
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from pyspark.sql import Window

spark = SparkSession \
    .builder \
    .appName("Wrangling Data") \
    .getOrCreate()
path = "data/sparkify_log_small.json"
user_log = spark.read.json(path)
user_log.take(5)
user_log.printSchema()
user_log.describe().show()
user_log.describe("artist").show()
user_log.describe("sessionId").show()
user_log.select("page").dropDuplicates().sort("page").show()
user_log.select(["userId", "firstname", "page", "song"]).where(user_log.userId == "1046").collect()
#Calculating Statistics by Hour
get_hour = udf(lambda x: datetime.datetime.fromtimestamp(x / 1000.0). hour)
user_log = user_log.withColumn("hour", get_hour(user_log.ts))
user_log.head()
songs_in_hour = user_log.filter(user_log.page == "NextSong").groupby(user_log.hour).count().orderBy(user_log.hour.cast("float"))
songs_in_hour.show()
songs_in_hour_pd = songs_in_hour.toPandas()
songs_in_hour_pd.hour = pd.to_numeric(songs_in_hour_pd.hour)
plt.scatter(songs_in_hour_pd["hour"], songs_in_hour_pd["count"])
plt.xlim(-1, 24);
plt.ylim(0, 1.2 * max(songs_in_hour_pd["count"]))
plt.xlabel("Hour")
plt.ylabel("Songs played");
#Drop Rows with Missing Values
user_log_valid = user_log.dropna(how = "any", subset = ["userId", "sessionId"])
user_log_valid.count()
user_log.select("userId").dropDuplicates().sort("userId").show()
user_log_valid = user_log_valid.filter(user_log_valid["userId"] != "")
user_log_valid.count()
#Users Downgrade Their Accounts
user_log_valid.filter("page = 'Submit Downgrade'").show()
user_log.select(["userId", "firstname", "page", "level", "song"]).where(user_log.userId == "1138").collect()
flag_downgrade_event = udf(lambda x: 1 if x == "Submit Downgrade" else 0, IntegerType())
user_log_valid = user_log_valid.withColumn("downgraded", flag_downgrade_event("page"))
user_log_valid.head()
windowval = Window.partitionBy("userId").orderBy(desc("ts")).rangeBetween(Window.unboundedPreceding, 0)
user_log_valid = user_log_valid.withColumn("phase", Fsum("downgraded").over(windowval))
user_log_valid.select(["userId", "firstname", "ts", "page", "level", "phase"]).where(user_log.userId == "1138").sort("ts").collect()
```

```python
songplays_table = songplays_table\
    .join(time_table, songplays_table.start_time == time_table.start_time, how='inner') \
    .select('songplay_id', songplays_table.start_time, 'user_id', 'level', 'song_id', \
    'artist_id', 'session_id', 'location', 'user_agent', songplays_table.year, songplays_table.month)
songplays_table = df.join(song_df, df.song == song_df.title, how='inner')\                   .select(monotonically_increasing_id().alias('songplay_id'), \                                                              col('start_time').alias('start_time'),\                                                                col('userId').alias('user_id'),\                                                                'level', \                                                               'song_id', \                                                               'artist_id', \                                                                col('sessionId').alias('session_id'), \                                                               'location', \                                                                col('userAgent').alias('user_agent'))
```



#### spark context和spark session关系

SparkSession是Spark 2.0引如的新概念。SparkSession为用户提供了统一的切入点，来让用户学习spark的各项功能。
 在spark的早期版本中，SparkContext是spark的主要切入点，由于RDD是主要的API，我们通过sparkcontext来创建和操作RDD。对于每个其他的API，我们需要使用不同的context。例如，对于Streaming，我们需要使用StreamingContext；对于sql，使用sqlContext；对于Hive，使用hiveContext。但是随着DataSet和DataFrame的API逐渐成为标准的API，就需要为他们建立接入点。所以在spark2.0中，引入SparkSession作为DataSet和DataFrame API的切入点，SparkSession封装了SparkConf、SparkContext和SQLContext。为了向后兼容，SQLContext和HiveContext也被保存下来。
SparkSession实质上是SQLContext和HiveContext的组合

#### DataFrames操作

- `select()`：返回具有选定列的新DataFrame
- `filter()`：使用给定条件过滤行
- `where()`：只是它的别名 `filter()`
- `groupBy()`：使用指定的列对DataFrame进行分组，因此我们可以对它们进行聚合
- `sort()`：返回按指定列排序的新DataFrame。默认情况下，第二个参数“升序”为True。
- `dropDuplicates()`：返回一个具有基于所有列或仅列的子集的唯一行的新DataFrame
- `withColumn()`：通过添加列或替换具有相同名称的现有列来返回新的DataFrame。第一个参数是新列的名称，第二个参数是如何计算它的表达式。
- `agg({"salary": "avg", "age": "max"})`计算平均工资和最大年龄。
- col("numWords")  选择一列

spark SQL提供了内置的方法最常见的聚合，例如`count()`，`countDistinct()`，`avg()`，`max()`，`min()`，等在pyspark.sql.functions模块

在Spark SQL中，我们可以使用pyspark.sql.functions模块中的udf方法**定义自己的函数**。UDF返回的变量的默认类型为字符串。如果我们想返回其他类型，则需要使用pyspark.sql.types模块中的不同类型来显式地返回。

**窗口函数**是一种组合DataFrame中行范围值的方法。在定义窗口时，我们可以选择如何（使用`partitionBy`方法）对行进行排序和分组以及我们要使用的窗口宽度（由`rangeBetween`或描述`rowsBetween`）

#### spark SQL

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import desc
from pyspark.sql.functions import asc
from pyspark.sql.functions import sum as Fsum
import datetime
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

spark = SparkSession \
    .builder \
    .appName("Data wrangling with Spark SQL") \
    .getOrCreate()
path = "data/sparkify_log_small.json"
user_log = spark.read.json(path)
user_log.take(1)
user_log.printSchema()
user_log.createOrReplaceTempView("user_log_table") #creates a temporary view against which you can run SQL queries
spark.sql("SELECT * FROM user_log_table LIMIT 2").show()
spark.sql('''
          SELECT * 
          FROM user_log_table 
          LIMIT 2
          '''
          ).show()
spark.sql('''
          SELECT COUNT(*) 
          FROM user_log_table 
          '''
          ).show()
spark.sql('''
          SELECT userID, firstname, page, song
          FROM user_log_table 
          WHERE userID == '1046'
          '''
          ).collect()
spark.sql('''
          SELECT DISTINCT page
          FROM user_log_table 
          ORDER BY page ASC
          '''
          ).show()
#User Defined Functions
spark.udf.register("get_hour", lambda x: int(datetime.datetime.fromtimestamp(x / 1000.0).hour))
spark.sql('''
          SELECT *, get_hour(ts) AS hour
          FROM user_log_table 
          LIMIT 1
          '''
          ).collect()
songs_in_hour = spark.sql('''
          SELECT get_hour(ts) AS hour, COUNT(*) as plays_per_hour
          FROM user_log_table
          WHERE page = "NextSong"
          GROUP BY hour
          ORDER BY cast(hour as int) ASC
          '''
          )
songs_in_hour.show()
# Converting Results to Pandas
songs_in_hour_pd = songs_in_hour.toPandas()
print(songs_in_hour_pd)
```

#### 从s3读取数据

**sqlContext.jsonFile(“/path/to/myDir”)** is deprecated from spark 1.6 instead use **spark.read.json(“/path/to/myDir”) or spark.read.format(“json”).load(“/path/to/myDir”)**

### 数据湖

数据湖概念的诞生，源自企业面临的一些挑战，如数据应该以何种方式处理和存储。最开始，企业对种类庞杂的应用程序的管理都经历了一个比较自然的演化周期。

最开始的时候，每个应用程序会产生、存储大量数据，而这些数据并不能被其他应用程序使用，这种状况导致**数据孤岛**的产生。随后数据集市应运而生，应用程序产生的数据存储在一个集中式的数据仓库中，可根据需要导出相关数据传输给企业内需要该数据的部门或个人

**然而数据集市只解决了部分问题。**剩余问题，包括数据管理、数据所有权与访问控制等都亟须解决，因为企业寻求获得更高的使用有效数据的能力。

为了解决前面提及的各种问题，**企业有很强烈的诉求搭建自己的数据湖**，数据湖不但能存储传统类型数据，也能存储任意其他类型数据，并且能在它们之上做进一步的处理与分析，产生最终输出供各类程序消费。

**数据湖是一个存储企业的各种各样原始数据的大型仓库，其中的数据可供存取、处理、分析及传输。**

### kafka

Apache Kafka是使用Scala和Java编写的Apache软件基金会的**开源流处理软件平台**。

Kafka是一种高吞吐量的[分布式](https://baike.baidu.com/item/分布式/19276232)发布订阅消息系统，它可以处理消费者在网站中的所有动作流数据。 这种动作（网页浏览，搜索和其他用户的行动）是在现代网络上的许多社会功能的一个关键因素。

Kafka的目的是通过[Hadoop](https://baike.baidu.com/item/Hadoop)的并行加载机制来统一线上和离线的消息处理，也是为了通过[集群](https://baike.baidu.com/item/集群/5486962)来提供实时的消息。

### 数据管道

data pipeline:是处理数据的一系列步骤

### DAGs（Directed Acyclic Graphs）

- **有向无环图（DAG）：** DAG是图的特殊子集，其中节点之间的边具有特定方向，并且不存在循环。当我们说“不存在周期”时，我们的意思是节点无法创建返回自己的路径。
- **节点：**数据管道流程中的一个步骤。
- **边缘：**节点之间的依赖关系或其他关系。

### Airflow

- airflow是一款开源的，分布式任务调度框架，它将一个具有上下级依赖关系的工作流，组装成一个有向无环图。
- 特点:
- 分布式任务调度：允许一个工作流的task在多台worker上同时执行
- 可构建任务依赖：以有向无环图的方式构建任务依赖关系
- task原子性：工作流上每个task都是原子可重试的，一个工作流某个环节的task失败可自动或手动进行重试，不必从头开始任务



#### Airflow DAG的操作顺序

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
    schedule_interval='@daily')  # 时间间隔

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

#### [**context variables**](https://airflow.apache.org/macros.html) 

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

#### 使用hook连接到s3示例

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



#### s3到redshift示例

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



