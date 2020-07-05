# Data_Engineering-
Data Engineering  learning notes



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

