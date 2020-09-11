- [Hadoop-](https://hadoop.apache.org/)用于大数据存储和数据分析的工具生态系统。Hadoop是一个比Spark更旧的系统，但仍被许多公司使用。Spark和Hadoop之间的主要区别在于它们如何使用内存。Hadoop将中间结果写入磁盘，而Spark尝试尽可能将数据保留在内存中。这使得Spark在许多用例中都更快。
- **Hadoop MapReduce-**一种用于并行处理和分析大型数据集的系统。
- **Hadoop YARN-**资源管理器，可跨集群调度作业。管理器跟踪可用的计算机资源，然后将这些资源分配给特定任务。
- **Hadoop分布式文件系统（HDFS）** -一种大数据存储系统，可将数据拆分为多个块，并将这些块存储在计算机集群中。
- **Apache Pig-**一种类似于SQL的语言，在Hadoop MapReduce上运行
- **Apache Hive-**在Hadoop MapReduce上运行的另一个类似SQL的界面
- EC2 : Elastic Compute Cloud
- EMR: Elastic MapReduce是AWS提供的一项服务，它使您（用户）无需手动安装每台机器的Spark及其依赖项
- M family: multipurpose family 多用途组合
- R family:optimized for Ram
- C family:optimized for CPU

# Spark与Hadoop有何关系？

本课程的重点是Spark，它是另一个大数据框架。Spark包含用于数据分析，机器学习，图形分析和实时数据流的库。Spark通常比Hadoop快。这是因为Hadoop将中间结果写入磁盘，而Spark尝试将中间结果尽可能保留在内存中。

Hadoop生态系统包括一个称为HDFS（Hadoop分布式文件系统）的分布式文件存储系统。另一方面，Spark不包含文件存储系统。您可以在HDFS之上使用Spark，但不必这样做。Spark可以从其他来源（例如[Amazon S3）](https://aws.amazon.com/s3/)读取数据。

Spark具有一个称为[Spark Streaming](https://spark.apache.org/docs/latest/streaming-programming-guide.html)的流媒体库，尽管它不像其他某些流媒体库那样流行和快速。其他流行的流媒体库包括[Storm](http://storm.apache.org/)和 [Flink](https://flink.apache.org/)。本课程不会涉及流媒体，但是您可以通过以下链接来了解有关这些技术的更多信息。





MapReduce是一种用于处理大型数据集的编程技术。" Hadoop MapReduce”是此编程技术的特定实现。

该技术的工作原理是首先分割大型数据集，然后将数据分布在整个群集中。在映射步骤中，将分析每个数据并将其转换为（键，值）对。然后，将这些键值对在整个群集中混排，以便所有键都在同一台计算机上。在reduce步骤中，将具有相同键的值组合在一起。

尽管Spark没有实现MapReduce，但是您可以编写行为与map-reduce范例相似的Spark程序。



spark适用场景 ：

- [数据分析](http://spark.apache.org/sql/)
- [机器学习](http://spark.apache.org/mllib/)
- [流媒体](http://spark.apache.org/streaming/)
- [图分析](http://spark.apache.org/graphx/)

# Spark的局限性

Spark有一些限制。

Spark Streaming的等待时间至少为500毫秒，因为它对记录的微批次进行操作，而不是一次处理一个记录。诸如[Storm](http://storm.apache.org/)，[Apex](https://apex.apache.org/)或[Flink之](https://flink.apache.org/)类的本地流工具可以降低此延迟值，并且可能更适合于低延迟应用程序。Flink和Apex也可以用于批处理计算，因此，如果您已经将它们用于流处理，则无需将Spark添加到您的技术堆栈中。

Spark的另一个限制是它选择了机器学习算法。当前，Spark仅支持与输入数据大小成线性比例的算法。通常，尽管有许多项目将Spark与Tensorflow和其他深度学习工具集成在一起，但深度学习也不可用。





请记住，Spark不是数据存储系统，除了Spark以外，还有许多其他工具可用于处理和分析大型数据集。您可能听说过较新的数据库存储系统，例如[HBase](https://hbase.apache.org/)或[Cassandra](http://cassandra.apache.org/)。还有分布式SQL引擎，例如[Impala](https://impala.apache.org/)和[Presto](https://prestodb.io/)，根据您在Python和SQL方面的经验，其中许多技术都使用您可能已经熟悉的查询语法。



spark DAG(Directed Acyclical Graph) 数据配方

```python
spark = SparkSession \
    .builder \
    .appName("Wrangling Data") \
    .getOrCreate()
```

- `select()`：返回具有选定列的新DataFrame
- `filter()`：使用给定条件过滤行
- `where()`：只是它的别名 `filter()`
- `groupBy()`：使用指定的列对DataFrame进行分组，因此我们可以对它们进行聚合
- `sort()`：返回按指定列排序的新DataFrame。默认情况下，第二个参数"升序”为True。
- `dropDuplicates()`：返回一个具有基于所有列或仅列的子集的唯一行的新DataFrame
- `withColumn()`：通过添加列或替换具有相同名称的现有列来返回新的DataFrame。第一个参数是新列的名称，第二个参数是如何计算它的表达式。
- `agg({"salary": "avg", "age": "max"})`计算平均工资和最大年龄。

spark SQL提供了内置的方法最常见的聚合，例如`count()`，`countDistinct()`，`avg()`，`max()`，`min()`，等在pyspark.sql.functions模块

在Spark SQL中，我们可以使用pyspark.sql.functions模块中的udf方法定义自己的函数。UDF返回的变量的默认类型为字符串。如果我们想返回其他类型，则需要使用pyspark.sql.types模块中的不同类型来显式地返回。

RDD是数据的低层抽象。在Spark的第一个版本中，您直接使用RDD。您可以将RDD视为分布在各种计算机上的长列表。尽管数据框架和SQL更容易，但仍可以将RDD用作Spark代码的一部分。

# spark从S3中读数据

```python
df = spark.read.load("s3://my_bucket/path/to/file/file.csv”)
# 如果我们使用的是spark，并且存储桶下面的所有对象都具有相同的架构，则可以执行以下操作
df = spark.read.load("s3://my_bucket/”)
```



# HDFS和AWS S3之间的区别

- **AWS S3**是一个**对象存储系统**，它使用键值对（即存储区和键）存储数据，而**HDFS**是一种**实际的分布式文件系统**，可以保证容错能力。HDFS通过具有重复因素来实现容错能力，这意味着默认情况下，它将在集群中的3个不同节点上复制相同文件（可以将其配置为不同的重复次数）。
- HDFS通常**安装在本地系统中**，并且传统上让工程师在现场维护和诊断Hadoop生态系统，这**比在云上存储数据要花费更多**。由于**位置**的**灵活性**和**降低的维护成本**，云解决方案变得更加流行。借助您可以在AWS内使用的广泛服务，S3比HDFS更受欢迎。
- 由于**AWS S3是二进制对象存储库**，因此它可以**存储各种格式**，甚至图像和视频。HDFS将严格要求某种文件格式-流行的选择是**avro**和**parquet**，它们具有相对较高的压缩率，这对于存储大型数据集很有用。





* Spark Accumulators
* Spark Broadcast
* spark WebUI

# Spark

Apache Spark是一个在集群上运行的统一计算引擎以及一组并行数据处理软件库

![](images/spark1.png)  

**统一平台**：Spark通过统一计算引擎和利用一套统一的API，支持广泛的数据分析任务，从简单的数据加载，到SQL查询，再到机器学习和流式计算

**计算引擎**:在Spark致力于统一平台的同时，它也专注于计算引擎，Spark从存储系统加载数据并对其执行计算，加载结束时不负责永久存储，也不偏向于使用某一特定的存储系统，主要原因是大多数数据已经存在于混合存储系统
中，而移动这些数据的费用非常高，因此Spark专注于对数据执行计算，而不考虑数据存储于何处

**配套的软件库**：Spark包括SQL和处理结构化数据的库（Spark SQL）、机器学习库（MLlib）、流处理库（Spark Streaming和较新的结构化流式处理），以及图分析（GraphX）的库。除了这些库之外，还有数百种开源外部库，从用于各种存储系统的连接器到机器学习算法。spark-packages.org（https://spark-packages.org/）上提供了一个外部库的索引。



**Spark的基本架构**：Spark管理和协调跨多台计算机的计算任务。Spark用来执行计算任务的若干台机器由像Spark的集群管理器、YARN或Mesos这样的集群管理器管理，然后我们提交Spark应用程序给这些集群管理器，它们将计算资源分配给应用程序，以便完成我们的工作。

**Spark应用程序**：Spark应用程序由一个驱动器进程和一组执行器进程组成。**驱动进程**运行main()函数，位于集群中的一个节点上，它负责三件事：维护Spark应用程序的相关信息；回应用户的程序或输入；分析任务并分发给若干执行器进行处理。**执行器**负责执行驱动器分配给它的实际计算工作，这意味着每个执行器只负责两件事：执行由驱动器分配给它的代码，并将该执行器的计算状态报告给运行驱动器的节点。

![](images/spark2.png)



## SparkSession

```python
from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()
```

## DataFrame

DataFrame是最常见的结构化API，简单来说它是包含行和列的数据表。列和列类型的一些规则被称为**模式**（schema）。与电子表格不同的是：电子表格位于一台计算机上，而Spark DataFrame可以跨越数千台计算机。

我们可以非常容易地将Pandas（Python） DataFrame转换为Spark DataFrame或将R DataFrame转换为Spark DataFrame。

DataFrame的分区定义了DataFrame以及Dataset在集群上的物理分布，而划分模式定义了partition的分配方式，你可以自定义分区的方式，也可以采取随机分配的方式。

Spark中的DataFrame和Dataset代表不可变的数据集合，可以通过它指定对特定位置数据的操作，该操作将以惰性评估方式执行。当对DataFrame执行动作操作时，将触发Spark执行具体转换操作并返回结果，这些代表了如何操纵行和列来计算出用户期望结果的执行计划

```python
# spark is an existing SparkSession
df = spark.read.json("examples/src/main/resources/people.json")
# Displays the content of the DataFrame to stdout
df.show()
df.printSchema()  #表结构
df.select("name").show()
df.select(df['name'], df['age'] + 1).show()
df.filter(df['age'] > 21).show()   
df.groupBy("age").count().show()

# 创建dataframe
training = spark.createDataFrame([
    (0, "a b c d e spark", 1.0),
    (1, "b d", 0.0),
    (2, "spark f g h", 1.0),
    (3, "hadoop mapreduce", 0.0)
], ["id", "text", "label"])

flightData2015 = spark\
.read\
.option("inferSchema", "true")\
.option("header", "true")\
.csv("/data/flight-data/csv/2015-summary.csv")
```

### 模式Schema

Schema定义了DataFrame的列名和类型，可以**手动定义**或者**从数据源读取模式**（通常定义为模式读取）。

一个模式是由许多字段构成的StructType。这些字段即为StructField，具有名称、类型、布尔标志（该标志指定该列是否可以包含缺失值或空值），并且用户可指定与该列关联的元数据（metadata）。元数据存储着有关此列的信息。模式还可以包含其他的StructType（Spark的复杂类型）

如果（在运行时）数据的类型与定义的schema模式不匹配，Spark将抛出一个错误。

```python
from pyspark.sql.types import StructField, StructType, StringType, LongType
myManualSchema = StructType([
StructField("DEST_COUNTRY_NAME", StringType(), True),
StructField("ORIGIN_COUNTRY_NAME", StringType(), True),
StructField("count", LongType(), False, metadata={"hello":"world"})
])
df = spark.read.format("json").schema(myManualSchema)\
    .load("/data/flight-data/json/2015-summary.json")
```

### 列和表达式

```python
from pyspark.sql.functions import col, column
col("someColumnName")
df.col("count")  #获取列内容
from pyspark.sql.functions import expr
expr("(((someCol + 5) * 200) - 6) < otherCol")
```

### 记录和行

在Spark中，DataFrame的每一行都是一个记录，而记录是Row类型的对象。Spark使用列表达式操纵Row类型对象。Row对象内部其实是字节数组，但是Spark没有提供访问这些数组的接口，因此我们只能使用列表达式去操纵。

当使用DataFrame时，向驱动器请求行的命令总是返回一个或多个Row类型的行数据

需要注意的是，只有DataFrame具有模式，行对象本身没有模式，这意味着，如果你手动创建Row对象，则必须按照该行所附属的DataFrame的列顺序来初始化Row对象

```python
# 在DataFrame上调用first()来查看一行
df.first()
from pyspark.sql import Row
myRow = Row("Hello", None, 1, False)
myRow[0]
myRow[2]
```

```python
# 使用私有数据和自定义模式创建dataframe
from pyspark.sql import Row
from pyspark.sql.types import StructField, StructType, StringType, LongType
myManualSchema = StructType([
StructField("some", StringType(), True),
StructField("col", StringType(), True),
StructField("names", LongType(), False)
])
myRow = Row("Hello", None, 1)
myDf = spark.createDataFrame([myRow], myManualSchema)
myDf.show()
```



### 数据类型

| 数据类型      | Python的值类型                                               | 获取或者创建数据类型的API                                    |
| ------------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
| ByteType      | int或long。注意：数字在运行时转换为1字节 <br/>的带符号整数。 确保数字在-128~127的范围内 | ByteType()                                                   |
| ShortType     | int或long。注意：数字在运行时将转换为2字 <br/>节带符号的整数。 确保数字在-32768到<br/>32767的范围内 | ShortType()                                                  |
| IntegerType   | int或long。注意：Python对“整数”有一个<br/>宽松的定义。如果使用IntegerType()，那么太<br/>大的数字将被Spark SQL拒绝。在这种情况下，<br/>最好使用LongType() | IntegerType()                                                |
| LongType      | long。注意：数字在运行时将转换为8字节有符 <br/>号整数。确保数字在-9223372036854775808~<br/>9223372036854775807范围内。否则，请将<br/>数据类型转换为decimal.Decimal，并使用<br/>DecimalType | IntegerType()                                                |
| FloatType     | float型。注意：在运行时，数字将被转换为4 <br/>字节的单精度浮点数 | FloatType()                                                  |
| DoubleType    | float型                                                      | DoubleType()                                                 |
| DecimalType   | decimal.Decimal                                              | DecimalType()                                                |
| StringType    | string                                                       | StringType()                                                 |
| BinaryType    | bytearray                                                    | BinaryType()                                                 |
| BooleanType   | Bool                                                         | BooleanType()                                                |
| TimestampType | datetime.datetime                                            | TimestampType()                                              |
| DateType      | datetime.date                                                | DateType()                                                   |
| ArrayType     | List，tuple或array                                           | ArrayType（elementType，<br/>[containsNull]）。注意：<br/>containsNull的默认值为<br/>True |
| MapType       | 字典                                                         | MapType（keyType，<br/>valueType，<br/>[valueContainsNull]）。<br/>注意：valueContainsNull<br/>的默认值为True |
| StructType    | 列表或元组                                                   | StructType（fields）。注<br/>意： fields是一个包含多<br/>个StructFiled的list，并且<br/>任意两个StructField不能<br/>同名 |
| StructField   | 该字段对应的Python数据类型（例如，int是<br/>IntegerType的StructField） | StructField（name，<br/>dataType，[nullable]）。<br/>注意：nullable指定该<br/>field是否可以为空值，默<br/>认值为True |

### select和selectExpr

Select函数和selectExpr函数支持在DataFrame上执行类似数据表的SQL查询

```python
# SELECT DEST_COUNTRY_NAME FROM dfTable LIMIT 2
df.select("DEST_COUNTRY_NAME").show(2)
# SELECT DEST_COUNTRY_NAME, ORIGIN_COUNTRY_NAME FROM dfTable LIMIT 2
df.select("DEST_COUNTRY_NAME", "ORIGIN_COUNTRY_NAME").show(2)

from pyspark.sql.functions import expr, col, column
df.select(
expr("DEST_COUNTRY_NAME"),
col("DEST_COUNTRY_NAME"),
column("DEST_COUNTRY_NAME"))\
.show(2)
```

expr是我们目前使用到的最灵活的引用方式。它能够引用一列，也可以引用对列进行操纵的字符串表达式。

```python
# SELECT DEST_COUNTRY_NAME as destination FROM dfTable LIMIT 2
df.select(expr("DEST_COUNTRY_NAME AS destination")).show(2)
df.select(expr("DEST_COUNTRY_NAME as destination").alias("DEST_COUNTRY_NAME")).show(2)
```

因为select后跟着一系列expr是非常常见的写法，所以Spark有一个有效地描述此操作序列的接口：selectExpr，它可能是最常用的接口

```python
df.selectExpr("DEST_COUNTRY_NAME as newColumnName", "DEST_COUNTRY_NAME").show(2)
# SELECT *,(DEST_COUNTRY_NAME=ORIGIN_COUNTRY_NAME) as withinCountry FROM dfTable LIMIT 2
df.selectExpr(
"*", # all original columns
"(DEST_COUNTRY_NAME = ORIGIN_COUNTRY_NAME) as withinCountry")\
.show(2)
# SELECT avg(count), count(distinct(DEST_COUNTRY_NAME)) FROM dfTable LIMIT 2
df.selectExpr("avg(count)", "count(distinct(DEST_COUNTRY_NAME))").show(2)
```

### 字面量（literal）

有时候需要给Spark传递显式的值，它们只是一个值而非新列。这可能是一个常量值，或接下来需要比较的值。我们的方式是通过字面量（literal）传递

```python
from pyspark.sql.functions import lit
# SELECT *, 1 as One FROM dfTable LIMIT 2
df.select(expr("*"), lit(1).alias("One")).show(2)
```

### 添加列

```python
# SELECT *, 1 as numberOne FROM dfTable LIMIT 2
df.withColumn("numberOne", lit(1)).show(2)
df.withColumn("withinCountry", expr("ORIGIN_COUNTRY_NAME == DEST_COUNTRY_NAME")).show(2)
# 使用withcolumn重命名列
df.withColumn("Destination", expr("DEST_COUNTRY_NAME")).columns
# 重命名列
df.withColumnRenamed("DEST_COUNTRY_NAME", "dest").columns
```

### 删除列

```python
dfWithLongColName.drop("ORIGIN_COUNTRY_NAME", "DEST_COUNTRY_NAME")
```

### 更改列的类型

```python
df.withColumn("count2", col("count").cast("long"))
```

### 转义符

你可能会遇到列名中包含空格或者连字符等保留字符，要处理这些保留字符意味着要适当地对列名进行转义。在Spark中，我们通过使用反引号（`）字符来实现。

```python
dfWithLongColName.selectExpr(
"`This Long Column-Name`",
"`This Long Column-Name` as `new col`")\
.show(2)
```

### 区分大小写

Spark默认是不区分大小写的，但可以通过如下配置使Spark区分大小写：

```python
# set spark.sql.caseSensitive true
spark.conf.set("spark.sql.caseSensitive", "true")
```

### 过滤操作（where和filter）

where和filter可以执行相同的操作，接受相同参数类型,一般使用where，因为这更像SQL语法

```python
df.filter(col("count") < 2).show(2)
df.where("count < 2").show(2)
```

我们可能本能地想把多个过滤条件放到一个表达式中，尽管这种方式可行，但是并不总有效。因为Spark会同时执行所有过滤操作，不管过滤条件的先后顺序，因此当你想指定多个AND过滤操作时，只要按照先后顺序以链式的方式把这些过滤条件串联起来

```python
# SELECT * FROM dfTable WHERE count < 2 AND ORIGIN_COUNTRY_NAME != "Croatia" LIMIT 2
df.where(col("count") < 2).where(col("ORIGIN_COUNTRY_NAME") != "Croatia")\
.show(2)
```

### 去重(distinct)

是一个转换操作

```python
df.select("ORIGIN_COUNTRY_NAME", "DEST_COUNTRY_NAME").distinct().count()
```

### 随机抽样（sample）

```python
seed = 5
# 指定是否放回抽样， true为有放回的抽样（可以有重复样本），false为无放回的抽样（无重复样本）
withReplacement = False 
fraction = 0.5
df.sample(withReplacement, fraction, seed).count()
```

### 随机分割（randomSplit）

当需要将原始DataFrame随机分割成多个分片时，可以使用随机分割。这通常是在机器学习算法中，用于分割数据集来创建训练集、验证集和测试集

```python
seed = 5
dataFrames = df.randomSplit([0.25, 0.75], seed)
dataFrames[0].count() > dataFrames[1].count()
```

### union操作-连接和追加行

DataFrame是不可变的，这意味着用户不能向DataFrame追加行。如果想要向DataFrame追加行，你必须将原始的DataFrame与新的DataFrame联合起来，即union操作，也就是拼接两个DataFrame。若想联合两个DataFrame，你必须确保它们具有相同的模式和列数，否则联合操作将会失败

```python
from pyspark.sql import Row
schema = df.schema
newRows = [
  Row("New Country", "Other Country", 5L),
  Row("New Country 2", "Other Country 3", 1L)
]
parallelizedRows = spark.sparkContext.parallelize(newRows)
newDF = spark.createDataFrame(parallelizedRows, schema)
df.union(newDF)\
  .where("count = 1")\
  .where(col("ORIGIN_COUNTRY_NAME") != "United States")\
  .show()
```

### 排序

sort和orderBy方法是相互等价的操作，执行的方式也一样。它们均接收列表达式和字符串，以及多个列。默认设置是按升序排序

```python
df.sort("count").show(5)
df.orderBy("count", "DEST_COUNTRY_NAME").show(5)
df.orderBy(col("count"), col("DEST_COUNTRY_NAME")).show(5)
from pyspark.sql.functions import desc, asc
df.orderBy(expr("count desc")).show(2)
df.orderBy(col("count").desc(), col("DEST_COUNTRY_NAME").asc()).show(2)
```

出于性能优化的目的，最好是在进行别的转换之前，先对每个分区进行内部排序。可以使用sortWithinPartitions方法实现这一操作

```python
spark.read.format("json").load("/data/flight-data/json/*-summary.json")\
.sortWithinPartitions("count")
```

asc_nulls_first指示空值安排在升序排列的前面，

desc_nulls_first指示空值安排在降序排列的前面

asc_nulls_last指示空值安排在升序排列的后面

desc_nulls_last指示空值安排在降序排列的后面

### limit

```python
# SELECT * FROM dfTable LIMIT 6
df.limit(5).show()
# SELECT * FROM dfTable ORDER BY count desc LIMIT 6
df.orderBy(expr("count desc")).limit(6).show()
```

### 替换操作

```python
# 第一个参数是替换字典
# 第三个参数表示要替换的列
df_immigration = df_immigration.na.replace(location_dict,1,["bornCountry","residentCountry"])
```



### 重分区和合并

另一个重要的优化是根据一些经常过滤的列对数据进行分区，控制跨群集数据的物理布局，包括分区方案和分区数

不管是否有必要，重新分区都会导致数据的全面洗牌。如果将来的分区数大于当前的分区数，或者当你想要基于某一组特定列来进行分区时，通常只能重新分区

```python
df.rdd.getNumPartitions() #1
df.repartition(5)
# 如果你知道你经常按某一列执行过滤操作，则根据该列进行重新分区是很有必要的
df.repartition(col("DEST_COUNTRY_NAME"))
# 还可以指定你想要的分区数量
df.repartition(5, col("DEST_COUNTRY_NAME"))
```

合并操作（coalesce）不会导致数据的全面洗牌，但会尝试合并分区。

```python
df.repartition(5, col("DEST_COUNTRY_NAME")).coalesce(2)
```

### 驱动器获取行

Spark的驱动器维护着集群状态，有时候你需要让驱动器收集一些数据到本地，这样你可以在本地机器上处理它们。

到目前为止，我们并没有明确定义这个操作。但我们使用了几种不同的方法来实现完全相同的效果。下面的代码示例使用collect函数从整个DataFrame中获取所有数据，使用take函数选择前N行，并使用show函数打印一些行。

```python
collectDF = df.limit(10)
collectDF.take(5) # 获取整数行
collectDF.show() # 更友好的打印
collectDF.show(5, False)
collectDF.collect()
```

为了遍历整个数据集，还有一种让驱动器获取行的方法，即toLocalIterator函数。toLocalIterator函数式一个迭代器，将每个分区的数据返回给驱动器。这个函数允许你以串行的方式一个一个分区地迭代整个数据集

```python
collectDF.toLocalIterator()
```

### 其它方法

alias或contains

### 聚合操作（aggregation）

聚合操作将数据整合到一起，是大数据分析中很常见的基本操作.一般情况下，用户使用聚合操作对数据分组后的各组内的数值型数据进行汇总，这个汇总运算可能是求和、累乘、或者是简单的计数。另外，Spark可以将任何类型的值聚合成为array数组、list列表、或map映射

**group by**   指定一个或多个key也可以指定一个或多个聚合函数，来对包含value的列执行转换操作

**window**    指定一个或多个key也可以指定一个或多个聚合函数，来对包含value的列执行转换操作。但是，输入到该函数的行与当前行有某种联系。

**grouping set**  在多个不同级别进行聚合。grouping set是SQL中的一个保留字，而在DataFrame中需要使用rollup和cube。

**rollup**   指定一个或多个key，也可以指定一个或多个聚合函数，来对包含value的列执行转换操作，并会针对指定的多个key进行分级分组汇总

**cube**  指定一个或多个key，也可以指定一个或多个聚合函数，来对包含value的列执行转换操作，并会针对指定的多个key进行全组合分组汇总。

每个分组操作都会返回RelationalGroupedDataset，基于它来进行聚合操作

**notes**:要考虑的一件重要的事是返回结果的精确度。在进行大数据计算的时候，获得一个精确的结果开销会很大，但是计算出一个近似结果相对要容易得多。本书提到的一些近似函数，通常都会提高Spark作业执行速度和效率，特别是对交互式和ad hoc进行分析

大多数聚合函数可以在org.apache.spark.sql.functions包中找到

```python
df = spark.read.format("csv")\
.option("header", "true")\
.option("inferSchema", "true")\
.load("/data/retail-data/all/*.csv")\
.coalesce(5)
df.cache()
df.createOrReplaceTempView("dfTable")
```

#### 聚合函数

**count**

count聚合操作是一个transformation转换操作而不是一个动作操作。在这种情况下，我们可以执行以下两项操作之一： 第一个是对指定的列进行计数，第二个是使用count(*)或count(1)对所有列进行计数

关于对null值进行计数有一些注意的地方。例如，当执行count(*)时，Spark会对null值进行计数，而当对某指定列计数时，则不会对null值进行计数

```python
from pyspark.sql.functions import count
df.select(count("StockCode")).show() 
```

**countDistinct**

获得唯一（unique）组的数量,这个函数仅在统计针对某列的计数时才有意义

**approx_count_distinct**

在处理大数据集的时候，精确的统计计数并不那么重要，某种精度的近似值也是可以接受的，此时可以使用approx_count_distinct函数

```python
from pyspark.sql.functions import approx_count_distinct
df.select(approx_count_distinct("StockCode", 0.1)).show() # 3364  / 4070
# approx_count_distinct带了另一个参数，该参数指定可容忍的最大误差
```

**first和last**

这两个函数可以得到D a t a F r a m e的第一个值和最后一个值，它是基于DataFrame中行的顺序而不是DataFrame中值的顺序

```python
from pyspark.sql.functions import first, last
df.select(first("StockCode"), last("StockCode")).show()
```

**min和max**

从 DataFrame 中提取最小值和最大数值

**sum**

使用sum函数累加一行中的所有值

除了计算总和外，还可以使用**sumDistinct**函数来对一组去重（distinct）值进行求和

**avg或mean**

获取平均值

你还可以计算所有去重（distinct）值的平均值。实际上，大多数聚合函数都支持对去重值进行聚合计算

**方差和标准差**

在Spark中使用相应的函数来计算这些值。然而值得注意的是，Spark既支持统计样本标准差，也支持统计总体标准差，它们两个在统计学上是完全不同的概念，一定要区分它们。

如果使用variance函数和stddev函数，默认是计算样本标准差或样本方差的。

你还可以显式指定这些值或引用总体标准偏差或方差

```python
from pyspark.sql.functions import var_pop, stddev_pop
from pyspark.sql.functions import var_samp, stddev_samp
df.select(var_pop("Quantity"), var_samp("Quantity"),
stddev_pop("Quantity"), stddev_samp("Quantity")).show()
```

**skewness和kurtosis**

偏度系数（skewness）和峰度系数（kurtosis）都是对数据集中的极端数据点的衡量指标。偏度系数衡量数据相对于平均值的不对称程度，而峰度系数衡量数据分布形态陡缓程度

```python
from pyspark.sql.functions import skewness, kurtosis
df.select(skewness("Quantity"), kurtosis("Quantity")).show()
```

**协方差和相关性**

比较两个不同列的值之间的相互关系。分别用于计算协方差和相关性。相关性采用Pearson相关系数来衡量，范围是-1~+1。协方差的范围由数据中的输入决定

跟var函数一样，协方差又分为样本协方差和总体协方差，因此在使用的时候需要指定，这一点很重要。相关性没有这个概念，因此没有总体或样本的相关性之分

```python
from pyspark.sql.functions import corr, covar_pop, covar_samp
df.select(corr("InvoiceNo", "Quantity"), covar_samp("InvoiceNo", "Quantity"),
covar_pop("InvoiceNo", "Quantity")).show()
```

#### 聚合输出复杂类型

在Spark中，不仅可以在数值型上执行聚合操作，还能在复杂类型上执行聚合操作。例如，可以收集某列上的值到一个list列表里，或者将unique唯一值收集到一个set集合里。用户可以在流水线处理的后续操作中再访问该集合，或者将整个集合传递给用户自定义函数（UDF）

```python
from pyspark.sql.functions import collect_set, collect_list
df.agg(collect_set("Country"), collect_list("Country")).show()
```

#### 分组

```python
df.groupBy("InvoiceNo", "CustomerId").count().show()
```

#### 使用表达式分组

```python
from pyspark.sql.functions import count
df.groupBy("InvoiceNo").agg(
count("Quantity").alias("quan"),
expr("count(Quantity)")).show() q
```

#### 使用Map进行分组

```python
df.groupBy("InvoiceNo").agg(expr("avg(Quantity)"),expr("stddev_pop(Quantity)"))\
.show()
```

#### 分组集（grouping set）

分组集是用于将多组聚合操作组合在一起的底层工具，使得能够在group-by语句中创建任意的聚合操作

```python
dfNoNull = dfWithDate.drop()
dfNoNull.createOrReplaceTempView("dfNoNull")
#SELECT CustomerId, stockCode, sum(Quantity) FROM dfNoNull
#GROUP BY customerId, stockCode
#ORDER BY CustomerId DESC, stockCode DESC
# --SQL
SELECT CustomerId, stockCode, sum(Quantity) FROM dfNoNull
GROUP BY customerId, stockCode GROUPING SETS((customerId, stockCode))
ORDER BY CustomerId DESC, stockCode DESC
```

**分组集取决于聚合级别的 null 值。如果不过滤空值，则会得到不正确的结果。cube、rollup和分组集都是这样**

**GROUPING SETS操作仅在SQL中可用。若想在DataFrame中执行相同的操作，使用rollup和cube操作可以得到完全相同的结果。**

#### rollup

rollup分组聚合是一种多维聚合操作，可以执行多种不同group-by风格的计算

```python
rolledUpDF = dfNoNull.rollup("Date", "Country").agg(sum("Quantity"))\
.selectExpr("Date", "Country", "`sum(Quantity)` as total_quantity")\
.orderBy("Date")
rolledUpDF.show()
```

#### cube

cube分组聚合则更进一步，它不同于rollup的分级聚合，而是对所有参与的列值进行所有维度的全组合聚合

```python
from pyspark.sql.functions import sum
dfNoNull.cube("Date", "Country").agg(sum(col("Quantity")))\
.select("Date", "Country", "sum(Quantity)").orderBy("Date").show()
```

#### 透视转换(pivot)

透视转换可以根据某列中的不同行创建多个列。例如，在当前数据中，我们有一个Country列，通过一个透视转换，我们可以对每个Country执行聚合操作，并且以易于查看的方式显示他们

```python
pivoted = dfWithDate.groupBy("date").pivot("Country").sum()
```

在使用了透视转换后，现在DataFrame会为每一个Country和数值型列组合产生一个新列，以及之前的date列。例如，对于USA，就有USA_sum（Quantity），USA_sum（UnitPrice），USA_sum（CustomerID）这些列，对应于我们数据集中的每个数值型列（为USA和每个数值型列构建新列是因为一些聚合操作会作用于这些数值上）

#### 用户定义的聚合函数（UDAF）

用户定义的聚合函数（UDAF）是用户根据自定义公式或业务逻辑定义自己的聚合函数的一种方法。可以使用UDAF来计算输入数据组 (与单行相对) 的自定义计算。Spark维护单个AggregationBuffer ，它用于存储每组输入数据的中间结果。

若要创建UDAF，必须继承UserDefinedAggregateFunction基类并实现以下方法：
• inputSchema用于指定输入参数，输入参数类型为StructType。
• bufferSchema用于指定UDAF中间结果，中间结果类型为StructType。
• dataType用于指定返回结果，返回结果的类型为DataType。
• deterministic是一个布尔值，它指定此UDAF对于某个输入是否会返回相同的结果。
• initialize初始化聚合缓冲区的初始值。
• update描述应如何根据给定行更新内部缓冲区。
• merge描述应如何合并两个聚合缓冲区。
• evaluate将生成聚合最终结果。

#### 窗口函数（window function）

在用group-by处理数据分组时，每一行只能进入一个分组。窗口函数基于称为框（frame）的一组行，计算表的每一输入行的返回值，每一行可以属于一个或多个框

Spark支持三种窗口函数：排名函数、解析函数和聚合函数

```python
from pyspark.sql.functions import col, to_date
dfWithDate = df.withColumn("date", to_date(col("InvoiceDate"), "MM/d/yyyy H:mm"))
dfWithDate.createOrReplaceTempView("dfWithDate")
from pyspark.sql.window import Window
from pyspark.sql.functions import desc
windowSpec = Window\
.partitionBy("CustomerId", "date")\
.orderBy(desc("Quantity"))\
.rowsBetween(Window.unboundedPreceding, Window.currentRow)  #当前输入行之前的所有行都包含在这个frame里
# 计算一个客户有史以来的最大购买数量
from pyspark.sql.functions import max
maxPurchaseQuantity = max(col("Quantity")).over(windowSpec)
```

使用dense_rank而不是rank，是为了避免在有等值的情况下避免排序结果不连续

```python
from pyspark.sql.functions import dense_rank, rank
purchaseDenseRank = dense_rank().over(windowSpec)
purchaseRank = rank().over(windowSpec)

from pyspark.sql.functions import col
dfWithDate.where("CustomerId IS NOT NULL").orderBy("CustomerId")\
.select(
col("CustomerId"),
col("date"),
col("Quantity"),
purchaseRank.alias("quantityRank"),
purchaseDenseRank.alias("quantityDenseRank"),
maxPurchaseQuantity.alias("maxPurchaseQuantity")).show()
```



### 连接操作（join）

join操作比较左侧和右侧数据集的一个或多个键，并评估连接表达式的结果，以此来确定Spark是否将左侧数据集的一行和右侧数据集的一行组合起来。

最常见的连接表达式即equi-join，它用于比较左侧数据集一行和右侧数据集一行中的指定键是否匹配，相等则组合左侧和右侧数据集的对应行，对于键值不匹配的行则会丢弃。除了equi-join之外，Spark还提供很多复杂的连接策略，甚至还能使用复杂类型并在执行连接时执行诸如检查数组中是否存在键的操作

#### 连接类型

* equi-join, select a.* from a join b on (a.xx = b.xx); 
* inner join，内部连接（保留左、右数据集内某个键都存在的行）。
* outer join，外部连接（保留左侧或右侧数据集中具有某个键的行）
* left outer join，左外部连接（保留左侧数据集中具有某个键的行）。
* right outer join，右外部连接（保留右侧数据集中具有某个键的行）。
*  left semi join，左半连接（如果某键在右侧数据行中出现，则保留且仅保留左侧
  数据行）。
*  left anti join，左反连接 （如果某键在右侧数据行中没出现，则保留且仅保留左
  侧数据行）。
* natural join，自然连接（通过隐式匹配两个的数据集之间具有相同名称的列来执
  行连接）。
* cross join（笛卡尔连接Cartesian join），交叉连接（将左侧数据集中的每一行与
  右侧数据集中的每一行匹配）

#### 对复杂类型的连接操作

尽管这看起来像是一个挑战，但实际上并不是，任何返回Boolean值的表达式都是有效的连接表达式

```python
from pyspark.sql.functions import expr
person.withColumnRenamed("id", "personId")\
.join(sparkStatus, expr("array_contains(spark_status, id)")).show()
```

#### 处理重复列名

连接操作中棘手的问题是在生成的 DataFrame 中处理重复的列名。DataFrame 中的每一列在Spark的SQL引擎Catalyst中都有唯一的ID。它仅在内部可见，不能直接引用。当DataFrame的列的名字相同时，指定引用一个列会出现问题

在以下两种情况下可能会发生问题：

* 指定的连接表达式没有将执行连接操作的两个同名列的其中一个key删除。
*  连接操作的两个DataFrame中的非连接列同名。

**方法1： 采用不同的连接表达式**

当有两个同名的键时，最简单的解决方法是将连接表达式从布尔表达式更改为字符串或序列。这会在连接过程中自动删除其中一个列：

```python
person.join(gradProgramDupe,"graduate_program").select("graduate_program").show()
```

**方法2： 连接后删除列**

另一种方法是在连接后删除有冲突的列。在执行此操作时，我们需要通过原始源DataFrame引用该列，如果连接使用相同的键名，或者源 DataFrame具有同名的列，则可以执行此操作

```python
val joinExpr = person.col("graduate_program") === graduateProgram.col("id")
person.join(graduateProgram, joinExpr).drop(graduateProgram.col("id")).show()
```

注意对该列的引用是通过.col方法而不是通过column函数的，这使我们可以通过其特定的ID隐式指定该列。

**方法3： 在连接前重命名列**

### 列拼接（concat_ws）

```python
temp_table = df_immigration.withColumn("visa_id",md5(concat_ws('-',col("visa"),col("visaType"))))
```

## 数据类型

### BOOL类型

```python 
# in Python
from pyspark.sql.functions import instr
priceFilter = col("UnitPrice") > 600
descripFilter = instr(df.Description, "POSTAGE") >= 1
df.where(df.StockCode.isin("DOT")).where(priceFilter | descripFilter).show()

# in Python
from pyspark.sql.functions import instr
DOTCodeFilter = col("StockCode") == "DOT"
priceFilter = col("UnitPrice") > 600
descripFilter = instr(col("Description"), "POSTAGE") >= 1
df.withColumn("isExpensive", DOTCodeFilter & (priceFilter | descripFilter))\
.where("isExpensive")\
.select("unitPrice", "isExpensive").show(5)
```

### 数值类型

round函数会向上取整

bround函数进行向下取整

corr计算两列的相关性

describe 计算一列或一组列的汇总统计信息，它会计算所有数值型列的计数、均值、标准差、最小值和最大值

StatFunctions包中封装了许多可供使用的统计函数，这些是适用于各种计算的DataFrame方法

monotonically_increasing_id函数为每行添加一个唯一的ID。它会从0开始，为每行生成一个唯一值

```python
from pyspark.sql.functions import expr， pow
fabricatedQuantity = pow(col("Quantity") * col("UnitPrice")， 2) + 5
df.select(expr("CustomerId")， fabricatedQuantity.alias("realQuantity")).show(2)

# in Python
df.selectExpr(
"CustomerId",
"(POWER((Quantity * UnitPrice), 2.0) + 5) as realQuantity").show(2)

from pyspark.sql.functions import lit, round, bround
df.select(round(lit("2.5")), bround(lit("2.5"))).show(2)

from pyspark.sql.functions import corr
df.stat.corr("Quantity", "UnitPrice")
df.select(corr("Quantity", "UnitPrice")).show()

df.describe().show()
from pyspark.sql.functions import count， mean， stddev_pop， min， max

colName = "UnitPrice"
quantileProbs = [0.5]
relError = 0.05
df.stat.approxQuantile("UnitPrice"， quantileProbs， relError) # 2.51
```

### 字符串类型

initcap函数会将给定字符串中空格分隔的每个单词首字母大写。

lower 将字符串转为小写

upper 将字符串转为大写

ltrim 删除左边空格

rtrim 删除右边空格

trim	删除左右空格

lpad	在左侧添加空格，如果lpad或rpad方法输入的数值参数小于字符串长度，它将从字符串的右侧删
除字符。

rpad	在右侧添加空格，如果lpad或rpad方法输入的数值参数小于字符串长度，它将从字符串的右侧删
除字符。

#### 正则表达式

Spark充分利用了Java正则表达式的强大功能，但Java正则表达式与其他编程语言中的略有差别，因此实际应用之前需要检查。

regexp_extract  提取值

regexp_replace  替换值

构建正则表达式来实现该操作可能会有些冗长，所以Spark还提供了translate函数来实现该替换操作。这是在字符级上完成的操作，并将用给定字符串中替换掉所有出现的某字符串

有时，我们并不是要提取字符串，而是只想检查它们是否存在。此时可以在每列上用contains方法来实现这个操作。该方法将返回一个布尔值，它表示指定的值是否在该列的字符串中;在Python和SQL中，可以使用instr函数

```python
from pyspark.sql.functions import regexp_replace
regex_string = "BLACK|WHITE|RED|GREEN|BLUE"
df.select(
regexp_replace(col("Description"), regex_string, "COLOR").alias("color_clean"),
col("Description")).show(2)

from pyspark.sql.functions import instr
containsBlack = instr(col("Description"), "BLACK") >= 1
containsWhite = instr(col("Description"), "WHITE") >= 1
df.withColumn("hasSimpleColor", containsBlack | containsWhite)\
.where("hasSimpleColor")\
.select("Description").show(3, False)

from pyspark.sql.functions import expr, locate
simpleColors = ["black", "white", "red", "green", "blue"]
def color_locator(column, color_string):
return locate(color_string.upper(), column)\
.cast("boolean")\
.alias("is_" + c)
selectedColumns = [color_locator(df.Description, c) for c in simpleColors]
selectedColumns.append(expr("*")) # has to a be Column type
df.select(*selectedColumns).where(expr("is_white OR is_red"))\
.select("Description").show(3, False)
```

### 日期和时间戳类型

date  针对日历日期

timestamp  包括日期和时间信息

Spark的TimestampType类只支持二级精度，这意味着如果要处理毫秒或微秒，可能需要将数据作为long类型操作才能解决该问题。在强制转换为TimestampType时，任何更高的精度都被删除

datediff  查看两个日期之间的间隔时间,返回两个日期之间的天数

months_between 给出两个日期之间相隔的月数

to_date  该函数以指定的格式将字符串转换为日期数据,如果使用这个函数，则要在Java SimpleDateFormat中指定我们想要的格式

to_timestamp强制要求使用一种日期格式

```python
from pyspark.sql.functions import current_date, current_timestamp
dateDF = spark.range(10)\
.withColumn("today", current_date())\
.withColumn("now", current_timestamp())
dateDF.createOrReplaceTempView("dateTable")
dateDF.printSchema()

from pyspark.sql.functions import date_add, date_sub
dateDF.select(date_sub(col("today"), 5), date_add(col("today"), 5)).show(1)

from pyspark.sql.functions import datediff, months_between, to_date
dateDF.withColumn("week_ago", date_sub(col("today"), 7))\
.select(datediff(col("week_ago"), col("today"))).show(1)
dateDF.select(
to_date(lit("2016-01-01")).alias("start"),
to_date(lit("2017-05-22")).alias("end"))\
.select(months_between(col("start"), col("end"))).show(1)

from pyspark.sql.functions import to_date, lit
spark.range(5).withColumn("date", lit("2017-01-01"))\
.select(to_date(col("date"))).show(1)

from pyspark.sql.functions import to_date
dateFormat = "yyyy-dd-MM"
cleanDateDF = spark.range(1).select(
to_date(lit("2017-12-11"), dateFormat).alias("date"),
to_date(lit("2017-20-12"), dateFormat).alias("date2"))
cleanDateDF.createOrReplaceTempView("dateTable2")
from pyspark.sql.functions import to_timestamp
cleanDateDF.select(to_timestamp(col("date”), dateFormat)).show()            
```

### 空值

在实际应用中，建议始终使用null来表示 DataFrame中缺少或空的数据。相较于使用空字符串或其他值来说，使用null值更有利于Spark进行优化。

基于DataFrame，处理null值主要的方式是使用.na子包，还有一些用于执行操作并显式指定Spark应如何处
理null值的函数

coalesce  从一组列中选择第一个非空值

ifnull  如果第一个值为空， 则允许选择第二个值， 并将其默认为第一个

nullif  如果两个值相等， 则返回null， 否则返回第二个值。

nvl   如果第一个值为null，则返回第二个值，否则返回第一个

nvl2  如果第一个不为null，返回第二个值；否则，它将返回最后一个指定值

drop 删除包含null的行

fill  	用一组值填充一列或多列，它可以通过指定一个映射（即一个特定值和一组列）来完成此操作

replace  不只针对空值的灵活操作,根据当前值替换掉某列中的所有值，唯一的要求是替换值与原始值的
类型相同

```python
from pyspark.sql.functions import coalesce
df.select(coalesce(col("Description")， col("CustomerId"))).show()
# 指定"any”作为参数，当存在一个值是null时，就删除改行；
# 若指定"all”为参数，只有当所有的值为null或者NaN时才能删除该行
df.na.drop()
df.na.drop("any")
df.na.drop("all")
df.na.drop("all"， subset=["StockCode"， "InvoiceNo"])
# 替换某字符串类型列中的所有null值为某一字符串
df.na.fill("All Null values become this string")
# 对于Integer类型的列，可以使用df.na.fill(5:Integer)来实现；
# 对于Doubles类型的列，则使用df.na.fill(5:Double)。想要指定多列，需传入一个列名的数组
df.na.fill("all"， subset=["StockCode"， "InvoiceNo"])
fill_cols_vals = {"StockCode": 5， "Description" : "No Value"}
df.na.fill(fill_cols_vals)

df.na.replace([""]， ["UNKNOWN"]， "Description")
```

### 复杂类型

结构体、数组和map映射

#### 结构体

可以把结构体视为 DataFrame中的 DataFrame

```python
from pyspark.sql.functions import struct
complexDF = df.select(struct("Description", "InvoiceNo").alias("complex"))
complexDF.createOrReplaceTempView("complexDF")
# 可以像查询另一个 DataFrame一样查询它，唯一的区别是， 使用".”来访问或列方法getField来实现
complexDF.select("complex.Description")
complexDF.select(col("complex").getField("Description"))
complexDF.select("complex.*")
```

#### 数组

split函数并指定分隔符

size  计算数组大小

array_contains   查询此数组是否包含某个值

explode  输入参数为一个包含数组的列，并为该数组中的每个值创建一行

```python
from pyspark.sql.functions import split
df.select(split(col("Description"), " ")).show(2)
df.select(split(col("Description"), " ").alias("array_col"))\
.selectExpr("array_col[0]").show(2)
# 查询数组的大小
from pyspark.sql.functions import size
df.select(size(split(col("Description")， " "))).show(2)

from pyspark.sql.functions import array_contains
df.select(array_contains(split(col("Description"), " "), "WHITE")).show(2)

from pyspark.sql.functions import split, explode
df.withColumn("splitted", split(col("Description"), " "))\
.withColumn("exploded", explode(col("splitted")))\
.select("Description", "InvoiceNo", "exploded").show(2)
```

#### map

map映射是通过map函数构建两列内容的键值对映射形式。然后，便可以像在数组中一样去选择它们

```python
from pyspark.sql.functions import create_map
df.select(create_map(col("Description"), col("InvoiceNo")).alias("complex_map"))\
.show(2)
# 使用正确的键值（key）对它们进行查询。若键值（key）不存在则返回 null
df.select(map(col("Description"), col("InvoiceNo")).alias("complex_map"))\
.selectExpr("complex_map['WHITE METAL LANTERN']").show(2)
# 展开map类型，将其转换成列
df.select(map(col("Description"), col("InvoiceNo")).alias("complex_map"))\
.selectExpr("explode(complex_map)").show(2)
```

### JSON类型

Spark对处理 JSON 数据有一些独特的支持，比如可以在Spark中直接操作JSON字符串，并解析JSON或提取JSON对象

```python
jsonDF = spark.range(1).selectExpr("""'{"myJSONKey" : {"myJSONValue" : [1, 2, 3]}}' as jsonString""")
# 无论是字典还是数组，均可以使用get_json_object直接查询JSON对象。
# 如果此查询的JSON对象仅有一层嵌套， 则可使用json_tuple
from pyspark.sql.functions import get_json_object, json_tuple
jsonDF.select(
get_json_object(col("jsonString"), "$.myJSONKey.myJSONValue[1]") as "column",
json_tuple(col("jsonString"), "myJSONKey")).show(2)
# 还可以使用to_json函数将 StructType 转换为 JSON 字符串
from pyspark.sql.functions import to_json
df.selectExpr("(InvoiceNo, Description) as myStruct")\
.select(to_json(col("myStruct")))
# 使用from_json函数将JSON数据解析出来。这需要你指定一个模式，也可以指定其他的映射：
from pyspark.sql.functions import from_json
from pyspark.sql.types import *
parseSchema = StructType((
StructField("InvoiceNo",StringType(),True),
StructField("Description",StringType(),True)))
df.selectExpr("(InvoiceNo, Description) as myStruct")\
.select(to_json(col("myStruct")).alias("newJSON"))\
.select(from_json(col("newJSON"), parseSchema), col("newJSON")).show(2)
```

### 用户自定义函数(UDF)

Spark最强大的功能之一就是自定义函数，用户自定义函数（UDF）让用户可以使用Python 或 Scala 编写自己的自定义转换操作，甚至可以使用外部库。UDF可以将一个或多个列作为输入，同时也可以返回一个或多个列。Spark UDF非常强大，因为它允许使用多种不同的编程语言编写，而不需要使用一些难懂的格式或限定某些领域特定语言来编写。这些函数只是描述了（一个接一个地）处理数据记录的方法。默认情况下，这些函数被注册为SparkSession或者Context的临时函数。

如果该函数是用Scala或Java编写的， 则可以在Java虚拟机(JVM)中使用它。这意味着不能使用spark为内置函数提供的代码生成功能，或导致性能的一些下降

如果函数是用 Python 编写的， 则会出现一些截然不同的情况。Spark在worker上启动一个Python 进程， 将所有数据序列化为 Python 可解释的格式（请记住， 数据之前在JVM 中），在 Python 进程中对该数据逐行执行函数，最终将对每行的操作结果返回给JVM 和Spark

启动此 Python 进程代价很高， 但主要代价是将数据序列化为Python可理解的格式的这个过程。造成代价高的原因有两个: 一个是计算昂贵， 另一个是数据进入 Python 后Spark无法管理worker的内存。这意味着， 如果某个worker因资源受限而失败 (因为 JVM 和 Python 都在同一台计算机上争夺内存)， 则可能会导致该worker出现故障。所以建议使用 Scala 或 Java编写UDF，不仅编写程序的时间少，还能提高性能。当然仍然可以使用Python编写函数。

```python
udfExampleDF = spark.range(5).toDF("num")
def power3(double_value):
return double_value ** 3
power3(2.0)

from pyspark.sql.functions import udf
power3udf = udf(power3)
from pyspark.sql.functions import col
udfExampleDF.select(power3udf(col("num"))).show(2)
# 此时， 我们只能将它用作 DataFrame 函数。也就是说， 我们不能在字符串表达式中使用它。
# 但是，也可以将此UDF 注册为Spark SQL 函数。这种做法很有用，因为它使得能在SQL语言中以及跨语言环境下使用此函数
from pyspark.sql.types import IntegerType， DoubleType
spark.udf.register("power3py"， power3， DoubleType())
udfExampleDF.selectExpr("power3py(num)").show(2)
```

还可以使用Hive语法来创建UDF/UDAF。为了实现这一点， 首先必须在创建SparkSession 时启用Hive支持（通过SparkSession.builder().enableHiveSupport()来启用）。然后， 你可以在SQL中注册UDF。这仅支持预编译的Scala和Java包， 因此你需要将它们指定为依赖项:

```text
-- in SQL
CREATE TEMPORARY FUNCTION myFunc AS 'com.organization.hive.udf.FunctionName'
```

此外， 还能通过删除TEMPORARY将其注册为Hive Metastore中的永久函数

## 数据分区

为了让多个执行器并行地工作,S p a r k将数据分解成多个数据块,每个数据块叫做一个分区。分区是位于集群中的一台物理机上的多行数据的集合,DataFrame的分区也说明了在执行过程中,数据在集群中的物理分布。如果只有一个
分区,即使拥有数千个执行器,S p a r k也只有一个执行器在处理数据。类似地,如果有多个分区,但只有一个执行器,那么S p a r k仍然只有一个执行器在处理数据,就是因为只有一个计算资源单位
值得注意的是,当使用DataFrame时,(大部分时候)你不需要手动操作分区,只需指定数据的高级转换操作,然后Spark决定此工作如何在集群上执行

## Dataset

Dataset类似于RDD，但是，它们不使用Java序列化或Kryo，而是使用专用的[Encoder](http://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/Encoder.html)对对象进行序列化以进行处理或通过网络传输。虽然编码器和标准序列化都负责将对象转换为字节，但是编码器是动态生成的代码，并使用一种格式，该格式允许Spark执行许多操作，如过滤，排序和哈希处理，而无需将字节反序列化为对象

dataset只有java和scala接口

## SQL

使用Spark SQL，你可以将任何DataFrame注册为数据表或视图（临时表），并使用纯SQL对它进行查询。编写SQL查询或编写DataFrame代码并不会造成性能差异，它们都会被"编译”成相同的底层执行计划。

```python
# Register the DataFrame as a SQL temporary view
df.createOrReplaceTempView("people")

sqlDF = spark.sql("SELECT * FROM people")
sqlDF.show()
```

## 操作

操作分为**转换操作**和**动作操作**

### 转换操作

要" 更改”DataFrame，你需要告诉Spark如何修改它以执行你想要的操作，这个过程被称为转换。

如： divisBy2 = myRange.where("number % 2 = 0")

这些转换并没有实际输出，这是因为我们仅指定了一个抽象转换。在我们调用一个动作操作之前，Spark不会真的执行转换操作。

转换操作是使用Spark表达业务逻辑的核心，有两类转换操作：第一类是指定**窄依赖关系**的转换操作，第二类是指定**宽依赖关系**的转换操作。

窄依赖关系（narrow dependency）的转换操作（我们称之为窄转换）是每个输入分区仅决定一个输出分区的转换。在前面的代码片段中，where语句指定了一个窄依赖关系，其中一个分区最多只会对一个输出分区有影响。Spark将自动执行流水线处理，这意味着如果我们在DataFrame上指定了多个过滤操作，它们将全部在内存中执行。

宽依赖关系（wide dependency）的转换（或宽转换）是每个输入分区决定了多个输出分区。这种宽依赖关系的转换经常被称为洗牌（shuffle）操作，它会在整个集群中执行互相交换分区数据的功能。当我们执行shuffle操作时，Spark将结果写入磁盘

### 惰性评估

惰性评估（lazy evaluation）的意思就是等到绝对需要时才执行计算。

一个很好的例子就是DataFrame的谓词下推（predicate pushdown），假设我们构建一个含有多个转换操作的Spark作业，并在最后指定了一个过滤操作，假设这个过滤操作只需要数据源（输入数据）中的某一行数据，则最有效的方法是在最开始仅访问我们需要的单个记录，Spark会通过自动下推这个过滤操作来优化整个物理执行计划。

### 动作操作

转换操作使我们能够建立逻辑转换计划。为了触发计算，我们需要运行一个动作操作 （action）。

有三类动作：

* 在控制台中查看数据的动作。
* 在某个语言中将数据汇集为原生对象的动作。如collect操作
*  写入输出数据源的动作

**可以通过调用explain函数观察到Spark正在创建一个执行计划,并且可以看到这个计划将会怎样在集群上执行,调用某个DataFrame的explain操作会显示DataFrame的来源(即Spark是如何执行查询操作的)**
```python
flightData2015.sort("count").explain()
```

## 全局临时视图

Spark SQL中的临时视图是会话作用域的，如果创建它的会话终止，它将消失。如果要在所有会话之间共享一个临时视图并保持活动状态，直到Spark应用程序终止，则可以创建全局临时视图。全局临时视图与系统保留的`global_temp`数据库相关联，我们必须使用限定名称来引用它，例如`SELECT * FROM global_temp.view1`。

```python
# Register the DataFrame as a global temporary view
df.createGlobalTempView("people")

# Global temporary view is tied to a system preserved database `global_temp`
spark.sql("SELECT * FROM global_temp.people").show()
# Global temporary view is cross-session
spark.newSession().sql("SELECT * FROM global_temp.people").show()
```



## 标量函数

- [数组函数](http://spark.apache.org/docs/latest/sql-ref-functions-builtin.html#array-functions)
- [Map功能](http://spark.apache.org/docs/latest/sql-ref-functions-builtin.html#map-functions)
- [日期和时间戳功能](http://spark.apache.org/docs/latest/sql-ref-functions-builtin.html#date-and-timestamp-functions)
- [JSON函数](http://spark.apache.org/docs/latest/sql-ref-functions-builtin.html#json-functions)

## 类聚集函数

- [汇总功能](http://spark.apache.org/docs/latest/sql-ref-functions-builtin.html#aggregate-functions)
- [视窗功能](http://spark.apache.org/docs/latest/sql-ref-functions-builtin.html#window-functions)

## UDF（用户定义的函数）

- [标量用户定义函数（UDF）](http://spark.apache.org/docs/latest/sql-ref-functions-udf-scalar.html)
- [用户定义的聚合函数（UDAF）](http://spark.apache.org/docs/latest/sql-ref-functions-udf-aggregate.html)
- [与Hive UDF / UDAF / UDTF集成](http://spark.apache.org/docs/latest/sql-ref-functions-udf-hive.html)

```python
get_hour = udf(lambda x: datetime.datetime.fromtimestamp(x / 1000.0). hour)
user_log = user_log.withColumn("hour", get_hour(user_log.ts))
user_log.head()
```

## 配置

默认情况下，shuffle操作会输出200个shuffle分区，我们将此值设置为5以减少shuffle输出分区的数量：

```python
spark.conf.set("spark.sql.shuffle.partitions", "5")
flightData2015.sort("count").take(2)
```

## 数据源

核心数据源：CSV， JSON，Parquet，ORC， JDBC/ODBC连接，纯文本文件

社区创建的数据源：Cassandra，HBase，MongoDB，AWS Redshift，XML，其他数据源

### 读取数据的核心结构

```python
DataFrameReader.format(...).option("key", "value").schema(...).load()
```

我们将使用此格式来读取所有数据源。format是可选的，默认情况下Spark将使用Parquet格式，option使你能配置键值对（key-value）来参数化读取数据的方式。最后，如果数据源包含某种schema或你想使用模式推理（schema inference），则可以选择指定schema

### 读取模式

从外部源读取数据很容易会遇到错误格式的数据，尤其是在处理半结构化数据时。读取模式指定当Spark遇到错误格式的记录时应采取什么操作。默认是permissive

| 读取模式      | 说明                                                         |
| ------------- | ------------------------------------------------------------ |
| permissive    | 当遇到错误格式的记录时，将所有字段设置为null并将所有错误格<br/>式的记录放在名为_corrupt_record字符串列中 |
| dropMalformed | 删除包含错误格式记录的行                                     |
| failFast      | 遇到错误格式的记录后立即返回失败                             |

### 写数据的核心结构

```python
DataFrameWriter.format(...).option(...).partitionBy(...).bucketBy(...).sortBy(...).sa
ve()
```

format是可选的，默认情况下Spark将使用arquet 格式，option仍用于配置写出数据的方法，PartitionBy，bucketBy和sortBy仅适用基于文件的数据源，你可以使用这些方法来控制写出目标文件的具体结构。

### 保存模式

默认值为errorIfExists

| 保存模式      | 描述                                                       |
| ------------- | ---------------------------------------------------------- |
| append        | 将输出文件追加到目标路径已存在的文件上或目录的文件列表     |
| overwrite     | 将完全覆盖目标路径中已存在的任何数据                       |
| errorIfExists | 如果目标路径已存在数据或文件，则抛出错误并返回写入操作失败 |
| ignore        | 如果目标路径已存在数据或文件，则不执行任何操作             |



### 读写CSV文件

CSV 读取程序中的可选项

- **path** –输入路径的字符串或字符串列表，或存储CSV行的字符串的RDD 。

- **schema** – [`pyspark.sql.types.StructType`](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html?highlight=csv#pyspark.sql.types.StructType)输入模式的可选参数或DDL格式的字符串（例如）。`col0 INT,col1 DOUBLE`

- **sep** –为每个字段和值设置分隔符（一个或多个字符）。如果设置为None，则使用默认值`,`。

- **encoding** – 按给定的编码类型解码CSV文件。如果设置为None，则使用默认值`UTF-8`。

- **quote** –设置用于转义带引号的值的单个字符，其中分隔符可以是值的一部分。如果设置为None，则使用默认值`"`。如果要关闭引号，则需要设置一个空字符串。

- **escape** -设置一个字符，用于转义已经引用的值内的引号。如果设置为None，则使用默认值`\`。

- **comment** –设置用于跳过以该字符开头的行的单个字符。默认情况下（无），它是禁用的。

- **header**  –使用第一行作为列名。如果设置为None，则使用默认值`false`。

- **inferSchema** –从数据自动推断输入模式。它需要对数据进行一次额外的传递。如果设置为None，则使用默认值`false`。

- **forceSchema** –如果将其设置为`true`，则将强制将指定或推断的模式应用于数据源文件，并且将忽略CSV文件中的标头。如果选项设置为`false`，该架构将针对所有的头被验证CSV文件或RDD第一头如果`header`选项设置为`true`。架构标题中的字段名称和CSV标头中的列名称通过考虑其位置进行检查`spark.sql.caseSensitive`。如果设置`true`为None， 则默认情况下使用。尽管默认值为`true`，但建议禁用该`enforceSchema`选项，以免产生错误的结果。

- **ignoreLeadingWhiteSpace** –一个标志，指示是否应跳过正在读取的值中的前导空格。如果设置为None，则使用默认值`false`。

- **ignoreTrailingWhiteSpace** –一个标志，指示是否应跳过正在读取的值的尾随空格。如果设置为None，则使用默认值`false`。

- **nullValue** –设置空值的字符串表示形式。如果设置为None，它将使用默认值空字符串。从2.0.1开始，此`nullValue`参数适用于所有支持的类型，包括字符串类型。

- **nanValue** –设置非数字值的字符串表示形式。如果设置为None，则使用默认值`NaN`。

- **PositiveInf** –设置正无穷大值的字符串表示形式。如果设置为None，则使用默认值`Inf`。

- **negativeInf** –设置负无穷大值的字符串表示形式。如果设置为None，则使用默认值`Inf`。

- **dateFormat** –设置指示日期格式的字符串。自定义日期格式遵循[datetime模式](https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html)的格式。这适用于日期类型。如果设置为None，则使用默认值`yyyy-MM-dd`。

- **timestampFormat** –设置指示时间戳格式的字符串。自定义日期格式遵循[datetime模式](https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html)的格式。这适用于时间戳类型。如果设置为None，则使用默认值`yyyy-MM-dd'T'HH:mm:ss[.SSS][XXX]`。

- **maxColumns** –定义一条记录可以具有多少列的硬限制。如果设置为None，则使用默认值`20480`。

- **maxCharsPerColumn** –定义读取任何给定值所允许的最大字符数。如果设置为None，则使用默认值， `-1`即无限长度。

- **maxMalformedLogPerPartition** –自Spark 2.2.0起不再使用此参数。如果指定，它将被忽略。

- **mode**  –

  - 允许一种在解析过程中处理损坏记录的模式。如果没有

    设置，它使用默认值`PERMISSIVE`。请注意，Spark尝试在列修剪下仅解析CSV中所需的列。因此，损坏的记录可以根据所需的字段集而有所不同。可以通过`spark.sql.csv.parser.columnPruning.enabled` （默认启用）控制此行为。

  - `PERMISSIVE`：当遇到损坏的记录时，将格式错误的字符串放入由配置的字段中`columnNameOfCorruptRecord`，并将格式错误的字段设置为`null`。为了保留损坏的记录，用户可以设置`columnNameOfCorruptRecord`在用户定义的模式中命名的字符串类型字段。如果架构没有该字段，它将在解析期间删除损坏的记录。具有比模式少/多的令牌的记录不是CSV损坏的记录。当它遇到记号少于模式长度的记录时，请设置`null`为额外字段。当记录的令牌数量超过架构的长度时，它会丢弃额外的令牌。
  - `DROPMALFORMED`：忽略整个损坏的记录。
  - `FAILFAST`：遇到损坏的记录时将引发异常。

- **columnNameOfCorruptRecord** –允许重命名由`PERMISSIVE`mode 创建的格式错误的字符串的新字段。这将覆盖 `spark.sql.columnNameOfCorruptRecord`。如果设置为None，则使用中指定的值`spark.sql.columnNameOfCorruptRecord`。

- **multiLine** –解析记录，该记录可能跨越多行。如果设置为None，则使用默认值`false`。

- **charToEscapeQuoteEscaping** –设置单个字符，用于转义引号字符的转义。如果设置为None，则默认值是转义字符和引号字符不同时的转义字符，`\0`否则。

- **sampleRatio** –定义用于模式推断的行的比例。如果设置为None，则使用默认值`1.0`。

- **emptyValue** –设置一个空值的字符串表示形式。如果设置为None，它将使用默认值空字符串。

- **locale** –将语言环境设置为IETF BCP 47格式的语言标签。如果设置为None，则使用默认值`en-US`。例如，`locale`在解析日期和时间戳时使用。

- **lineSep** –定义用于解析的行分隔符。如果没有设置，它涵盖了所有`\\r`，`\\r\\n`和`\\n`。最大长度为1个字符。

- **pathGlobFilter** –可选的glob模式，仅包括路径与模式匹配的文件。语法遵循org.apache.hadoop.fs.GlobFilter。它不会改变[分区发现](https://spark.apache.org/docs/latest/sql-data-sources-parquet.html#partition-discovery)的行为。

- **recursiveFileLookup** –递归扫描目录中的文件。使用此选项将禁用[分区发现](https://spark.apache.org/docs/latest/sql-data-sources-parquet.html#partition-discovery)。

```python
spark = SparkSession.builder.appName("myproject").getOrCreate()
flightData2015 = spark.read\
	.option("inferSchema", "true").option("header", "true")\
	.csv("/data/flight-data/csv/2015-summary.csv")
df.printSchema()
df.show(5)

staticDataFrame = spark.read.format("csv")\
.option("header", "true")\
.option("inferSchema", "true")\
.load("/data/retail-data/by-day/*.csv")

spark.read.format("csv")
.option("mode", "FAILFAST")
.option("inferSchema", "true")
.option("path", "path/to/file(s)")
.schema(someSchema)
.load()

val myManualSchema = new StructType(Array(
new StructField("DEST_COUNTRY_NAME", StringType, true),
new StructField("ORIGIN_COUNTRY_NAME", StringType, true),
new StructField("count", LongType, false)
))
spark.read.format("csv")
.option("header", "true")
.option("mode", "FAILFAST")
.schema(myManualSchema)
.load("/data/flight-data/csv/2010-summary.csv")
.show(5)
        
dataframe.write.format("csv")
.option("mode", "OVERWRITE")
.option("dateFormat", "yyyy-MM-dd")
.option("path", "path/to/file(s)")
.option("sep", "\t")
.save()   
```

### 读写json文件

在Spark中，我们提及的JSON文件指的是换行符分隔的JSON，每行必须包含一个单独的、独立的有效JSON对象，这与包含大的JSON对象或数组的文件是有区别的

换行符分隔JSON对象还是一个对象可以跨越多行，这个可以由multiLine选项控制，当multiLine为true时，则可以将整个文件作为一个json对象读取，并且Spark将其解析为DataFrame。换行符分隔的JSON实际上是一种更稳定的格式，因为它可以在文件末尾追加新记录（而不是必须读入整个文件然后再写出）

由于JSON结构化对象封装的原因，导致JSON文件选项比CSV的要少得多。

JSON 对象可用的选项以及说明：

- **path** –字符串表示JSON数据集的路径，路径列表或存储JSON对象的字符串的RDD。

- **schema** – [`pyspark.sql.types.StructType`](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html?highlight=json#pyspark.sql.types.StructType)输入模式的可选参数或DDL格式的字符串（例如）。`col0 INT,col1 DOUBLE`

- **originalsAsString** –将所有原始值推断为字符串类型。如果设置为None，则使用默认值`false`。

- **preferredsDecimal** –将所有浮点值推断为十进制类型。如果这些值不适合小数，则将其推断为双精度。如果设置为None，则使用默认值`false`。

- **allowComments** –忽略JSON记录中的Java / C ++样式注释。如果设置为None，则使用默认值`false`。

- **allowUnquotedFieldNames** –允许不带引号的JSON字段名称。如果设置为None，则使用默认值`false`。

- **allowSingleQuotes** –除双引号外还允许单引号。如果设置为None，则使用默认值`true`。

- **allowNumericLeadingZero** –允许数字前导零（例如00012）。如果设置为None，则使用默认值`false`。

- **allowBackslashEscapingAnyCharacter** –允许使用反斜杠引号机制接受所有字符的引号。如果设置为None，则使用默认值`false`。

- **mode** –

  - 允许一种在解析过程中处理损坏记录的模式。如果没有

    设置，它使用默认值`PERMISSIVE`。

  - `PERMISSIVE`：当遇到损坏的记录时，将格式错误的字符串放入由配置的字段中`columnNameOfCorruptRecord`，并将格式错误的字段设置为`null`。为了保留损坏的记录，用户可以设置`columnNameOfCorruptRecord`在用户定义的模式中命名的字符串类型字段。如果架构没有该字段，它将在解析期间删除损坏的记录。推断模式时，它会`columnNameOfCorruptRecord` 在输出模式中隐式添加一个字段。
  - `DROPMALFORMED`：忽略整个损坏的记录。
  - `FAILFAST`：遇到损坏的记录时将引发异常。

  

- **columnNameOfCorruptRecord** –允许重命名由`PERMISSIVE`mode 创建的格式错误的字符串的新字段。这将覆盖 `spark.sql.columnNameOfCorruptRecord`。如果设置为None，则使用中指定的值`spark.sql.columnNameOfCorruptRecord`。

- **dateFormat** –设置指示日期格式的字符串。自定义日期格式遵循[datetime模式](https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html)的格式。这适用于日期类型。如果设置为None，则使用默认值`yyyy-MM-dd`。

- **timestampFormat** –设置指示时间戳格式的字符串。自定义日期格式遵循[datetime模式](https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html)的格式。这适用于时间戳类型。如果设置为None，则使用默认值`yyyy-MM-dd'T'HH:mm:ss[.SSS][XXX]`。

- **multiLine** –每个文件解析一条记录，该记录可能跨越多行。如果设置为None，则使用默认值`false`。

- **allowUnquotedControlChars** –允许JSON字符串包含不带引号的控制字符（值小于32的ASCII字符，包括制表符和换行符）。

- **encoding** –允许为JSON文件强制设置标准基本或扩展编码之一。例如UTF-16BE，UTF-32LE。如果设置为None，则在multiLine选项设置为时，将自动检测输入JSON的编码`true`。

- **lineSep** –定义用于解析的行分隔符。如果没有设置，它涵盖了所有`\r`，`\r\n`和`\n`。

- **sampleRatio** –定义用于模式推断的输入JSON对象的一部分。如果设置为None，则使用默认值`1.0`。

- **dropFieldIfAllNull** –在模式推断期间是否忽略所有空值的列或空数组/结构。如果设置为None，则使用默认值`false`。

- **locale**  –将语言环境设置为IETF BCP 47格式的语言标签。如果设置为None，则使用默认值`en-US`。例如，`locale`在解析日期和时间戳时使用。

- **pathGlobFilter** –可选的glob模式，仅包括路径与模式匹配的文件。语法遵循org.apache.hadoop.fs.GlobFilter。它不会改变[分区发现](https://spark.apache.org/docs/latest/sql-data-sources-parquet.html#partition-discovery)的行为。

- **recursiveFileLookup** –递归扫描目录中的文件。使用此选项将禁用[分区发现](https://spark.apache.org/docs/latest/sql-data-sources-parquet.html#partition-discovery)。

```python
spark.read.format("json").option("mode", "FAILFAST")\
.option("inferSchema", "true")\
.load("/data/flight-data/json/2010-summary.json").show(5)

csvFile.write.format("json").mode("overwrite").save("/tmp/my-json-file.json")
```

### 读写Parquet文件

Parquet是一种开源的面向列的数据存储格式，它提供了各种存储优化，尤其适合数据分析。Parquet提供列压缩从而可以节省空间，而且它支持按列读取而非整个文件地读取。

建议将数据写到Parquet以便长期存储，因为从Parquet文件读取始终比从JSON文件或CSV文件效率更高

Parquet的另一个优点是它支持复杂类型，也就是说如果列是一个数组（CSV文件无法存储数组列）、map映射或struct结构体，仍可以正常读取和写入，不会出现任何问题。

Parquet对象可用的选项以及说明

- **mergeSchema** –设置是否应该合并从所有Parquet零件文件收集的模式 。这将覆盖 。默认值在中指定 。`spark.sql.parquet.mergeSchema``spark.sql.parquet.mergeSchema`
- **pathGlobFilter** –可选的glob模式，仅包括路径与模式匹配的文件。语法遵循org.apache.hadoop.fs.GlobFilter。它不会改变[分区发现](https://spark.apache.org/docs/latest/sql-data-sources-parquet.html#partition-discovery)的行为。
- **recursiveFileLookup** –递归扫描目录中的文件。使用此选项将禁用[分区发现](https://spark.apache.org/docs/latest/sql-data-sources-parquet.html#partition-discovery)。

```python
spark.read.format(“parquet")\
.load(“/data/flight-data/parquet/2010-summary.parquet").show(5)
csvFile.write.format(“parquet").mode(“overwrite")\
.save(“/tmp/my-parquet-file.parquet")
```

### 读写ORC文件

ORC是为Hadoop作业而设计的自描述、类型感知的列存储文件格式。它针对大型流式数据读取进行优化，但集成了对快速查找所需行的相关支持。实际上，读取ORC文件数据时没有可选项，这是因为Spark非常了解该文件格式。一个问题常被问到：ORC和Parquet有什么区别？在大多数情况下，它们非常相似，本质区别是，Parquet
针对Spark进行了优化，而ORC则是针对Hive进行了优化。

```python
spark.read.format("orc").load("/data/flight-data/orc/2010-summary.orc").show(5)
csvFile.write.format("orc").mode("overwrite").save("/tmp/my-json-file.orc")
```

### SQL数据库

很多系统的标准语言都采用SQL，所以SQL数据源是很强大的连接器，只要支持SQL就可以和许多系统兼容。例如，你可以连接到MySQL数据库、PostgreSQL数据库或Oracle数据库，还可以连接到SQLite。

读写这些数据库需要两步：在Spark类路径中为指定的数据库包含Java Database Connectivity（JDBC）驱动，并为连接驱动器提供合适的JAR包。



## spark-submit

spark-submit轻松地将测试级别的交互式程序转化为生产级别的应用程序。sparksubmit将你的应用程序代码发送到一个集群并在那里执行，应用程序将一直运行，直到它（完成任务后）正确退出或遇到错误。你的程序可以在集群管理器的支持下进行，包括Standalone，Mesos和YARN等。

spark-submit提供了若干控制选项，你可以指定应用程序需要的资源，以及应用程序的运行方式和运行参数等

你可以使用Spark支持的任何语言编写应用程序，然后提交它执行

```python
./bin/spark-submit \
--master local \
./examples/src/main/python/pi.py 10
```

## Dataset：类型安全的结构化API

Dataset，用于在Java和Scala中编写静态类型的代码。Dataset API在Python和R中不可用，因为这些语
言是动态类型的。

**暂时跳过**

