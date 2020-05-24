- [Hadoop-](https://hadoop.apache.org/)用于大数据存储和数据分析的工具生态系统。Hadoop是一个比Spark更旧的系统，但仍被许多公司使用。Spark和Hadoop之间的主要区别在于它们如何使用内存。Hadoop将中间结果写入磁盘，而Spark尝试尽可能将数据保留在内存中。这使得Spark在许多用例中都更快。
- **Hadoop MapReduce-**一种用于并行处理和分析大型数据集的系统。
- **Hadoop YARN-**资源管理器，可跨集群调度作业。管理器跟踪可用的计算机资源，然后将这些资源分配给特定任务。
- **Hadoop分布式文件系统（HDFS）** -一种大数据存储系统，可将数据拆分为多个块，并将这些块存储在计算机集群中。
- **Apache Pig-**一种类似于SQL的语言，在Hadoop MapReduce上运行
- **Apache Hive-**在Hadoop MapReduce上运行的另一个类似SQL的界面

# Spark与Hadoop有何关系？

本课程的重点是Spark，它是另一个大数据框架。Spark包含用于数据分析，机器学习，图形分析和实时数据流的库。Spark通常比Hadoop快。这是因为Hadoop将中间结果写入磁盘，而Spark尝试将中间结果尽可能保留在内存中。

Hadoop生态系统包括一个称为HDFS（Hadoop分布式文件系统）的分布式文件存储系统。另一方面，Spark不包含文件存储系统。您可以在HDFS之上使用Spark，但不必这样做。Spark可以从其他来源（例如[Amazon S3）](https://aws.amazon.com/s3/)读取数据。

Spark具有一个称为[Spark Streaming](https://spark.apache.org/docs/latest/streaming-programming-guide.html)的流媒体库，尽管它不像其他某些流媒体库那样流行和快速。其他流行的流媒体库包括[Storm](http://storm.apache.org/)和 [Flink](https://flink.apache.org/)。本课程不会涉及流媒体，但是您可以通过以下链接来了解有关这些技术的更多信息。





MapReduce是一种用于处理大型数据集的编程技术。“ Hadoop MapReduce”是此编程技术的特定实现。

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
- `sort()`：返回按指定列排序的新DataFrame。默认情况下，第二个参数“升序”为True。
- `dropDuplicates()`：返回一个具有基于所有列或仅列的子集的唯一行的新DataFrame
- `withColumn()`：通过添加列或替换具有相同名称的现有列来返回新的DataFrame。第一个参数是新列的名称，第二个参数是如何计算它的表达式。
- `agg({"salary": "avg", "age": "max"})`计算平均工资和最大年龄。

spark SQL提供了内置的方法最常见的聚合，例如`count()`，`countDistinct()`，`avg()`，`max()`，`min()`，等在pyspark.sql.functions模块

在Spark SQL中，我们可以使用pyspark.sql.functions模块中的udf方法定义自己的函数。UDF返回的变量的默认类型为字符串。如果我们想返回其他类型，则需要使用pyspark.sql.types模块中的不同类型来显式地返回。

RDD是数据的低层抽象。在Spark的第一个版本中，您直接使用RDD。您可以将RDD视为分布在各种计算机上的长列表。尽管数据框架和SQL更容易，但仍可以将RDD用作Spark代码的一部分。