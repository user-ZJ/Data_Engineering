**Amazon S3**:存储数据

**Elastic Compute Cloud（EC2）**：计算服务器

- **本地模式**：您像一台机器一样在笔记本电脑上运行Spark程序。
- **独立模式**：您正在定义Spark Primary和Secondary在您的（虚拟）计算机上工作。您可以在EMR或您的计算机上执行此操作。独立模式使用资源管理器，例如YARN或Mesos。

**EMR(Elastic Map Reduce)**:AWS提供的一项服务，它使您（用户）在创建集群时无需手动安装每台机器的Spark及其依赖项

### EC2和EMR

|                        | **AWS EMR**                                                  | **AWS EC2**                                 |
| :--------------------- | :----------------------------------------------------------- | :------------------------------------------ |
| **分布式计算**         | 是                                                           | 是                                          |
| **节点分类**           | 将辅助节点分类为核心节点和任务节点，如果删除数据节点，则可能会丢失数据。 | 不使用节点分类                              |
| **可以支持HDFS吗？**   | 是                                                           | 仅当您使用多步骤过程自己在EC2上配置HDFS时。 |
| **可以使用什么协议？** | 在AWS S3上使用S3协议，它比s3a协议要快                        | ECS使用s3a                                  |
| **比较费用**           | 更高一点                                                     | 较低                                        |

## AWS CLI

使用AWS CLI，您可以运行命令来访问当前可用的AWS服务。

安装AWS CLI `pip install awscli`

### 创建EMR

```shell
aws emr create-cluster --name <cluster_name> \
 --use-default-roles --release-label emr-5.28.0  \
--instance-count 3 --applications Name=Spark Name=Zeppelin  \
--bootstrap-actions Path="s3://bootstrap.sh" \
--ec2-attributes KeyName=<your permission key name> \
--instance-type m5.xlarge --log-uri s3:///emrlogs/
```

> - `aws emr` ：调用AWS CLI，尤其是EMR的命令。
> - `create-cluster` ：创建集群
> - `--name`：您可以为此指定任何名称-这将显示在您的AWS EMR UI中。这可以与现有的EMR复制。
> - `--release-label`：这是您要使用的EMR版本。
> - `--instance-count`：注释实例数。一个用于主要对象，其余用于辅助对象。例如，如果给定--instance-count 4，则将为主要实例保留1个实例，然后为次要实例保留3个实例。
> - `--applications`：您要在启动时预先安装在EMR上的应用程序列表
> - `--bootstrap-actions`：您可以在S3中存储一个预安装或设置的脚本环境变量，并在EMR启动时调用该脚本
> - `--ec2-attributes KeyName`：指定您的权限密钥名称，例如，如果是MyKey.pem，则只需`MyKey`为此字段指定
> - `--instance-type`：指定要使用的实例类型。[您可以在此处访问详细列表](https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-supported-instance-types.html)，但可以找到适合您的数据和预算的列表。
> - `--log-uri`：S3位置，用于存储您的EMR日志。此日志可以存储EMR指标以及用于提交代码的指标/日志。
> - --auto-terminate

### 查看集群状态

```
aws emr describe-cluster --cluster-id <CLUSTER_ID>
```

## IAM

https://console.aws.amazon.com/iam/home#/home

管理对AWS的资源访问，包括用户，角色，

管理密钥：控制面板->删除您的根访问密钥->管理安全证书->访问密钥->创建新的访问密钥

> 使用访问密钥从 AWS CLI、PowerShell 工具、AWS 开发工具包以编程方式调用 AWS，或者直接进行 AWS API 调用

## EMR

https://console.aws.amazon.com/elasticmapreduce/

在左侧菜单中选择“集群”，然后单击“创建集群”按钮，

> 集群配置
>
> - 发布：`emr-5.20.0`或更高版本
> - 应用程序：：`Spark`使用Ganglia 3.7.2和Zeppelin 0.8.0的Hadoop 2.8.5 YARN上的Spark 2.4.0
> - 实例类型： `m3.xlarge`
> - 实例数： `3`
> - EC2密钥对：`Proceed without an EC2 key pair`如果需要，也可以随意使用一个

创建集群后，您会在集群名称旁边看到状态“ *Starting”*。请稍等片刻，将此状态更改为“ *正在等待”，*然后再继续下一步

在左侧菜单中选择“笔记本”，然后单击“创建笔记本”按钮。

> notebook配置
>
> - 输入笔记本名称
> - 选择“选择现有集群”，然后选择刚创建的集群
> - 使用“ AWS服务角色”的默认设置-如果您之前没有做过此设置，则应为“ EMR_Notebooks_DefaultRole”或“创建默认角色”。

创建EMR笔记本后，您需要等待一小段时间，笔记本状态才能从“ *开始”*或“ *待处理”*变为“ *就绪”。*笔记本状态为“ *就绪”后*，单击“打开”按钮以打开笔记本

当你与你的笔记本电脑完成后，单击`File`> `Download as`> `Notebook`以将其下载到您的计算机。

## AWS Athena



## Spark

## S3

查看s3中目录

udacity-dend.s3.us-west-2.amazonaws.com

查看s3中文件

udacity-dend.s3.us-west-2.amazonaws.com/log_json_path.json

