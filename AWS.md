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

### AWS CLI

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

## IAM-用户/角色管理

https://console.aws.amazon.com/iam/home#/home

```text
1. 选择角色
2. 选择创建角色
3. 在AWS Service组中，选择Redshift。
4. 在“ 选择您的用例”下，选择“ Redshift-可自定义”，然后选择“ 下一步：权限”。
5. 在“ 附加权限策略”页面上，选择AmazonS3ReadOnlyAccess/AdministratorAccess，然后选择下一步：标签。
6. 跳过此页面，然后选择下一步：查看。
7. 对于“ 角色名称”，输入myRedshiftRole，然后选择“ 创建角色”。
```

```text
1. 在左侧导航窗格中，选择Users。
2. 选择添加用户。
3. 输入您的用户名（例如airflow_redshift_user）(如果需要boto3编程访问，需要勾选programmatic access)
4. 选择“ 程序访问”，然后选择“ 下一步：权限”。
5. 选择直接附加现有策略。
6. 搜索redshift并选择AmazonRedshiftFullAccess。然后，搜索S3并选择AmazonS3ReadOnlyAccess。选择两个策略后，选择下一步：标签。
7. 跳过此页面，然后选择下一步：查看。
8. 查看您的选择，然后选择“ 创建用户”。
9. 保存您的凭据！这是您唯一可以在AWS上查看或下载这些凭证的时间。选择“ 下载.csv”以下载这些凭据，然后将此文件保存到安全位置。下一步，您需要复制并粘贴此访问密钥ID和秘密访问密钥。

```



管理对AWS的资源访问，包括用户，角色，

管理密钥：控制面板->删除您的根访问密钥->管理安全证书->访问密钥->创建新的访问密钥

> 使用访问密钥从 AWS CLI、PowerShell 工具、AWS 开发工具包以编程方式调用 AWS，或者直接进行 AWS API 调用

## EC2创建安全组

EC2 : Elastic Compute Cloud

https://console.aws.amazon.com/ec2

```text
1. 转到您的Amazon EC2控制台，然后在左侧导航窗格中的网络和安全下，选择安全组。
2. 选择创建安全组按钮。
3. 输入redshift_security_group对安全组的名称。
4. 在“ 描述 ”中输入“授权redshift集群访问” 。
5. 选择“ 安全性组规则”下的“ 入站”选项卡。
6. 单击添加规则，然后输入以下值：
类型：自定义TCP规则。
通讯协定：TCP。
端口范围：5439。Amazon Redshift的默认端口为5439，但您的端口可能有所不同。
来源：选择“自定义IP”，然后键入0.0.0.0/0。
重要：除了演示目的外，不建议使用0.0.0.0/0，因为它允许从Internet上的任何计算机进行访问。在实际环境中，您将根据自己的网络设置创建入站规则。
7. 选择创建。
```

## RedShift集群

redshift是数据库集群，是**列导向**的

https://console.aws.amazon.com/redshift/

**确保每次完成工作后都删除集群，以免给自己造成巨大的意外费用**

```text
1. 在Amazon Redshift仪表板上，选择启动集群。
2. 在“群集详细信息”页面上，输入以下值，然后选择“继续”：
集群标识符：输入redshift-cluster。
数据库名称：输入dev。
数据库端口：输入5439。
数据库主用户名：输入awsuser。
数据库主用户密码和确认密码：输入主用户帐户的密码。
3. 在“节点配置”页面上，接受默认值，然后选择“ 继续”。
4. 在“其他配置”页面上，输入以下值：选择继续。
VPC安全组：redshift_security_group
可用的IAM角色：myRedshiftRole
5. 查看您的群集配置，然后选择启动群集。
6. 将会出现一个确认页面，集群将需要几分钟才能完成。在左侧导航窗格中选择“ 群集 ”以返回到群集列表。
7. 在“群集”页面上，查看刚启动的群集，然后查看“ 群集状态”信息。确保群集状态是可以与数据库运行状况是健康的您稍后尝试连接到数据库之前。您可能需要5到10分钟。
```

## S3

https://console.aws.amazon.com/s3/

```text
创建存储桶
1. 转到Amazon S3控制台，然后选择创建存储桶
2. 输入存储桶的名称，然后选择要在其中创建存储桶的区域。
3. 保留默认设置，然后选择下一步。
4. 指定此存储桶的公共访问设置。例如，取消选中所有这些框将使任何人都可以访问该存储桶。请注意这一点-如果您共享此链接并且很多人使用它访问大量数据，则可能最终需要从存储桶中进行数据传输时支付大量费用。
5. 查看您的设置，然后选择创建存储桶。

删除存储桶
1. 要删除存储桶，请选择存储桶，然后单击删除。
2. 然后，输入存储桶的名称，然后选择确认。

上传到存储桶
1. 点击刚刚创建的存储桶，然后选择左上角的上传。
2. 选择添加文件，然后选择log_data.csv
3. 保留默认设置，然后选择下一步。
4. 选择标准存储类别，然后选择下一步。
5. 查看您的设置，然后选择上传。
6. 通过在存储桶中选择文件，您应该能够查看有关刚刚上传的文件的详细信息。
```

查看s3中目录

udacity-dend.s3.us-west-2.amazonaws.com

查看s3中文件

udacity-dend.s3.us-west-2.amazonaws.com/log_json_path.json

## RDS-Relational Database Service

https://console.aws.amazon.com/rds/

```text
使用RDS创建PostgreSQL数据库实例
1. 转到Amazon RDS控制台，然后单击左侧导航窗格上的数据库。在顶部菜单栏的右侧选择要创建此数据库的区域。
2. 单击创建数据库按钮
3. 在“选择引擎”页面上选择PostgreSQL。
4. 在“用例”下选择“ 开发/测试 ”。
5. 指定数据库配置
对于数据库实例类，选择db.t2.small
对于数据库实例标识符，请输入postgreSQL-test或您选择的其他名称
输入主用户名和密码
在“ 备份”部分中，选择，1 day因为这是出于演示目的。
保留其余的默认值，然后单击右下角的“ 创建数据库 ”。
6. 单击左侧导航窗格上的“ 数据库 ”以返回到数据库列表。您应该看到状态为正在创建的新创建的数据库。
7. 等待几分钟，以将其更改为“ 可用 ”状态。
```



## EMR

Elastic MapReduce是AWS提供的一项服务，它使您（用户）无需手动安装每台机器的Spark及其依赖项

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

## 

# aws编程控制-boto3

lesson4

