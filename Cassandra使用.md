pip install cassandra-driver

```python
import cassandra
from cassandra.cluster import Cluster
try:
    cluster = Cluster(['127.0.0.1']) 
    session = cluster.connect()
    #Create a keyspace to work in
    session.execute("""
    CREATE KEYSPACE IF NOT EXISTS udacity 
    WITH REPLICATION = 
    { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }""")
    session.set_keyspace('udacity')  #Connect to our Keyspace
    query = "CREATE TABLE IF NOT EXISTS music_library "
	query = query + "(year int, artist_name text, album_name text, PRIMARY KEY (year, artist_name))"
    session.execute(query)
    query = "INSERT INTO music_library (year, artist_name, album_name)"
	query = query + " VALUES (%s, %s, %s)"
    session.execute(query, (1970, "The Beatles", "Let it Be"))
    session.execute(query, (1965, "The Beatles", "Rubber Soul"))
    query = 'SELECT * FROM music_library'
    rows = session.execute(query)
    for row in rows:
    	print (row.year, row.album_name, row.artist_name)
    query = "select * from music_library WHERE YEAR=1970"
    rows = session.execute(query)
    query = "drop table music_library"
    rows = session.execute(query)
    session.shutdown()
	cluster.shutdown()
except Exception as e:
    print(e)
```

主键：

- 必须是唯一的
- 主键仅由分区键组成，或者还可以包括其他群集列
- “简单主键”只是一列，也是“分区键”。复合主键由多列组成，将有助于创建唯一值和您的检索查询
- PARTITION KEY将确定整个系统中的数据分布

### WHERE子句

- Apache Cassandra中的数据建模以查询为重点，并且该重点需要放在WHERE子句上
- 不包含WHERE子句将导致错误