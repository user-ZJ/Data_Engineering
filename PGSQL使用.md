```python
try:
	conn = psycopg2.connect("host=127.0.0.1 dbname=studentdb user=student password=student")
except psycopg2.Error as e: 
    print("Error: Could not make connection to the Postgres database")
    print(e)
try:
	cur = conn.cursor()
except psycopg2.Error as e: 
    print("Error: Could not get curser to the Database")
    print(e)
conn.set_session(autocommit=True)
try:
	cur.execute("CREATE TABLE test (col1 int, col2 int, col3 int);")
	cur.execute("select * from test")
	cur.execute("select count(*) from test")
	print(cur.fetchall())
except psycopg2.Error as e:
    print(e)

cur.execute("CREATE TABLE IF NOT EXISTS songs (song_title varchar, artist_name varchar, year int, album_name varchar, single Boolean,label text[],descript text);")
cur.execute("INSERT INTO music_library (album_name, artist_name, year) \
                 VALUES (%s, %s, %s)", \
                 ("Let It Be", "The Beatles", 1970))
cur.execute("SELECT * FROM transactions JOIN albums_sold ON transactions.transaction_id = albums_sold.transaction_id ;")
cur.execute("select * from account where acctid=%s and money>%s",(acctid, money))
cur.execute("DROP table music_store")
cur.close()
conn.close()
```

数据约束：非空，UNIQUE，主键

```python
CREATE TABLE IF NOT EXISTS customer_transactions (
    customer_id int NOT NULL, 
    store_id int NOT NULL, 
    spent numeric
);

CREATE TABLE IF NOT EXISTS customer_transactions (
    customer_id int NOT NULL UNIQUE, 
    store_id int NOT NULL UNIQUE, 
    spent numeric 
);
CREATE TABLE IF NOT EXISTS customer_transactions (
    customer_id int NOT NULL, 
    store_id int NOT NULL, 
    spent numeric,
    UNIQUE (customer_id, store_id, spent)
);

CREATE TABLE IF NOT EXISTS store (
    store_id int PRIMARY KEY, 
    store_location_city text,
    store_location_state text
);
#组合键
CREATE TABLE IF NOT EXISTS customer_transactions (
    customer_id int, 
    store_id int, 
    spent numeric,
    PRIMARY KEY (customer_id, store_id)
);
```

条件插入或更新：

```python
INSERT INTO customer_address (customer_id, customer_street, customer_city, customer_state)
VALUES
 (
 432, '923 Knox Street', 'Albany', 'NY'
 ) 
ON CONFLICT (customer_id) 
DO NOTHING;

INSERT INTO customer_address (customer_id, customer_street)
VALUES
    (
    432, '923 Knox Street, Suite 1' 
) 
ON CONFLICT (customer_id) 
DO UPDATE
    SET customer_street  = EXCLUDED.customer_street;
```

