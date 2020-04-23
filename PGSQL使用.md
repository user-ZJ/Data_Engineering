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

cur.execute("CREATE TABLE IF NOT EXISTS songs (song_title varchar, artist_name varchar, year int, album_name varchar, single Boolean);")
cur.execute("INSERT INTO music_library (album_name, artist_name, year) \
                 VALUES (%s, %s, %s)", \
                 ("Let It Be", "The Beatles", 1970))
cur.close()
conn.close()
```

