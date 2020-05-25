PyCK
===

ClickHouse operation and query kit

Installation
---

`pip3 install --user .`

Usage
---

```python
import ck

# start a local ClickHouse server
# the default data directory is ~/.ck_data
session = ck.LocalSession()
# use PassiveSession if you just want a ClickHouse client
# session = ck.PassiveSession(host='192.168.xxx.xxx')

# ping the server
# it will return True
print(session.ping())

# make a simple query
# the result will be returned as bytes
print(session.query('select 1'))

# pretty print
print(session.query('select 1 as x, 2 as y format Pretty').decode())

# create a table
session.query('create table test (x Int32) engine=Memory')

# load data from a file to a table
session.query_file(
    'insert into test format CSV',
    path_in='1.csv'
)

# load data from a table to a dataframe
print(session.query_pandas('select * from test'))

# make an async query
join = session.query_async('select 1')
print(join())
```
