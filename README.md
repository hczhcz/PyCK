PyCK
===

Installation
---

`pip3 install --user .`

Usage
---

```python
import ck
from ck import iteration
import pandas as pd

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

# read data from a file
session.query(
    'insert into test format CSV',
    gen_in=iteration.file_in('1.csv')
)

# write data to a file
session.query(
    'select * from test format CSV',
    gen_out=iteration.file_out('2.csv')
)
```
