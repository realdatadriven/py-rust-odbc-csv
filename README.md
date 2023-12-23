# RUST ODBC 2 CSV

Extracting data from ODBC Source to CSV using Rust, made to be memory eficient and fast!

```python
import os, json
from py_rust_odbc_csv import odbc_csv
```

## REQUIRED PARAMETERS

the required parameters are the ODBC connection string and the query to be executed, but you can also pass the bach size and the desired filename

```python
conn = f"Driver={{ODBC Driver NAME}};System=host;Uid={os.environ['USERNAME']};Pwd={os.environ['PASS']}"
query = "SELECT * FROM \"DB\".\"TABLE\""
```

## EXCUTION

To execute we call the odbc_csv function and pass the list of arguments:\

- 1st ODBC Connection String;
- 2nd Query String;
- 3rd Bach Size, Number of rows to load at a time (Optional);
- 4rd Required File Name, if not the csv file will be generate with a UUIDv4 name (Optional);

The result will be in JSON format (```json {success: bool, msg: str, fname: str (path)}```)

```python
res = odbc_csv([conn, query])
r = json.loads(res)
```
