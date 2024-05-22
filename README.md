# Agent tool SQL Coder

## Tool

Can 
  - Connect to database and extract schema

## Usage

```python
python main.py fetch_databases <database_url>
python main.py fetch_tables <database_url>
python script.py query <database_url> "<query_string>"

```

```python
export DB_USER= ...
export DB_PASS= ...

source .venv/bin/activate .
python main.py tables postgresql+psycopg2://$DB_USER:$DB_PASS@localhost cap public
```
