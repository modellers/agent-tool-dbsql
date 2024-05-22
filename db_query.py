from sqlalchemy import create_engine, inspect, text

# Function to create a database connection
def get_engine(database_url):
    return create_engine(database_url)

# Function to fetch all databases (assuming PostgreSQL)
def fetch_databases(engine):
    with engine.connect() as connection:
        result = connection.execute(text("SELECT datname FROM pg_database WHERE datistemplate = false;"))
        databases = [row[0] for row in result]  # Accessing by index, not by string
    return databases

def fetch_schemas(engine):
    with engine.connect() as connection:
        result = connection.execute(text(f"SELECT schema_name FROM information_schema.schemata;"))
        schemas = [row[0] for row in result]
    return schemas

def fetch_schema_tables(engine, schema):
    with engine.connect() as connection:
        result = connection.execute(text(f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{schema}';"))
        tables = [row[0] for row in result]
    return tables

# Function to fetch all table schemas in a database along with their relations
def show_tables(engine, schema):
    tables = fetch_schema_tables(engine, schema)
    tables = {table: get_create_table(engine, table, schema) for table in tables}
    return tables

# Function to get the create table statement for each table
def get_create_table(engine, table_name, schema):
    query = text(f"""
SELECT 
    c.table_name, 
    c.column_name, 
    c.data_type, 
    c.is_nullable, 
    c.column_default,
    pgd.description AS column_comment,
    pgd_table.description AS table_comment
FROM 
    information_schema.columns c
LEFT JOIN 
    pg_catalog.pg_statio_all_tables AS st 
    ON c.table_schema = st.schemaname AND c.table_name = st.relname
LEFT JOIN 
    pg_catalog.pg_description AS pgd 
    ON pgd.objoid = st.relid AND pgd.objsubid = c.ordinal_position
LEFT JOIN
    pg_catalog.pg_class AS pc
    ON pc.relname = c.table_name
LEFT JOIN
    pg_catalog.pg_description AS pgd_table
    ON pgd_table.objoid = pc.oid AND pgd_table.objsubid = 0
WHERE 
    c.table_schema = :schema AND c.table_name = :table_name;

    """)
    pretty_print = True
    with engine.connect() as connection:
        result = connection.execute(query, {'schema': schema, 'table_name': table_name})
        columns = [dict(row) for row in result.mappings()]

    if (pretty_print):
        table_comment = ""
        if columns[0]['table_comment'] is not None:
            table_comment = f"-- {columns[0]['table_comment']}\n"
        create_table_stmt = f"{table_comment}CREATE TABLE {schema}.{table_name} (\n"
        col_definitions = []
        for col in columns:
            new_line = "," 
            if col == columns[-1]:
                new_line = ""
            col_def = f"    {col['column_name']} {col['data_type'].upper()}"
            if col['is_nullable'] == 'NO':
                col_def += " NOT NULL"
            if col['column_default'] is not None:
                col_def += f" DEFAULT {col['column_default']}"
            col_def += new_line
            # add column comment
            if col['column_comment'] is not None:
                column_comment = col['column_comment'].replace("\n", " ")
                col_def += f" -- {column_comment}"
            col_definitions.append(col_def)
        create_table_stmt += "\n".join(col_definitions) + "\n);"
    else:
        return {}
    return create_table_stmt

# Function to execute a query
def execute_query(engine, query_string):
    with engine.connect() as connection:
        result = connection.execute(text(query_string))
        # Converting result to a list of dictionaries
        results_list = [dict(row) for row in result.mappings()]
    return results_list
