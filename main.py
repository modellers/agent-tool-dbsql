import click
import db_query


# CLI setup with click
@click.group()
def cli():
    pass


@cli.command(
    name="databases",
    help="Show all databases in a database server"

)
@click.argument('database_url')
def fetch_databases_cmd(database_url):
    engine = db_query.get_engine(database_url)
    databases = db_query.fetch_databases(engine)
    click.echo(databases)


@cli.command(
    name="database-schemas",
    help="Show all schemas in a database"
)
@click.argument('database_url')
@click.argument('database_name')
def fetch_schemas_cmd(database_url, database_name):
    engine = db_query.get_engine(database_url + "/" + database_name)
    databases = db_query.fetch_schemas(engine)
    click.echo(databases)


@cli.command(
    name="schema-tables",
    help="Show all tables in a schema along with their create table statements"
)
@click.argument('database_url')
@click.argument('database_name')
@click.argument('schema_name')
def fetch_schema_tables_cmd(database_url, database_name, schema_name):
    engine = db_query.get_engine(database_url + "/" + database_name)
    tables = db_query.fetch_schema_tables(engine, schema_name)
    click.echo(f"\n{tables}\n")


@cli.command(
    name="tables",
    help="Show the create table statement for each table in a schema"
)
@click.argument('database_url')
@click.argument('database_name')
@click.argument('schema_name')
def show_tables_cmd(database_url, database_name, schema_name):
    engine = db_query.get_engine(database_url + "/" + database_name)
    create_tables = db_query.show_tables(engine, schema_name)
    # pretty print the create table statements
    for table, create_table in create_tables.items():
        click.echo(f"\n{create_table}\n")
    # click.echo(f"\n{create_tables}\n")


@cli.command(
    name="query",
    help="Execute a query and return the results as a JSON string"
)
@click.argument('database_url')
@click.argument('database_name')
@click.argument('query_string')
def query_cmd(database_url, database_name, query_string):
    engine = db_query.get_engine(database_url)
    result = db_query.execute_query(engine, query_string)
    click.echo(result)


if __name__ == '__main__':
    cli()
