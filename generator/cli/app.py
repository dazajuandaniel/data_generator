import os
import typer
import logging
from generator.postgres.adventure_works.crud import AdventureWorks
app = typer.Typer()

@app.callback()
def callback():
    """CLI to manage the creation of data"""


@app.command(name="create-record")
def create_record(
    schema: str = typer.Option(
        "public",
        "--database",
        help="The name of the database to create a record in"
    ),
    table: str = typer.Option(
        "--table",
        help="The name of the table to create a record in"
    ),
    num_records: str = typer.Option(
        "--num-records",
        help="The number of records to create"
    )
):
    """Creates a record in the specified table"""
    generate_data = AdventureWorks(schema=schema, table=table)
    generate_data.insert(num_records)

