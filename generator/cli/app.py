import os
import typer
import logging
from generator.postgres.adventure_works.crud import GenerateData
app = typer.Typer()

@app.callback()
def callback():
    """CLI to manage the creation of data"""


@app.command(name="create-record")
def create_record(
    database: str = typer.Option(
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
    print("Success")
    generate_data = GenerateData(table=table, num_records=num_records)
    generate_data.create_data()

