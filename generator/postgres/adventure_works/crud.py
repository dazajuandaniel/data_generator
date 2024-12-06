"""
File to handle DB Connection and Insert Faker Data operations on the database.
"""
import os
import random
from datetime import datetime, timedelta
import warnings
from sqlalchemy.exc import SAWarning, IntegrityError
from faker import Faker
from sqlalchemy import MetaData, select, and_, text, insert
from loguru import logger as loggy
from dotenv import load_dotenv
from generator.db.session import Databaseobject

load_dotenv(dotenv_path='.env')

# Initialize Faker
faker = Faker()


class AdventureWorks:
    def __init__(self, schema:str, table:str):
        # Initialize the Database
        self.db = Databaseobject()
        self.engine = self.db.get_engine()
        self.schema = schema
        self.table  = table

        # Create a metadata object
        self.metadata = MetaData()

        # Ignore terminal xml type warnings
        warnings.filterwarnings("ignore", category=SAWarning)

        # Reflect the ORM
        with self.engine.connect() as conn:
            # Need to reflect every schema here to load tables from db
            self.metadata.reflect(bind=self.engine, schema=self.schema)
            self.metadata.reflect(bind=self.engine, schema="sales")
            self.metadata.reflect(bind=self.engine, schema="person")
            self.metadata.reflect(bind=self.engine, schema="humanresources")
            self.metadata.reflect(bind=self.engine, schema="production")
            self.metadata.reflect(bind=self.engine, schema="purchasing")

    def populate_customer(self, insert_count: int):
        """Add data to the customer table."""

        # Add all the tables needed
        customer_table = self.metadata.tables["sales.customer"]

        # Execute the setval query to set PK sequence to fix id duplicate error
        setval_query = """SELECT setval('sales.customer_customerid_seq', (SELECT MAX(customerid) FROM sales.customer))"""

        # Generate faker data
        with self.engine.connect() as connection:
            # connection.execute(setval_query)
            person = connection.execute(text(f'SELECT businessentityid FROM person.person ORDER BY random() limit {insert_count}'))
            person_as_dict = person.mappings().all()
            persons = [int(x["businessentityid"]) for x in person_as_dict]

            salesstore = connection.execute(text(f'SELECT businessentityid FROM sales.store ORDER BY random() limit {insert_count}'))
            salesstore_as_dict = salesstore.mappings().all()
            stores = [int(x["businessentityid"]) for x in salesstore_as_dict]

            salesterritory = connection.execute(text(f'SELECT territoryid FROM sales.salesterritory ORDER BY random() limit {insert_count}'))
            salesterritory_as_dict = salesterritory.mappings().all()
            salesterritories = [int(x["territoryid"]) for x in salesterritory_as_dict]

            # Run Insert Statement
            total = 0
            for _ in range(int(insert_count)):
                stmt = insert(customer_table).values(
                    customerid=random.choice(persons), 
                    personid=None,
                    storeid=random.choice(stores),
                    territoryid=random.choice(salesterritories),
                    rowguid=faker.uuid4(),
                    modifieddate=datetime.now()

                    )
                try:
                    connection.execute(stmt)
                    connection.commit()
                    total = total +1
                except Exception as e:
                    connection.rollback()
                    pass

        loggy.info(f"Added {total} records")

    def insert(self, num_of_records:int):
        """Runs the insert table"""
        switcher = {
            "customer": self.populate_customer
        }

        if self.table not in switcher.keys():
            loggy.error(f"{self.table} table is not supported")
            return 

        switcher[self.table](num_of_records)

        loggy.info(f"Success adding records to {self.schema}.{self.table}")

