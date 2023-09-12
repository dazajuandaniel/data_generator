"""
File to handle DB Connection and Insert Faker Data operations on the database.
"""
import os
import random
from datetime import datetime, timedelta
import warnings
from sqlalchemy.exc import SAWarning, IntegrityError
from sqlalchemy import create_engine, MetaData, select, and_, update, func
from faker import Faker
from logger import get_logger
from dotenv import load_dotenv

BASEDIR = os.path.abspath(os.path.dirname(__file__))
load_dotenv(dotenv_path=os.path.join(BASEDIR, '../.env'))

loggy = get_logger(__name__)

# Provide the PostgreSQL connection details
host = os.getenv("PGHOST")
database = os.getenv("ADVENTUREWORKSNAME")
user = os.getenv("PGUSER")
password = os.getenv("PGPASSWORD")
port = os.getenv("PGPORT")

# Create the SQLAlchemy engine
engine = create_engine(
    f"postgresql://{user}:{password}@{host}:{port}/{database}")

# Initialize Faker
faker = Faker()

# Create a metadata object
metadata = MetaData(bind=engine)

# Ignore terminal xml type warnings
warnings.filterwarnings("ignore", category=SAWarning)

# Reflect metadata/schema from existing postgres database
with engine.connect() as conn:
    metadata.reflect(bind=engine)
    # Need to reflect every schema here to load tables from db
    metadata.reflect(schema="sales")
    metadata.reflect(schema="person")
    metadata.reflect(schema="humanresources")
    metadata.reflect(schema="production")
    metadata.reflect(schema="purchasing")


######### Create table objects############
# Sales Schema's tables
currency_table = metadata.tables["sales.currency"]
countryregioncurrency_table = metadata.tables["sales.countryregioncurrency"]
creditcard_table = metadata.tables["sales.creditcard"]
currencyrate_table = metadata.tables["sales.currencyrate"]
salesterritory_table = metadata.tables["sales.salesterritory"]
store_table = metadata.tables["sales.store"]
customer_table = metadata.tables["sales.customer"]
personcreditcard_table = metadata.tables["sales.personcreditcard"]
salesorderdetail_table = metadata.tables["sales.salesorderdetail"]
specialofferproduct_table = metadata.tables["sales.specialofferproduct"]
salesorderheader_table = metadata.tables["sales.salesorderheader"]
specialoffer_table = metadata.tables["sales.specialoffer"]
salesorderheadersalesreason_table = metadata.tables["sales.salesorderheadersalesreason"]
salesreason_table = metadata.tables["sales.salesreason"]
salesperson_table = metadata.tables["sales.salesperson"]
salespersonquotahistory_table = metadata.tables["sales.salespersonquotahistory"]
salestaxrate_table = metadata.tables["sales.salestaxrate"]
salesterritoryhistory_table = metadata.tables["sales.salesterritoryhistory"]
shoppingcartitem_table = metadata.tables["sales.shoppingcartitem"]
# Person Schema tables
countryregion_table = metadata.tables["person.countryregion"]
person_table = metadata.tables["person.person"]
address_table = metadata.tables["person.address"]
stateprovince_table = metadata.tables["person.stateprovince"]
businessentity_table = metadata.tables["person.businessentity"]
# Production Schema tables
product_table = metadata.tables["production.product"]
# Humanresources Schema tables
employee_table = metadata.tables["humanresources.employee"]
# Purchasing Schema tables
shipmethod_table = metadata.tables["purchasing.shipmethod"]


class Data:
    def __init__(self):
        pass

    @staticmethod
    def populate_countryregioncurrency(action: str, insert_count: int):
        """Add faker data to the countryregioncurrency table."""

        # Generate fake data
        with engine.connect() as connection:
            # FK's
            currencies = connection.execute(
                select([currency_table.c.currencycode])
            ).fetchall()
            # country_regions = connection.execute(
            #     select([countryregion_table.c.countryregioncode])
            # ).fetchall()

            # Fetch existing records
            existing_records = connection.execute(
                select([countryregioncurrency_table])
            ).fetchall()

            # For each record, generate new fake data and update
            for _ in range(insert_count):
                existing_record = random.choice(existing_records)
                fake_data = {
                    "countryregioncode": existing_record.countryregioncode,
                    "currencycode": random.choice(currencies)[0],
                }

                if action == "create":
                    connection.execute(
                        countryregioncurrency_table.insert(), fake_data)
                elif action == "edit":
                    update_query = (
                        update(countryregioncurrency_table)
                        .where(countryregioncurrency_table.c.countryregioncode == fake_data["countryregioncode"])
                        .values(fake_data)
                    )
                    connection.execute(update_query)

    @staticmethod
    def populate_creditcard(action: str, insert_count: int):
        """Add data to the creditcard table."""

        # Execute the setval query to set PK sequence
        setval_query = """SELECT setval('sales.creditcard_creditcardid_seq', 
                            (SELECT MAX(creditcardid) FROM sales.creditcard))"""

        insert_data = []
        for _ in range(insert_count):
            expire_date = faker.credit_card_expire(start="now", end="+10y")
            expire_month, expire_year = map(int, expire_date.split("/"))
            # Get the current year
            current_year = datetime.now().year
            # Convert the expiration year to full year format
            expire_year += current_year // 100 * 100 if expire_year <= 99 else 0

            insert_data.append(
                {
                    "cardtype": faker.credit_card_provider(card_type=None),
                    "cardnumber": faker.credit_card_number(card_type=None),
                    "expmonth": expire_month,
                    "expyear": expire_year,
                    "modifieddate": datetime.now(),
                }
            )

        # Execute the insert statement
        with engine.begin() as connection:
            if action == "create":
                connection.execute(setval_query)
                connection.execute(creditcard_table.insert(), insert_data)
            elif action == "edit":
                # Get the total number of records in the table
                total_records = connection.execute(
                    select([func]).select_from(creditcard_table)
                ).scalar()
                # Update a random selection of records
                records_to_update = random.sample(
                    range(1, total_records + 1), insert_count)
                for indx, record_id in enumerate(records_to_update):
                    update_query = (
                        update(creditcard_table)
                        .where(creditcard_table.c.creditcardid == record_id)
                        .values(insert_data[indx])
                    )
                    connection.execute(update_query)

    @staticmethod
    def populate_currency(action: str, insert_count: int):
        """Add data to the currency table."""

        # Generate fake data
        insert_data = [
            {"currencycode": faker.text()[:3].upper(),
             "name": faker.currency_name(),
             "modifieddate": datetime.now(),
             }
            for _ in range(insert_count)
        ]

        # Execute the insert statement
        with engine.begin() as connection:
            if action == "create":
                connection.execute(currency_table.insert(), insert_data)
            elif action == "edit":
                # Fetch existing records randomly to update
                existing_records = connection.execute(
                    select([currency_table])
                    .order_by(func.random())
                    .limit(insert_count)
                ).fetchall()

                # Iterate over the records to update
                for existing_record, new_data in zip(existing_records, insert_data):
                    del new_data["currencycode"]
                    # Update the record in the currency table
                    update_currency_query = (
                        update(currency_table)
                        .where(currency_table.c.currencycode == existing_record.currencycode)
                        .values(new_data)
                    )
                    connection.execute(update_currency_query)
            else:
                raise ValueError("Invalid action provided")

    @staticmethod
    def populate_currencyrate(action: str, insert_count: int):
        """Add data to the currencyrate table."""

        # Execute the setval query to set PK sequence
        setval_query = """SELECT setval('sales.currencyrate_currencyrateid_seq', 
                                    (SELECT MAX(currencyrateid) FROM sales.currencyrate))"""

        with engine.connect() as connection:
            # FK's
            currencies = connection.execute(
                select([currency_table.c.currencycode])
            ).fetchall()
            # Generate faker data
            fakedata = [
                {
                    "currencyratedate": datetime.now(),
                    "fromcurrencycode": random.choice(currencies)[0],
                    "tocurrencycode": random.choice(currencies)[0],
                    "averagerate": random.randint(1, 1500),
                    "endofdayrate": random.randint(1, 1500),
                }
                for _ in range(insert_count)
            ]

            if action == "create":
                connection.execute(setval_query)
                connection.execute(currencyrate_table.insert(), fakedata)
            elif action == "edit":
                # Get the total number of records in the table
                total_records = connection.execute(
                    select([func]).select_from(currencyrate_table)
                ).scalar()

                # Update a random selection of records
                records_to_update = random.sample(
                    range(1, total_records + 1), insert_count)

                for indx, record_id in enumerate(records_to_update):
                    update_query = (
                        update(currencyrate_table)
                        .where(currencyrate_table.c.currencyrateid == record_id)
                        .values(fakedata[indx])
                    )
                    connection.execute(update_query)

    @staticmethod
    def populate_customer(action: str, insert_count: int):
        """Add data to the customer table."""

        # Execute the setval query to set PK sequence to fix id duplicate error
        setval_query = """SELECT setval('sales.customer_customerid_seq', 
                                    (SELECT MAX(customerid) FROM sales.customer))"""
        # Generate faker data
        with engine.connect() as connection:
            # FK's
            persons = connection.execute(
                select([person_table.c.businessentityid])
            ).fetchall()
            stores = connection.execute(
                select([store_table.c.businessentityid])
            ).fetchall()
            salesterritories = connection.execute(
                select([salesterritory_table.c.territoryid])
            ).fetchall()

            fake_data = [
                {
                    "personid": random.choice(persons)[0],
                    "storeid": random.choice(stores)[0],
                    "territoryid": random.choice(salesterritories)[0],
                    "rowguid": faker.uuid4(),
                    "modifieddate": datetime.now(),
                }
                for _ in range(insert_count)
            ]

            connection.execute(setval_query)
            if action == "create":
                connection.execute(customer_table.insert(), fake_data)
            elif action == "edit":
                # Get the total number of records in the table
                total_records = connection.execute(
                    select([func.count()]).select_from(customer_table)
                ).scalar()

                # Update a random selection of records
                records_to_update = random.sample(
                    range(1, total_records + 1), insert_count)

                for indx, record_id in enumerate(records_to_update):
                    update_query = (
                        update(customer_table)
                        .where(customer_table.c.customerid == record_id)
                        .values(fake_data[indx])
                    )
                    connection.execute(update_query)

    @staticmethod
    def populate_personcreditcard(action: str, insert_count: int):
        """Add data to the customer table."""

        # Generate faker data
        with engine.connect() as connection:
            # FK's
            persons = connection.execute(
                select([person_table.c.businessentityid])
            ).fetchall()
            creditcards = connection.execute(
                select([creditcard_table.c.creditcardid])
            ).fetchall()

            fake_data = [
                {
                    "businessentityid": random.choice(persons)[0],
                    "creditcardid": random.choice(creditcards)[0],
                    "modifieddate": datetime.now(),
                }
                for _ in range(insert_count)
            ]
            if action == "create":
                connection.execute(personcreditcard_table.insert(), fake_data)
            elif action == "edit":
                # Get the total number of records in the table
                total_records = connection.execute(
                    select([func.count()]).select_from(personcreditcard_table)
                ).scalar()

                # Update a random selection of records
                records_to_update = random.sample(
                    range(1, total_records + 1), insert_count)

                for indx, record_id in enumerate(records_to_update):
                    update_query = (
                        update(personcreditcard_table)
                        .where(personcreditcard_table.c.businessentityid == record_id)
                        .values(fake_data[indx])
                    )
                    connection.execute(update_query)

    @staticmethod
    def populate_salesorderdetail(action: str, insert_count: int):
        # issue
        """Add data to the salesorderdetail table."""

        # Execute the setval query to set PK sequence to fix id duplicate error
        setval_query = """SELECT setval('sales.salesorderdetail_salesorderdetailid_seq', 
                            (SELECT MAX(salesorderdetailid) FROM sales.salesorderdetail))"""

        # Generate faker data
        with engine.connect() as connection:
            # FK's
            specialoffer_products = connection.execute(
                select(
                    [
                        specialofferproduct_table.c.specialofferid,
                        specialofferproduct_table.c.productid,
                    ]
                )
            ).fetchall()
            salesorderheaders = connection.execute(
                select([salesorderheader_table.c.salesorderid])
            ).fetchall()

            specialofferproduct = random.choice(specialoffer_products)
            specialoffer_id = specialofferproduct[0]
            product_id = specialofferproduct[1]

            faker_data = [
                {
                    "salesorderid": random.choice(salesorderheaders)[0],
                    "carriertrackingnumber": str(faker.uuid4())[:12],
                    "orderqty": random.randint(1, 20),
                    "productid": product_id,
                    "specialofferid": specialoffer_id,
                    "unitprice": random.randint(1, 100),
                    "unitpricediscount": random.randint(0, 10),
                    "rowguid": faker.uuid4(),
                    "modifieddate": datetime.now(),
                }
                for _ in range(insert_count)
            ]

            try:
                connection.execute(setval_query)
                if action == "create":
                    connection.execute(
                        salesorderdetail_table.insert(), faker_data)
                elif action == "edit":
                    # Get the total number of records in the table
                    total_records = connection.execute(
                        select([func.count()]).select_from(
                            salesorderdetail_table)
                    ).scalar()

                    # Update a random selection of records
                    records_to_update = random.sample(
                        range(1, total_records + 1), insert_count)

                    for indx, record_id in enumerate(records_to_update):
                        update_query = (
                            update(salesorderdetail_table)
                            .where(salesorderdetail_table.c.salesorderid == record_id)
                            .values(faker_data[indx])
                        )
                        connection.execute(update_query)
                else:
                    raise ValueError("Invalid action provided")
            except IntegrityError as e:
                # Handle the unique constraint violation error
                loggy.info("Skipping duplicate key value:", e)

    @staticmethod
    def populate_salesorderheader(action: str, insert_count: int):
        # issue
        """Add data to the salesorderheader table."""

        # Execute the setval query to set PK sequence to fix id duplicate error
        setval_query = """SELECT setval('sales.salesorderheader_salesorderid_seq',
                             (SELECT MAX(salesorderid) FROM sales.salesorderheader))"""

        # Generate faker data
        with engine.connect() as connection:
            # FK's
            customers = connection.execute(
                select([customer_table.c.customerid])
            ).fetchall()
            salespersons = connection.execute(
                select([salesperson_table.c.businessentityid])
            ).fetchall()
            salesterritories = connection.execute(
                select([salesterritory_table.c.territoryid])
            ).fetchall()
            person_addresses = connection.execute(
                select([address_table.c.addressid])
            ).fetchall()
            shipmethods = connection.execute(
                select([shipmethod_table.c.shipmethodid])
            ).fetchall()
            creditcards = connection.execute(
                select([creditcard_table.c.creditcardid])
            ).fetchall()
            currencyrates = connection.execute(
                select([currencyrate_table.c.currencyrateid])
            ).fetchall()

            fake_data = [
                {
                    "revisionnumber": random.randint(1, 10),
                    "orderdate": datetime.now(),
                    "duedate": datetime.now()
                    + timedelta(days=random.randint(1, 10)),
                    "shipdate": datetime.now()
                    + timedelta(days=random.randint(1, 5)),
                    "status": random.randint(1, 5),
                    "onlineorderflag": random.randint(0, 1),
                    "purchaseordernumber": str(faker.uuid4())[:6],
                    "accountnumber": str(faker.uuid4())[:12],
                    "customerid": random.choice(customers)[0],
                    "salespersonid": random.choice(salespersons)[0],
                    "territoryid": random.choice(salesterritories)[0],
                    "billtoaddressid": random.choice(person_addresses)[0],
                    "shiptoaddressid": random.choice(person_addresses)[0],
                    "shipmethodid": random.choice(shipmethods)[0],
                    "creditcardid": random.choice(creditcards)[0],
                    "creditcardapprovalcode": str(faker.uuid4()).replace("-", "")[
                        :12
                    ],
                    "currencyrateid": random.choice(currencyrates)[0],
                    "subtotal": random.randint(1000, 20000),
                    "taxamt": random.randint(1100, 20000),
                    "freight": random.randint(100, 1000),
                    "totaldue": random.randint(100, 25000),
                    "comment": faker.sentence(
                        nb_words=10, variable_nb_words=True, ext_word_list=None
                    ).strip("."),
                    "rowguid": faker.uuid4(),
                    "modifieddate": datetime.now(),
                }
                for _ in range(insert_count)
            ]
            connection.execute(setval_query)
            if action == "create":
                connection.execute(salesorderheader_table.insert(), fake_data)
            elif action == "edit":
                # Fetch existing records to update
                records_to_update = connection.execute(
                    select([salesorderheader_table]).limit(insert_count)
                ).fetchall()

                # Iterate over the records to update
                for existing_record, new_data in zip(records_to_update, fake_data):
                    # Update the record in the salesorderheader table
                    update_salesorderheader_query = (
                        update(salesorderheader_table)
                        .where(salesorderheader_table.c.salesorderid == existing_record.salesorderid)
                        .values(new_data)
                    )
                    connection.execute(update_salesorderheader_query)
            else:
                raise ValueError("Invalid action provided")

    @staticmethod
    def populate_salesorderheadersalesreason(action: str, insert_count: int):
        """Add data to the salesorderheadersalesreason table."""

        # Generate faker data
        with engine.connect() as connection:
            # FK's
            sales_reasons = connection.execute(
                select([salesreason_table.c.salesreasonid])
            ).fetchall()
            salesorderheaders = connection.execute(
                select([salesorderheader_table.c.salesorderid])
            ).fetchall()

            data_to_insert = []
            for _ in range(insert_count):
                salesorderid = random.choice(salesorderheaders)[0]
                salesreasonid = random.choice(sales_reasons)[0]

                # Check if the combination already exists
                existing_data = connection.execute(
                    select([salesorderheadersalesreason_table]).where(
                        and_(
                            salesorderheadersalesreason_table.c.salesorderid
                            == salesorderid,
                            salesorderheadersalesreason_table.c.salesreasonid
                            == salesreasonid,
                        )
                    )
                ).fetchone()

                if existing_data is None:
                    data_to_insert.append(
                        {
                            "salesorderid": salesorderid,
                            "salesreasonid": salesreasonid,
                        }
                    )

            try:
                if action == "create":
                    connection.execute(
                        salesorderheadersalesreason_table.insert(), data_to_insert
                    )
                elif action == "edit":
                    # Fetch existing records to update (randomly)
                    records_to_update = connection.execute(
                        select([salesorderheadersalesreason_table])
                        .order_by(func.random())
                        .limit(insert_count)
                    ).fetchall()

                    # Iterate over the records to update
                    for existing_record, new_data in zip(records_to_update, data_to_insert):
                        # Update the record in the salesorderheadersalesreason table
                        update_salesorderheadersalesreason_query = (
                            update(salesorderheadersalesreason_table)
                            .where(salesorderheadersalesreason_table.c.salesorderid == existing_record.salesorderid)
                            .values(new_data)
                        )
                        connection.execute(
                            update_salesorderheadersalesreason_query)
                else:
                    raise ValueError("Invalid action provided")
            except IntegrityError as e:
                # Handle the unique constraint violation error
                duplicate_key_error_message = str(e.orig)
                loggy.info(
                    f"Skipping duplicate key value: {duplicate_key_error_message}")

    @staticmethod
    def populate_salesperson(action: str, insert_count: int):
        """Add data to the salesperson table."""

        # Generate faker data
        with engine.connect() as connection:
            # FK's
            employees = connection.execute(
                select([employee_table.c.businessentityid])
            ).fetchall()
            salesterritories = connection.execute(
                select([salesterritory_table.c.territoryid])
            ).fetchall()

            try:
                if action == "create":
                    data_to_insert = []
                    existing_businessentity_ids = connection.execute(
                        select([salesperson_table.c.businessentityid])
                    ).fetchall()
                    existing_ids = set([row[0] for row in existing_businessentity_ids])

                    for _ in range(insert_count):
                        businessentityid = random.choice(employees)[0]
                        while businessentityid in existing_ids:
                            businessentityid = random.choice(employees)[0]

                        data_to_insert.append(
                            {
                                "businessentityid": businessentityid,
                                "territoryid": random.choice(salesterritories)[0],
                                "salesquota": faker.random_number(digits=6),
                                "bonus": faker.random_number(digits=5),
                                "commissionpct": faker.random_number(digits=2),
                                "salesytd": faker.random_number(digits=8),
                                "saleslastyear": faker.random_number(digits=8),
                                "rowguid": faker.uuid4(),
                            }
                        )

                        existing_ids.add(businessentityid)

                    connection.execute(
                        salesperson_table.insert(), data_to_insert
                    )
                elif action == "edit":
                    # Fetch existing records to update (randomly)
                    existing_records = connection.execute(
                        select([salesperson_table])
                        .order_by(func.random())
                        .limit(insert_count)
                    ).fetchall()

                    # Iterate over the records to update
                    for existing_record in existing_records:
                        # Check if the businessentityid is still referenced in salesorderheader
                        is_referenced_query = select([salesorderheader_table.c.salesorderid]).where(
                            salesorderheader_table.c.salespersonid == existing_record.businessentityid
                        )
                        is_referenced = connection.execute(is_referenced_query).fetchone()

                        if is_referenced is None:
                            # Generate new data for the update (excluding primary keys)
                            new_data = {
                                "territoryid": random.choice(salesterritories)[0],
                                "salesquota": faker.random_number(digits=6),
                                "bonus": faker.random_number(digits=5),
                                "commissionpct": faker.random_number(digits=2),
                                "salesytd": faker.random_number(digits=8),
                                "saleslastyear": faker.random_number(digits=8),
                                "rowguid": faker.uuid4(),
                            }

                            # Update the record in the salesperson table
                            update_salesperson_query = (
                                update(salesperson_table)
                                .where(salesperson_table.c.businessentityid == existing_record.businessentityid)
                                .values(new_data)
                            )
                            connection.execute(update_salesperson_query)
                        else:
                            # Handle the case where the record is still referenced
                            loggy.info(
                                f"Skipping update for businessentityid {existing_record.businessentityid} as it is still referenced in salesorderheader."
                            )
                else:
                    raise ValueError("Invalid action provided")
            except IntegrityError as e:
                # Handle the unique constraint violation error
                duplicate_key_error_message = str(e.orig)
                loggy.info(
                    f"Skipping duplicate key value: {duplicate_key_error_message}")


    @staticmethod
    def populate_salespersonquotahistory(action: str, insert_count: int):
        """Add data to the salespersonquotahistory table."""

        # Generate faker data
        with engine.connect() as connection:
            # FK's
            sales_persons = connection.execute(
                select([salesperson_table.c.businessentityid])
            ).fetchall()
            fake_data = [
                {
                    "businessentityid": random.choice(sales_persons)[0],
                    "quotadate": faker.date_between(
                        start_date="-10y", end_date="+10y"
                    ),
                    "salesquota": faker.random_number(digits=6),
                    "rowguid": faker.uuid4(),
                    "modifieddate": datetime.now(),
                }
                for _ in range(insert_count)
            ]
            if action == "create":
                connection.execute(
                    salespersonquotahistory_table.insert(), fake_data)
            elif action == "edit":
                # Fetch existing records to update (randomly)
                records_to_update = connection.execute(
                    select([salespersonquotahistory_table])
                    .order_by(func.random())
                    .limit(insert_count)
                ).fetchall()
                # Iterate over the records to update
                for existing_record, new_data in zip(records_to_update, fake_data):
                    # Check if a record with the same businessentityid and quotadate exists
                    existing_query = select([salespersonquotahistory_table]).where(
                        and_(
                            salespersonquotahistory_table.c.businessentityid == existing_record.businessentityid,
                            salespersonquotahistory_table.c.quotadate == new_data['quotadate']
                        )
                    )
                    existing = connection.execute(existing_query).fetchone()
                    if existing:
                        del new_data["businessentityid"]
                        # Update the existing record with new data
                        update_salespersonquotahistory_query = (
                            update(salespersonquotahistory_table)
                            .where(salespersonquotahistory_table.c.businessentityid == existing.businessentityid)
                            .values(new_data)
                        )
                        connection.execute(
                            update_salespersonquotahistory_query)
                    else:
                        loggy.info("Skipping duplicate key value")
            else:
                raise ValueError("Invalid action provided")

    @staticmethod
    def populate_salesreason(action: str, insert_count: int):
        """Add data to the salesreason table."""

        # Execute the setval query to set PK sequence to fix id duplicate error
        setval_query = """SELECT setval('sales.salesreason_salesreasonid_seq', 
                                    (SELECT MAX(salesreasonid) FROM sales.salesreason))"""

        # Generate faker data
        with engine.connect() as connection:
            connection.execute(setval_query)
            fake_data = [
                {
                    "name": faker.word().capitalize(),
                    "reasontype": faker.word().capitalize(),
                    "modifieddate": datetime.now(),
                }
                for _ in range(insert_count)
            ]
            if action == "create":
                connection.execute(salesreason_table.insert(), fake_data)
            elif action == "edit":
                # Fetch existing records to update (randomly)
                records_to_update = connection.execute(
                    select([salesreason_table])
                    .order_by(func.random())
                    .limit(insert_count)
                ).fetchall()

                # Iterate over the records to update
                for existing_record, new_data in zip(records_to_update, fake_data):
                    # Update the record in the salesreason table
                    update_salesreason_query = (
                        update(salesreason_table)
                        .where(salesreason_table.c.salesreasonid == existing_record.salesreasonid)
                        .values(new_data)
                    )
                    connection.execute(update_salesreason_query)
            else:
                raise ValueError("Invalid action provided")

    @staticmethod
    def populate_salestaxrate(action: str, insert_count: int):
        """Add data to the salestaxrate table."""

        # Execute the setval query to set PK sequence to fix id duplicate error
        setval_query = """SELECT setval('sales.salestaxrate_salestaxrateid_seq', 
                                      (SELECT MAX(salestaxrateid) FROM sales.salestaxrate))"""

        # Generate faker data
        with engine.connect() as connection:
            # FK
            stateprovinces = connection.execute(
                select([stateprovince_table.c.stateprovinceid])
            ).fetchall()

            connection.execute(setval_query)

            fake_data = [
                {
                    "stateprovinceid": random.choice(stateprovinces)[0],
                    "taxtype": random.randint(1, 3),
                    "taxrate": random.randint(1, 15),
                    "name": faker.sentence(
                        nb_words=4, variable_nb_words=True, ext_word_list=None
                    ).strip("."),
                    "rowguid": faker.uuid4(),
                    "modifieddate": datetime.now(),
                }
                for _ in range(insert_count)
            ]
            if action == "create":
                connection.execute(salestaxrate_table.insert(), fake_data)
            elif action == "edit":
                # Fetch existing records to update (randomly)
                records_to_update = connection.execute(
                    select([salestaxrate_table])
                    .order_by(func.random())
                    .limit(insert_count)
                ).fetchall()

                # Iterate over the records to update
                for existing_record, new_data in zip(records_to_update, fake_data):
                    # Update the record in the salestaxrate table
                    update_salestaxrate_query = (
                        update(salestaxrate_table)
                        .where(salestaxrate_table.c.salestaxrateid == existing_record.salestaxrateid)
                        .values(new_data)
                    )
                    connection.execute(update_salestaxrate_query)
            else:
                raise ValueError("Invalid action provided")

    @staticmethod
    def populate_salesterritory(action: str, insert_count: int):
        """Add data to the salesterritory table."""

        # Execute the setval query to set PK sequence to fix id duplicate error
        setval_query = """SELECT setval('sales.salesterritory_territoryid_seq', 
                                      (SELECT MAX(territoryid) FROM sales.salesterritory))"""

        # Generate faker data
        with engine.connect() as connection:
            # FK
            countryregions = connection.execute(
                select([countryregion_table.c.countryregioncode])
            ).fetchall()
            # data
            regions = [
                "North America",
                "Europe",
                "Asia",
                "South America",
                "Africa",
                "Oceania",
                "Middle East",
                "Caribbean",
                "Central America",
                "Scandinavia",
                "Southeast Asia",
                "Pacific Islands",
                "Eastern Europe",
                "South Asia",
                "Central Asia",
                "Nordic Countries",
                "Western Europe",
                "Southern Africa",
                "Andean Region",
                "Baltic States",
            ]

            countries = [
                "United States",
                "Canada",
                "United Kingdom",
                "Germany",
                "France",
                "China",
                "India",
                "Brazil",
                "Australia",
                "Japan",
                "Russia",
                "Mexico",
                "South Africa",
                "Italy",
                "Spain",
                "Netherlands",
                "Sweden",
                "Norway",
                "Argentina",
                "New Zealand",
            ]

            fake_data = [
                {
                    "countryregioncode": random.choice(countryregions)[0],
                    "name": random.choice(countries),
                    "group": random.choice(regions),
                    "salesytd": faker.random_number(digits=6),
                    "saleslastyear": faker.random_number(digits=8),
                    "costytd": faker.random_number(digits=4),
                    "costlastyear": faker.random_number(digits=6),
                    "rowguid": faker.uuid4(),
                    "modifieddate": datetime.now(),
                }
                for _ in range(insert_count)
            ]
            connection.execute(setval_query)

            if action == "create":
                connection.execute(salesterritory_table.insert(), fake_data)
            elif action == "edit":
                # Fetch existing records to update (randomly)
                records_to_update = connection.execute(
                    select([salesterritory_table])
                    .order_by(func.random())
                    .limit(insert_count)
                ).fetchall()

                # Iterate over the records to update
                for existing_record, new_data in zip(records_to_update, fake_data):
                    # Update the record in the salesterritory table
                    update_salesterritory_query = (
                        update(salesterritory_table)
                        .where(salesterritory_table.c.territoryid == existing_record.territoryid)
                        .values(new_data)
                    )
                    connection.execute(update_salesterritory_query)
            else:
                raise ValueError("Invalid action provided")

    @staticmethod
    def populate_salesterritoryhistory(action: str, insert_count: int):
        """Add data to the salesterritoryhistory table."""

        # Generate faker data
        with engine.connect() as connection:
            # FK's
            sales_persons = connection.execute(
                select([salesperson_table.c.businessentityid])
            ).fetchall()
            salesterritories = connection.execute(
                select([salesterritory_table.c.territoryid])
            ).fetchall()

            try:
                if action == "create":
                    data_to_insert = []
                    for _ in range(insert_count):
                        businessentityid = random.choice(sales_persons)[0]
                        salesterritoryid = random.choice(salesterritories)[0]

                        # Check if the combination already exists
                        existing_data = connection.execute(
                            select([salesterritoryhistory_table]).where(
                                and_(
                                    salesterritoryhistory_table.c.businessentityid
                                    == businessentityid,
                                    salesterritoryhistory_table.c.territoryid
                                    == salesterritoryid,
                                )
                            )
                        ).fetchone()

                        if existing_data is None:
                            data_to_insert.append(
                                {
                                    "businessentityid": businessentityid,
                                    "territoryid": salesterritoryid,
                                    "startdate": faker.date_between(
                                        start_date="-10y", end_date="+5y"
                                    ),
                                    "enddate": faker.date_between(
                                        start_date="+5y", end_date="+10y"
                                    ),
                                    "rowguid": faker.uuid4(),
                                    "modifieddate": datetime.now(),
                                }
                            )

                    connection.execute(
                        salesterritoryhistory_table.insert(), data_to_insert
                    )
                elif action == "edit":
                    # Fetch existing records to update (randomly)
                    existing_records = connection.execute(
                        select([salesterritoryhistory_table])
                        .order_by(func.random())
                        .limit(insert_count)
                    ).fetchall()

                    # Iterate over the records to update
                    for existing_record in existing_records:
                        # Generate new data for the update (excluding primary keys)
                        new_data = {
                            "startdate": faker.date_between(
                                start_date="-10y", end_date="+5y"
                            ),
                            "enddate": faker.date_between(
                                start_date="+5y", end_date="+10y"
                            ),
                            "rowguid": faker.uuid4(),
                            "modifieddate": datetime.now(),
                        }

                        # Update the record in the salesterritoryhistory table
                        update_salesterritoryhistory_query = (
                            update(salesterritoryhistory_table)
                            .where(salesterritoryhistory_table.c.businessentityid == existing_record.businessentityid)
                            .where(salesterritoryhistory_table.c.territoryid == existing_record.territoryid)
                            .values(new_data)
                        )
                        connection.execute(update_salesterritoryhistory_query)
                else:
                    raise ValueError("Invalid action provided")
            except IntegrityError as e:
                # Handle the unique constraint violation error
                loggy.info("Skipping duplicate key value.")

    @staticmethod
    def populate_shoppingcartitem(action: str, insert_count: int):
        """Add data to the shoppingcartitem table."""

        # Execute the setval query to set PK sequence to fix id duplicate error
        setval_query = """SELECT setval('sales.shoppingcartitem_shoppingcartitemid_seq', 
                                    (SELECT MAX(shoppingcartitemid) FROM sales.shoppingcartitem))"""

        # Generate faker data
        with engine.connect() as connection:
            products = connection.execute(
                select([product_table.c.productid])
            ).fetchall()

            fake_data = [
                {
                    "shoppingcartid": str(faker.uuid4()).replace("-", "")[:5],
                    "quantity": random.randint(1, 20),
                    "productid": random.choice(products)[0],
                    "datecreated": datetime.now(),
                    "modifieddate": datetime.now(),

                }
                for _ in range(insert_count)
            ]

            connection.execute(setval_query)

            if action == "create":
                connection.execute(shoppingcartitem_table.insert(), fake_data)
            elif action == "edit":
                # Fetch existing records to update (randomly)
                records_to_update = connection.execute(
                    select([shoppingcartitem_table])
                    .order_by(func.random())
                    .limit(insert_count)
                ).fetchall()

                # Iterate over the records to update
                for existing_record, new_data in zip(records_to_update, fake_data):
                    # Update the record in the shoppingcartitem table
                    update_shoppingcartitem_query = (
                        update(shoppingcartitem_table)
                        .where(shoppingcartitem_table.c.shoppingcartitemid == existing_record.shoppingcartitemid)
                        .values(new_data)
                    )
                    connection.execute(update_shoppingcartitem_query)
            else:
                raise ValueError("Invalid action provided")

    @staticmethod
    def populate_specialoffer(action: str, insert_count: int):
        """Add data to the specialoffer table."""

        # Execute the setval query to set PK sequence to fix id duplicate error
        setval_query = """SELECT setval('sales.specialoffer_specialofferid_seq', 
                                    (SELECT MAX(specialofferid) FROM sales.specialoffer))"""

        # Generate faker data
        data = [
            {
                "description": faker.text()[:50],
                "discountpct": random.uniform(0, 5),
                "type": faker.word().capitalize(),
                "category": faker.word().capitalize(),
                "startdate": faker.date_between(
                    start_date="-2y", end_date="today"
                ),
                "enddate": faker.date_between(start_date="+1m", end_date="+2y"),
                "minqty": random.randint(1, 20),
                "maxqty": random.randint(20, 40),
                "rowguid": faker.uuid4(),
            }
            for _ in range(insert_count)
        ]

        with engine.connect() as connection:
            connection.execute(setval_query)

            if action == "create":
                connection.execute(
                    specialoffer_table.insert(), data)

            elif action == "edit":
                # Get the total number of records in the table
                total_records = connection.execute(
                    select([func]).select_from(specialoffer_table)
                ).scalar()

                # Update a random selection of records
                records_to_update = random.sample(
                    range(1, total_records + 1), insert_count)
                for indx, record_id in enumerate(records_to_update):
                    update_query = (
                        update(specialoffer_table)
                        .where(specialoffer_table.c.specialofferid == record_id)
                        .values(data[indx])
                    )
                    connection.execute(update_query)

    @staticmethod
    def populate_specialofferproduct(action: str, insert_count: int):
        """Add data to the specialofferproduct table."""

        # Generate faker data
        with engine.connect() as connection:
            # FK's
            specialoffers = connection.execute(
                select([specialoffer_table.c.specialofferid])
            ).fetchall()
            products = connection.execute(
                select([product_table.c.productid])
            ).fetchall()

            try:
                if action == "create":
                    data_to_insert = []
                    for _ in range(insert_count):
                        specialofferid = random.choice(specialoffers)[0]
                        productid = random.choice(products)[0]

                        # Check if the combination already exists
                        existing_data = connection.execute(
                            select([specialofferproduct_table]).where(
                                and_(
                                    specialofferproduct_table.c.specialofferid
                                    == specialofferid,
                                    specialofferproduct_table.c.productid == productid,
                                )
                            )
                        ).fetchone()

                        if existing_data is None:
                            data_to_insert.append(
                                {
                                    "specialofferid": specialofferid,
                                    "productid": productid,
                                    "rowguid": faker.uuid4(),
                                    "modifieddate": datetime.now(),
                                }
                            )

                    connection.execute(
                        specialofferproduct_table.insert(), data_to_insert)
                elif action == "edit":
                    # Fetch existing records to update (randomly)
                    records_to_update = connection.execute(
                        select([specialofferproduct_table])
                        .order_by(func.random())
                        .limit(insert_count)
                    ).fetchall()

                    # Iterate over the records to update
                    for existing_record in records_to_update:
                        # Generate new data for the update (excluding primary keys)
                        new_data = {
                            "rowguid": faker.uuid4(),
                            "modifieddate": datetime.now(),
                        }

                        # Update the record in the specialofferproduct table
                        update_specialofferproduct_query = (
                            update(specialofferproduct_table)
                            .where(specialofferproduct_table.c.specialofferid == existing_record.specialofferid)
                            .where(specialofferproduct_table.c.productid == existing_record.productid)
                            .values(new_data)
                        )
                        connection.execute(update_specialofferproduct_query)
                else:
                    raise ValueError("Invalid action provided")
            except IntegrityError as e:
                # Handle the unique constraint violation error
                loggy.info("Skipping duplicate key value")

    @staticmethod
    def populate_store(action: str, insert_count: int):
        """Add data to the store table."""

        # Generate faker data
        with engine.connect() as connection:
            # FK's
            businessentity = connection.execute(
                select([businessentity_table.c.businessentityid])
            ).fetchall()
            salespersons = connection.execute(
                select([salesperson_table.c.businessentityid])
            ).fetchall()

            try:
                if action == "create":
                    data_to_insert = []
                    existing_businessentity_ids = connection.execute(
                        select([store_table.c.businessentityid])
                    ).fetchall()
                    existing_ids = set([row[0]
                                       for row in existing_businessentity_ids])

                    for _ in range(insert_count):
                        businessentity_id = random.choice(businessentity)[0]
                        while businessentity_id in existing_ids:
                            businessentity_id = random.choice(
                                businessentity)[0]

                        data_to_insert.append(
                            {
                                "businessentityid": businessentity_id,
                                "name": " ".join(
                                    faker.words(nb=random.randint(1, 4))
                                ).capitalize(),
                                "salespersonid": random.choice(salespersons)[0],
                                "rowguid": faker.uuid4(),
                                "modifieddate": datetime.now(),
                            }
                        )

                        existing_ids.add(businessentity_id)

                    connection.execute(store_table.insert(), data_to_insert)
                elif action == "edit":
                    # Fetch existing records to update (randomly)
                    existing_records = connection.execute(
                        select([store_table])
                        .order_by(func.random())
                        .limit(insert_count)
                    ).fetchall()

                    # Iterate over the records to update
                    for existing_record in existing_records:
                        # Generate new data for the update (excluding primary keys)
                        new_data = {
                            "name": " ".join(
                                faker.words(nb=random.randint(1, 4))
                            ).capitalize(),
                            "salespersonid": random.choice(salespersons)[0],
                            "rowguid": faker.uuid4(),
                            "modifieddate": datetime.now(),
                        }

                        # Update the record in the store table
                        update_store_query = (
                            update(store_table)
                            .where(store_table.c.businessentityid == existing_record.businessentityid)
                            .values(new_data)
                        )
                        connection.execute(update_store_query)
                else:
                    raise ValueError("Invalid action provided")
            except IntegrityError as e:
                # Handle the unique constraint violation error
                loggy.info("Skipping duplicate key value.")


class GenerateData:
    """
    generate a specific number of records to a target table in the
    postgres database.
    """

    def __init__(self, action: str, table: str, num_records: int):
        """
        define command line arguments.
        """
        self.action = action
        self.table = table
        self.num_records = num_records

        # create switcher to run the populate functions
        self.switcher = {
            "countryregioncurrency": Data().populate_countryregioncurrency,
            "creditcard": Data().populate_creditcard,
            "currency": Data().populate_currency,
            "currencyrate": Data().populate_currencyrate,
            "customer": Data().populate_customer,
            "personcreditcard": Data().populate_personcreditcard,
            "salesorderdetail": Data().populate_salesorderdetail,
            "salesorderheader": Data().populate_salesorderheader,
            "salesorderheadersalesreason": Data().populate_salesorderheadersalesreason,
            "salesperson": Data().populate_salesperson,
            "salespersonquotahistory": Data().populate_salespersonquotahistory,
            "salesreason": Data().populate_salesreason,
            "salestaxrate": Data().populate_salestaxrate,
            "salesterritory": Data().populate_salesterritory,
            "salesterritoryhistory": Data().populate_salesterritoryhistory,
            "shoppingcartitem": Data().populate_shoppingcartitem,
            "specialoffer": Data().populate_specialoffer,
            "specialofferproduct": Data().populate_specialofferproduct,
            "store": Data().populate_store,
        }

    def create_data(self):
        """
        Using the faker library, generate data and execute DML.
        """
        # table_names = [table.split('.')[-1] for table in metadata.tables.keys()]
        if self.table not in self.switcher.keys():
            return loggy.info(f"{self.table} table does not exist.")

        # calling functions based on table name
        loggy.info(
            f"{self.action} {self.num_records} records in {self.table}...")
        self.switcher.get(self.table)(self.action, self.num_records)
        loggy.info(f"{self.num_records} records {self.action} in {self.table}")
