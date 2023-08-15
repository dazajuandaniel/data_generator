import sys
from crud import Data, loggy, metadata


class GenerateData:
    """
    generate a specific number of records to a target table in the
    postgres database.
    """

    def __init__(self):
        """
        define command line arguments.
        """
        self.table = sys.argv[1]
        self.num_records = int(sys.argv[2])

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
        loggy.info(f"Creating {self.num_records} records in {self.table}...")
        self.switcher.get(self.table)(self.num_records)
        loggy.info(f"{self.num_records} records created in {self.table}")


if __name__ == "__main__":
    generate_data = GenerateData()
    generate_data.create_data()
