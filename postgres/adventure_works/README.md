# How to Use

## Setup Environment
The `Makefile` contains the relevent commands to deploy the database locally

```bash
make venv # Creates the Virtual Environment and Installs dependencies
make db_create # Creates the Database in Postgres
make db_bootstrap # Adds the Seed Data to the Database
```

## Generate Data
```bash
# Create Records
python generate_data.py --action create --table <table_name>  --num_records <number_of_rows_to_insert>
# Edit Records
python generate_data.py --action edit --table <table_name>  --num_records <number_of_rows_to_edit>
```
#### Available Tables to Generate Data
Valid tables so far:
* countryregioncurrency 
* creditcard 
* currency 
* currencyrate 
* customer
* personcreditcard 
* salesorderdetail 
* salesorderheader 
* salesorderheadersalesreason 
* salesperson 
* salespersonquotahistory 
* salesreason 
* salestaxrate
* salesterritory 
* salesterritoryhistory 
* shoppingcartitem 
* specialoffer 
* specialofferproduct 
* store
```
# References & Attribution
* [AdventureWorks-for-Postgres](https://github.com/lorint/AdventureWorks-for-Postgres)