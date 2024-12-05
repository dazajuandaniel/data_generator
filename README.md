# Data Generator
Repository to generate data for multiple engines/databases

# Motivation
Quickly generate data to test new/different cloud based techonologies

# Local Postgres
Run the following command to start a local instance of Postgres
```bash
docker-compose --file docker/postgres/docker-compose.yml up -d --force-recreate
```
# Postgres
## AdventureWorks Database

## Generate Data
```bash
python generate_data.py --table <table_name> --num_records <number_of_rows_to_insert>
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
* [AdventureWorks-for-Postgres](https://github.com/lorint/AdventureWorks-for-Postgres)**