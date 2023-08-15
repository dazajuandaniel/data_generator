1). Create venv and install requirments.txt. 

2). Database:
    
     - Make sure you have stable version of docker installed in your system
     - Open terminal and navigate to project root folder where 'docker-compose.yml' exist
     - Run 'docker-compose up' cmd and it will spin the postgres database with 
        name: Adventureworks
        And it will load data from 'adventure_works_2014_OLTP_script.zip' file
     - To keep docker container running 'docker-compose up -d'

    For more details  DB setup use this Readme
    https://github.com/lorint/AdventureWorks-for-Postgres

3). Command To load data in table using Faker:
    
    Syntax: python <.py> <table_name> <number_of_rows_to_insert>
    
    python load-faker-data.py currency 1 # it will add 1 new record in currency table    
    
    Valid tables so far:
        ['countryregioncurrency', 
        'creditcard', 'currency', 'currencyrate', 'customer', 'personcreditcard', 
        'salesorderdetail', 'salesorderheader', 'salesorderheadersalesreason', 
        'salesperson', 'salespersonquotahistory', 'salesreason', 'salestaxrate', 
        'salesterritory', 'salesterritoryhistory', 'shoppingcartitem', 'specialoffer', 
        'specialofferproduct', 'store'])



