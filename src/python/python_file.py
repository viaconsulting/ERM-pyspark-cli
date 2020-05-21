''' 
    This Script is set to be run on EMR. 
    Using AWSWRANGLER, this script runs a Query on Amazon Athena,
    take the dataframe and inserts into a Stage table on a Mysql
    database. Finally, a Procedure on the Mysql Database is called.
    A Date in format: 'yyyy-mm-dd' must be passad as an argument,
    after giving the s3 path of the python file.
    To work properly, this file must be copied to an S3 bucket
'''
from __future__ import print_function
import sys
from random import random
from operator import add

from pyspark.sql import SparkSession

import awswrangler as ws
import boto3

if __name__ == "__main__":

    ## Creating Spark Session:
    spark = SparkSession\
        .builder\
        .appName("RefinedFlight")\
        .getOrCreate()

    print("Begin:")

    ########################## Variables ##############################

    # Input date, with 'yyyy-mm-dd' format
    search_date= sys.argv[1]

    ## Athena database:
    dbase= "your_athena_database"

    ## MySql:
    database_type= "mysql"
    host= "db.mySql.database.host"
    port= 3306
    database_name= "data_name"
    USER= "user_name"
    PASSWORD= "Xxxxxxxxxxx"
    Athena_query= "select origin_airport as origin, destination_airport as destination, pricing_date as flight_date, search_date as search_date, min(total_miles) as price from rfzd_searchflights_prd.smiles_search_flights where search_date = "+ search_date +" and segment_type = 'SEGMENT_1'and ( total_miles is not null and total_miles <> '' ) group by search_date, origin_airport, destination_airport, pricing_date limit 10;"
    mysql_table_name='stage_refined_flight'
    procedure_name='procedure_name_on_mysql_database'

    ##################################################################
    print("Athena database:")
    print(dbase)
    print("Query to be run on Athena:")
    print(Athena_query)

    #################################################################
    '''
        When running EMR, there will be variables set automatically, so
        there's no reason to specify avery single onde of them on your code.
        Yet in some cases, the acess keys given won't have permission to 
        permform athena queries. 
        If that is the case, you can set on the variables as below:
        session = boto3.session.Session(aws_access_key_id='XXXXXXXXXXXXXXXXXXXX',
                                    aws_secret_access_key='xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx',
                                    region_name='xx-xxxxx-x')
        
        Otherwise, set only the region name.
    '''
    session = boto3.session.Session(region_name='xx-xxxxx-x')
    #################################################################

    ## Execution of the Athena query, retrieving a dataframe:
    print("Executing Athena Query")
    insert_df= ws.athena.read_sql_query(sql=Athena_query,database=dbase,boto3_session=session)

    # Creating a connection with Mysql, using the variables defined above in the code:
    engine= ws.db.get_engine(db_type= database_type,host= host,port= port,database= database_name,user= USER,password= PASSWORD)

    # Inserting dataframe into the Mysql tabe
    print("Preenchendo a Stage: ")
    ws.db.to_sql(df=insert_df,con=engine,name=mysql_table_name,if_exists='append',index=False)

    # Calling procedure that uses the argument given when calling the python file
    procedure= "CALL "+ procedure_name +"("+ search_date +");"

    print("Calling procedure: "+ procedure_name)
    ws.db.read_sql_query(sql=procedure,con=engine)

    print("End")

    ## Stopping Spark Session
    spark.stop()
