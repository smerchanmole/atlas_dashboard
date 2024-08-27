
# importing the requests library
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType, TimestampType
from pyspark.sql.functions import *
from datetime import datetime
import requests
import json
import pandas as pd
pd.DataFrame.iteritems = pd.DataFrame.items #if spark is under 3.4 we need this to convert pandas df to spark df
import math
from tabulate import tabulate
from requests.auth import HTTPBasicAuth, HTTPDigestAuth
from atlas_variables import * #URL atlas , JSON schemas from ATLAS API and tables DDLs
from user_pass_file import * #your atlas user & pass files & variables called user=your user and pwd=your_pass



######################################################################################
def getGlossary (URL_atlas, user, pwd):
    # api-endpoint
    URL_glosary = URL_atlas + "glossary?limit=-1&offset=0&sort=ASC'"


    # sending get request and saving the response as response object
    response = requests.get(url = URL_glosary, verify=False, auth = (user, pwd))
    return (response)

######################################################################################

######################################################################################
def getEntities (URL_atlas, user, pwd):
    # api-endpoint
    URL_entities = URL_atlas + "search/basic?excludeDeletedEntities=true&limit=5000&marker=*&offset=0&query=*"

    # sending get request and saving the response as response object
    response = requests.get(url = URL_entities, verify=False, auth = (user, pwd))
    return (response)

######################################################################################
def getHiveDBs (URL_atlas, user, pwd):
    # api-endpoint
    URL_entities = URL_atlas + "search/basic?excludeDeletedEntities=true&limit=5000&marker=*&offset=0&query=*&typeName=hive_db"

    # sending get request and saving the response as response object
    response = requests.get(url = URL_entities, verify=False, auth = (user, pwd))
    return (response)
######################################################################################
######################################################################################
def getHiveTables (URL_atlas, user, pwd):
    # api-endpoint
    URL_entities = URL_atlas + "search/basic?excludeDeletedEntities=true&limit=5000&marker=*&offset=0&query=*&typeName=hive_table"

    # sending get request and saving the response as response object
    response = requests.get(url = URL_entities, verify=False, auth = (user, pwd))
    return (response)
######################################################################################
def getIcebergTables (URL_atlas, user, pwd):
    # api-endpoint
    URL_entities = URL_atlas + "search/basic?excludeDeletedEntities=true&limit=5000&marker=*&offset=0&query=*&typeName=iceberg_table"

    # sending get request and saving the response as response object
    response = requests.get(url = URL_entities, verify=False, auth = (user, pwd))
    return (response)
######################################################################################

######################################################################################
def getIcebergColumns (URL_atlas, user, pwd):
    # api-endpoint
    URL_entities = URL_atlas + "search/basic?excludeDeletedEntities=true&limit=5000&marker=*&offset=0&query=*&typeName=iceberg_column"

    # sending get request and saving the response as response object
    response = requests.get(url = URL_entities, verify=False, auth = (user, pwd))
    return (response)
######################################################################################
######################################################################################
def getHiveColumns (URL_atlas, user, pwd):
    # api-endpoint
    URL_entities = URL_atlas + "search/basic?excludeDeletedEntities=true&limit=5000&marker=*&offset=0&query=*&typeName=hive_column"

    # sending get request and saving the response as response object
    response = requests.get(url = URL_entities, verify=False, auth = (user, pwd))
    return (response)
######################################################################################

def convertGlossaryFromJson2Pandas (json_catalog):
    print()
    print("-------------------------------------")
    print("CONVERTING JSON INTO DF--------------")
    print("-------------------------------------")
    print()

    df = pd.json_normalize(json_catalog)

    return df

######################################################################################
def convertEntitiesFromJson2Pandas (json_entities):
    print()
    print("-------------------------------------")
    print("CONVERTING JSON INTO DF--------------")
    print("-------------------------------------")
    print()

    df = pd.json_normalize(json_entities)

    return df

######################################################################################
def convertGlossaryFromPandas(df_catalog):
    print()
    print("-------------------------------------")
    print("CONVERTING DF CATALOG IN 3 DF:     --")
    print("- MAIN_CATEGORIES DF.              --")
    print("- TERMS DF.                        --")
    print("- SUBCATEGORIES DF.                --")
    print("-------------------------------------")
    #Lets get the elements with Category info

    df_terms = pd.DataFrame(columns=['catalogGuid','termGuid', 'relationGuid', 'displayText'])

    categories=[]
    terms=[]
    subcategories=[]
    for index, row in df_catalog.iterrows():
        #We store the Catalog info.
        guid=row['guid']
        qualifiedName=row['qualifiedName']
        name=row['name']
        shortDescription=row['shortDescription']
        longDescription=row['longDescription']
        catalogGuid=row['guid']

        if isinstance(row['terms'], list) is False:
            print ("--> INFO. This categorie doesn't have terms: ", row['name'])
        else:
            for row_term in row['terms']:
                termGuid=row_term['termGuid']
                relationGuid=row_term['relationGuid']
                displayText=row_term['displayText']
                terms.append([catalogGuid, termGuid, relationGuid, displayText])
                #print ("--->the term is: [catalogGuid, termGuid, relationGuid, displayText]")
                #print (terms[-1])
        if isinstance(row['categories'], list) is False:
            print ("--> INFO. This categorie doesn't have subcategories: ", row['name'])
        else:
            for row_subcategorie in row['categories']:
                categoryGuid=row_subcategorie['categoryGuid']
                relationGuid=row_subcategorie['relationGuid']
                displayText=row_subcategorie['displayText']

                subcategories.append([guid, categoryGuid, relationGuid, displayText])
                #print ("--->the sub categorie is: [guid, categorieGuid, relationGuid, displayText]")
                #print (subcategories[-1])
        categories.append([guid, qualifiedName, name, shortDescription, longDescription])
        #### Meter aqui tambien subcategorias
        df_categories = pd.DataFrame(categories, columns=['guid','qualifiedName','name','shortDescription','longDescription'])
        df_terms = pd.DataFrame(terms, columns=['catalogGuid', 'termGuid', 'relationGuid', 'displayText' ])
        df_subcategories = pd.DataFrame(subcategories, columns=['guid', 'categoryGuid', 'relationGuid', 'displayText'])

    return (df_categories, df_terms, df_subcategories)

######################################################################################

def manageGlossary(URL_atlas, user, pwd, photo_now):
    response = getGlossary(URL_atlas, user, pwd)
    print()
    print("The URL Response is:", response)

    if response.status_code != 200:
        print("The response is: ", response)
        print("We exit as is NOT  '200 OK'")
        return (-1)
    json_catalog = response.json()
    print()
    print("-------------------------------------")
    print("FULL CATALOG JSON -------------------")
    print("-------------------------------------")

    print(json.dumps(json_catalog, indent=4))

    print()
    print("..............................................")
    print("..............................................")
    print()

    df_catalog = convertGlossaryFromJson2Pandas(
        json_catalog)  # Convert json to simple DF (without flatten, as it is a complicate json
    print("-------------------------------------")
    print("FULL CATALOG ------------------------")
    print("-------------------------------------")
    print(df_catalog)

    (df_main_category, df_terms, df_subcategories) = convertGlossaryFromPandas(
        df_catalog)  # Flat the pandas, to split elements and sub categories.

    print()
    print("-------------------------------------")
    print("MAIN CATEGORIES ---------------------")
    print("-------------------------------------")
    print(df_main_category)
    # print(tabulate(df_main_category, headers='keys', tablefmt='psql'))

    print()
    print("-------------------------------------")
    print("TERMS -------------------------------")
    print("-------------------------------------")
    # print (df_terms)
    print(tabulate(df_terms, headers='keys', tablefmt='psql'))

    print()
    print("-------------------------------------")
    print("SUBCATEGORIES -----------------------")
    print("-------------------------------------")
    # print (df_subcategories)
    print(tabulate(df_subcategories, headers='keys', tablefmt='psql'))

    # print (df_catalog)
    # print(tabulate(df_print, headers='keys', tablefmt='psql'))

    ##### ADDING PHOTOTIMESTAMP

    # Add a timestamp column
    df_main_category['photo_timestamp'] = pd.to_datetime(photo_now)
    df_terms['photo_timestamp'] = pd.to_datetime(photo_now)
    df_subcategories['photo_timestamp'] = pd.to_datetime(photo_now)

    print()
    print("-------------------------------------")
    print("CREATING SPARK TABLES----------------")
    print("-------------------------------------")

    print()
    print("-------------------------------------")
    print("creating context---------------------")
    print("-------------------------------------")

    # ###########################################################
    # Generate the Spark Session
    #############################################################
    print("\n***** Creando la sesion Spark ...\n")
    spark = (SparkSession \
             .builder.appName("Spark-SQL-Test") \
             .config("spark.sql.iceberg.handle-timestamp-without-timezone", "true") \
             .enableHiveSupport() \
             .getOrCreate())

    spark.sparkContext.setLogLevel("ERROR")
    print("\n****** Sesion creada.\n")

    # ###########################################################
    # converting the pandas df into spark df
    #############################################################
    ## If you see an error here, spark is under 3.4 so you need pip install pandas==1.2.5
    spark_df_main_category = spark.createDataFrame(df_main_category, schema=SCHEMA_CATEGORIES)
    spark_df_terms = spark.createDataFrame(df_terms, schema=SCHEMA_TERMS)
    spark_df_subcategories = spark.createDataFrame(df_subcategories, schema=SCHEMA_SUBCATEGORIES)
    # ###########################################################
    # Generate the tables with DDLs the CSV into a Spark DF
    #############################################################

    print("****** CREATING THE  DATABASE:", DATABASE)
    resultados = spark.sql(SQL_CREATE_DATABASE)
    resultados.show(1)

    print("****** SELECTING THE CORRECT DATABASE:", DATABASE)
    resultados = spark.sql(SQL_USE_DATABASE)
    resultados.show(1)
    print("****** CREATING TABLE SQL_DDL_CATEGORIES ")
    resultados = spark.sql(SQL_DDL_CATEGORIES)
    resultados.show(1)
    print("****** CREATING TABLE SQL_DDL_TERMS ")
    resultados = spark.sql(SQL_DDL_TERMS)
    resultados.show(1)

    print("****** CREATING TABLE SQL_DDL_SUBCATEGORIES ")
    resultados = spark.sql(SQL_DDL_SUBCATEGORIES)
    resultados.show(1)

    ### Loading Data
    print("\n****** We are going to write into table CATEGORIES: ")
    print()
    spark_df_main_category.write.format("parquet").mode("append").saveAsTable(DATABASE+".categories")
    print("\n****** Registers loaded into CATEGORIES")
    print()
    ### Loading Data
    print("\n****** We are going to write into table TERMS: ")
    print()
    spark_df_terms.write.format("parquet").mode("append").saveAsTable(DATABASE+".terms")
    print("\n****** Registers loaded into TERMS")
    print()
    print("\n****** We are going to write into table SUBCATEGORIES: ")
    print()
    spark_df_subcategories.write.format("parquet").mode("append").saveAsTable(DATABASE+".subcategories")
    print("\n****** Registers loaded into SUBCATEGORIES")
    print()

    print()
    print("..............................................")
    print("..............................................")
    print()
    # ###########################################################
    # CLOSING THE SESSION
    #############################################################

    print("\n****** Cerrando sesion spark...\n")

    spark.stop
######################################################################################


def manageEntities(URL_atlas, user, pwd, photo_now):
    response = getEntities(URL_atlas, user, pwd)
    print()
    print("The URL Response is:", response)

    if response.status_code != 200:
        print("The response is: ", response)
        print("We exit as is NOT  '200 OK'")
        return (-1)
    json_entities = response.json()
    print()
    print("--------------------------------------")
    #print("FULL ENTITIES JSON -------------------")
    print("--------------------------------------")
    #print (json.dumps(json_entities, indent=4))

    print("-----------------------------------------------------------------")
    print("---Meta Info about the entities response  ------------------------")
    print("-----------------------------------------------------------------")
    print(json.dumps(json_entities["searchParameters"], indent=4))
    print(json.dumps(json_entities["approximateCount"], indent=4))
    print(json.dumps(json_entities["nextMarker"], indent=4))

    #print(json.dumps(json_entities, indent=4))
    print("-----------------------------------------------------------------")
    print("-----------------------------------------------------------------")
    print("-----------------------------------------------------------------")
    print("-----------------------------------------------------------------")
    #print(json.dumps(json_entities["entities"], indent=4))


    df_entities=convertEntitiesFromJson2Pandas(json_entities["entities"])

    print("-------------------------------------")
    print("Comparing rows with json results-----")
    print("-------------------------------------")

    rows, columns = df_entities.shape
    print(f"Number of rows: {rows}")

    # Count the occurrences of each element in the 'typeName' column
    value_counts = df_entities['typeName'].value_counts()
    # Print the results

    print("-------------------------------------")
    print("DIFFERENT Entities TYPES ------------")
    print("-------------------------------------")

    print(value_counts)
    print()

    print("-------------------------------------")
    print("FULL ENTITIES  (just 10) ------------")
    print("-------------------------------------")
    print (df_entities.head(10))
    #print(tabulate(df_entities.head(10), headers='keys', tablefmt='psql'))

    print("-------------------------------------")
    print("HIVE_DBs     ------------------------")
    print("-------------------------------------")

    # Filter the DataFrame to only include rows where 'typeName' is "hive column"
    df_entities_dbs= df_entities[df_entities['typeName'] == "hive_db"]
    print (df_entities_dbs)
    print(tabulate(df_entities_dbs, headers='keys', tablefmt='psql'))

    print("-------------------------------------")
    print("HIVE_tables  ------------------------")
    print("-------------------------------------")

    # Filter the DataFrame to only include rows where 'typeName' is "hive column"
    df_entities_tables = df_entities[df_entities['typeName'] == "hive_table"]
    print(df_entities_tables)
    print(tabulate(df_entities_tables, headers='keys', tablefmt='psql'))

######################################################################################


def manageEntitiesHiveDBs(URL_atlas, user, pwd, photo_now):
    response = getHiveDBs(URL_atlas, user, pwd)
    print()
    print("The URL Response is:", response)

    if response.status_code != 200:
        print("The response is: ", response)
        print("We exit as is NOT  '200 OK'")
        return (-1)
    json_hives_dbs = response.json()
    print()
    print("--------------------------------------")
    print("HIVE DBS JSON -------------------")
    print("--------------------------------------")
    print (json.dumps(json_hives_dbs, indent=4))

    print("-----------------------------------------------------------------")
    print("---Meta Info about the entities response  ------------------------")
    print("-----------------------------------------------------------------")
    print(json.dumps(json_hives_dbs["searchParameters"], indent=4))
    print(json.dumps(json_hives_dbs["approximateCount"], indent=4))
    print(json.dumps(json_hives_dbs["nextMarker"], indent=4))


    df_entities=convertEntitiesFromJson2Pandas(json_hives_dbs["entities"])

    print("-------------------------------------")
    print("Comparing rows with json results-----")
    print("-------------------------------------")

    rows, columns = df_entities.shape
    print(f"Number of rows: {rows}")

    # Count the occurrences of each element in the 'typeName' column
    value_counts = df_entities['typeName'].value_counts()
    # Print the results

    print("-------------------------------------")
    print("DIFFERENT Entities TYPES ------------")
    print("-------------------------------------")

    print(value_counts)
    print()

    print("-------------------------------------")
    print("HIVE_DBs     ------------------------")
    print("-------------------------------------")

    # Filter the DataFrame to only include rows where 'typeName' is "hive column"
    df_entities_dbs= df_entities[df_entities['typeName'] == "hive_db"]
    print (df_entities_dbs)
    print(tabulate(df_entities_dbs, headers='keys', tablefmt='psql'))

    # Add a timestamp column
    df_entities_dbs['photo_timestamp'] = pd.to_datetime(photo_now)

    print()
    print("-------------------------------------")
    print("CREATING SPARK TABLE----------------")
    print("-------------------------------------")

    print()
    print("-------------------------------------")
    print("creating context---------------------")
    print("-------------------------------------")

    # ###########################################################
    # Generate the Spark Session
    #############################################################
    print("\n***** Creando la sesion Spark ...\n")
    spark = (SparkSession \
             .builder.appName("Spark-SQL-Test") \
             .config("spark.sql.iceberg.handle-timestamp-without-timezone", "true") \
             .enableHiveSupport() \
             .getOrCreate())

    spark.sparkContext.setLogLevel("ERROR")
    print("\n****** Sesion creada.\n")

    # ###########################################################
    # converting the pandas df into spark df
    #############################################################
    ## If you see an error here, spark is under 3.4 so you need pip install pandas==1.2.5
    spark_df_entities_hive_dbs = spark.createDataFrame(df_entities_dbs, schema=SCHEMA_ENTITIES)
    # ###########################################################
    # Generate the tables with DDLs the CSV into a Spark DF
    #############################################################

    print("****** CREATING THE  DATABASE:", DATABASE)
    resultados = spark.sql(SQL_CREATE_DATABASE)
    resultados.show(1)

    print("****** SELECTING THE CORRECT DATABASE:", DATABASE)
    resultados = spark.sql(SQL_USE_DATABASE)
    resultados.show(1)
    print("****** CREATING TABLE SQL_DDL_ENTITIES ")
    resultados = spark.sql(SQL_DDL_ENTITIES)
    resultados.show(1)

    ### Loading Data
    print("\n****** We are going to write into table ENTITIES: ")
    print()
    spark_df_entities_hive_dbs.write.format("parquet").mode("append").saveAsTable(DATABASE + ".entities")
    print("\n****** Registers loaded into ENTITIES")
    print()
    print("..............................................")
    print("..............................................")
    print()
    # ###########################################################
    # CLOSING THE SESSION
    #############################################################

    print("\n****** Cerrando sesion spark...\n")

    spark.stop


######################################################################################

######################################################################################


def manageEntitiesTables(URL_atlas, user, pwd, photo_now):
    response = getHiveTables(URL_atlas, user, pwd)
    print()
    print("The URL Response is:", response)

    if response.status_code != 200:
        print("The response is: ", response)
        print("We exit as is NOT  '200 OK'")
        return (-1)
    json_hives_tables = response.json()
    print()
    print("--------------------------------------")
    print("HIVE TABLES JSON -------------------")
    print("--------------------------------------")
    #print (json.dumps(json_hives_tables, indent=4))

    print("-----------------------------------------------------------------")
    print("---Meta Info about the entities response  ------------------------")
    print("-----------------------------------------------------------------")
    print(json.dumps(json_hives_tables["searchParameters"], indent=4))
    print(json.dumps(json_hives_tables["approximateCount"], indent=4))
    print(json.dumps(json_hives_tables["nextMarker"], indent=4))


    df_entities_hive_tables=convertEntitiesFromJson2Pandas(json_hives_tables["entities"])

    print("-------------------------------------")
    print("Comparing rows with json results-----")
    print("-------------------------------------")

    rows, columns = df_entities_hive_tables.shape
    print(f"Number of rows: {rows}")

    # Count the occurrences of each element in the 'typeName' column
    value_counts = df_entities_hive_tables['typeName'].value_counts()
    # Print the results

    print("-------------------------------------")
    print("DIFFERENT Entities TYPES ------------")
    print("-------------------------------------")

    print(value_counts)
    print()

    print("-------------------------------------")
    print("HIVE_TABLES  ------------------------")
    print("-------------------------------------")

    # Filter the DataFrame to only include rows where 'typeName' is "hive column"
    df_entities_hive_tables= df_entities_hive_tables[df_entities_hive_tables['typeName'] == "hive_table"]
    print (df_entities_hive_tables)
    print(tabulate(df_entities_hive_tables, headers='keys', tablefmt='psql'))


    ##### REPEAT TO GET ALSO ICEBERG_TABLES
    response = getIcebergTables(URL_atlas, user, pwd)
    print()
    print("The URL Response is:", response)

    if response.status_code != 200:
        print("The response is: ", response)
        print("We exit as is NOT  '200 OK'")
        return (-1)
    json_iceberg_tables = response.json()
    print()
    print("--------------------------------------")
    print("ICEBERG TABLES JSON -------------------")
    print("--------------------------------------")
    #print (json.dumps(json_iceberg_tables, indent=4))

    print("-----------------------------------------------------------------")
    print("---Meta Info about the entities response  ------------------------")
    print("-----------------------------------------------------------------")
    print(json.dumps(json_iceberg_tables["searchParameters"], indent=4))
    print(json.dumps(json_iceberg_tables["approximateCount"], indent=4))
    print(json.dumps(json_iceberg_tables["nextMarker"], indent=4))


    df_entities_iceberg_tables=convertEntitiesFromJson2Pandas(json_iceberg_tables["entities"])

    print("-------------------------------------")
    print("Comparing rows with json results-----")
    print("-------------------------------------")

    rows, columns = df_entities_iceberg_tables.shape
    print(f"Number of rows: {rows}")

    # Count the occurrences of each element in the 'typeName' column
    value_counts = df_entities_iceberg_tables['typeName'].value_counts()
    # Print the results

    print("-------------------------------------")
    print("DIFFERENT Entities TYPES ------------")
    print("-------------------------------------")

    print(value_counts)
    print()

    print("-------------------------------------")
    print("ICEBERG_TABLES  ---------------------")
    print("-------------------------------------")

    # Filter the DataFrame to only include rows where 'typeName' is "hive column"
    df_entities_iceberg_tables= df_entities_iceberg_tables[df_entities_iceberg_tables['typeName'] == "iceberg_table"]
    print (df_entities_iceberg_tables)
    print(tabulate(df_entities_iceberg_tables, headers='keys', tablefmt='psql'))


    #########################
    #Now concat the 2 data frames
    ########################

    df_entities_tables= pd.concat([df_entities_iceberg_tables, df_entities_hive_tables])
    print ("----- BOTH HIVE & ICEBERG TABLES ")
    print (df_entities_tables)
    # Add a timestamp column
    df_entities_tables['photo_timestamp'] = pd.to_datetime(photo_now)

    # We have to drop the column created_time as we don't need it and on the previous entities doesn't appear
    df_entities_tables = df_entities_tables.drop('attributes.createTime', axis=1)  # Specify axis=1 for columns

    column_names = df_entities_tables.columns
    print ("---- df_entities-tables column names:")
    print(column_names)

    print()
    print("-------------------------------------")
    print("CREATING SPARK TABLE----------------")
    print("-------------------------------------")

    print()
    print("-------------------------------------")
    print("creating context---------------------")
    print("-------------------------------------")

    # ###########################################################
    # Generate the Spark Session
    #############################################################
    print("\n***** Creando la sesion Spark ...\n")
    spark = (SparkSession \
             .builder.appName("Spark-SQL-Test") \
             .config("spark.sql.iceberg.handle-timestamp-without-timezone", "true") \
             .enableHiveSupport() \
             .getOrCreate())

    spark.sparkContext.setLogLevel("ERROR")
    print("\n****** Sesion creada.\n")

    # ###########################################################
    # converting the pandas df into spark df
    #############################################################
    ## If you see an error here, spark is under 3.4 so you need pip install pandas==1.2.5
    spark_df_entities_tables = spark.createDataFrame(df_entities_tables, schema=SCHEMA_ENTITIES)
    # ###########################################################
    # Generate the tables with DDLs the CSV into a Spark DF
    #############################################################

    print("****** CREATING THE  DATABASE:", DATABASE)
    resultados = spark.sql(SQL_CREATE_DATABASE)
    resultados.show(1)

    print("****** SELECTING THE CORRECT DATABASE:", DATABASE)
    resultados = spark.sql(SQL_USE_DATABASE)
    resultados.show(1)
    print("****** CREATING TABLE SQL_DDL_ENTITIES ")
    resultados = spark.sql(SQL_DDL_ENTITIES)
    resultados.show(1)

    ### Loading Data
    print("\n****** We are going to write into table ENTITIES: ")
    print()
    spark_df_entities_tables.write.format("parquet").mode("append").saveAsTable(DATABASE + ".entities")
    print("\n****** Registers loaded into ENTITIES")
    print()
    print("..............................................")
    print("..............................................")
    print()
    # ###########################################################
    # CLOSING THE SESSION
    #############################################################

    print("\n****** Cerrando sesion spark...\n")

    spark.stop


######################################################################################


def manageEntitiesColumns(URL_atlas, user, pwd, photo_now):
    response = getHiveColumns(URL_atlas, user, pwd)
    print()
    print("The URL Response is:", response)

    if response.status_code != 200:
        print("The response is: ", response)
        print("We exit as is NOT  '200 OK'")
        return (-1)
    json_hives_columns = response.json()
    print()
    print("--------------------------------------")
    print("HIVE COLUMNS JSON -------------------")
    print("--------------------------------------")
    #print (json.dumps(json_hives_columns, indent=4))

    print("-----------------------------------------------------------------")
    print("---Meta Info about the entities response  ------------------------")
    print("-----------------------------------------------------------------")
    print(json.dumps(json_hives_columns["searchParameters"], indent=4))
    print(json.dumps(json_hives_columns["approximateCount"], indent=4))
    print(json.dumps(json_hives_columns["nextMarker"], indent=4))


    df_entities_hive_columns=convertEntitiesFromJson2Pandas(json_hives_columns["entities"])

    print("-------------------------------------")
    print("Comparing rows with json results-----")
    print("-------------------------------------")

    rows, columns = df_entities_hive_columns.shape
    print(f"Number of rows: {rows}")

    # Count the occurrences of each element in the 'typeName' column
    value_counts = df_entities_hive_columns['typeName'].value_counts()
    # Print the results

    print("-------------------------------------")
    print("DIFFERENT Entities TYPES ------------")
    print("-------------------------------------")

    print(value_counts)
    print()

    print("-------------------------------------")
    print("HIVE_COLUMNS ------------------------")
    print("-------------------------------------")

    # Filter the DataFrame to only include rows where 'typeName' is "hive column"
    df_entities_hive_columns= df_entities_hive_columns[df_entities_hive_columns['typeName'] == "hive_column"]
    print (df_entities_hive_columns)

    ##### REPEAT TO GET ALSO ICEBERG_COLUMNS
    response = getIcebergColumns(URL_atlas, user, pwd)
    print()
    print("The URL Response is:", response)

    if response.status_code != 200:
        print("The response is: ", response)
        print("We exit as is NOT  '200 OK'")
        return (-1)
    json_iceberg_columns = response.json()
    print()
    print("--------------------------------------")
    print("ICEBERG COLUMNS JSON -----------------")
    print("--------------------------------------")
    print (json.dumps(json_iceberg_columns, indent=4))

    print("-----------------------------------------------------------------")
    print("---Meta Info about the entities response  ------------------------")
    print("-----------------------------------------------------------------")
    print(json.dumps(json_iceberg_columns["searchParameters"], indent=4))
    print(json.dumps(json_iceberg_columns["approximateCount"], indent=4))
    print(json.dumps(json_iceberg_columns["nextMarker"], indent=4))


    df_entities_iceberg_columns=convertEntitiesFromJson2Pandas(json_iceberg_columns["entities"])

    print("-------------------------------------")
    print("Comparing rows with json results-----")
    print("-------------------------------------")

    rows, columns = df_entities_iceberg_columns.shape
    print(f"Number of rows: {rows}")

    # Count the occurrences of each element in the 'typeName' column
    value_counts = df_entities_iceberg_columns['typeName'].value_counts()
    # Print the results

    print("-------------------------------------")
    print("DIFFERENT Entities TYPES ------------")
    print("-------------------------------------")

    print(value_counts)
    print()

    print("-------------------------------------")
    print("ICEBERG_COLUMNS ---------------------")
    print("-------------------------------------")

    # Filter the DataFrame to only include rows where 'typeName' is "hive column"
    df_entities_iceberg_columns= df_entities_iceberg_columns[df_entities_iceberg_columns['typeName'] == "iceberg_column"]
    print (df_entities_iceberg_columns)

    #########################
    #Now concat the 2 data frames
    ########################

    df_entities_columns= pd.concat([df_entities_iceberg_columns, df_entities_hive_columns])
    print ("----- BOTH HIVE & ICEBERG COLUMNS ")
    print (df_entities_columns)
    # Add a timestamp column
    df_entities_columns['photo_timestamp'] = pd.to_datetime(photo_now)

    # WE DONT HAVE THIS COLUMN IN COLUMN ENTITIES
    # We have to drop the column created_time as we don't need it and on the previous entities doesn't appear
    # df_entities_columns = df_entities_columns.drop('attributes.createTime', axis=1)  # Specify axis=1 for columns

    column_names = df_entities_columns.columns
    print ("---- df_entities-tables column names:")
    print(column_names)

    print()
    print("-------------------------------------")
    print("CREATING SPARK TABLE----------------")
    print("-------------------------------------")

    print()
    print("-------------------------------------")
    print("creating context---------------------")
    print("-------------------------------------")

    # ###########################################################
    # Generate the Spark Session
    #############################################################
    print("\n***** Creando la sesion Spark ...\n")
    spark = (SparkSession \
             .builder.appName("Spark-SQL-Test") \
             .config("spark.sql.iceberg.handle-timestamp-without-timezone", "true") \
             .enableHiveSupport() \
             .getOrCreate())

    spark.sparkContext.setLogLevel("ERROR")
    print("\n****** Sesion creada.\n")

    # ###########################################################
    # converting the pandas df into spark df
    #############################################################
    ## If you see an error here, spark is under 3.4 so you need pip install pandas==1.2.5
    spark_df_entities_columns = spark.createDataFrame(df_entities_columns, schema=SCHEMA_ENTITIES)
    # ###########################################################
    # Generate the tables with DDLs the CSV into a Spark DF
    #############################################################

    print("****** CREATING THE  DATABASE:", DATABASE)
    resultados = spark.sql(SQL_CREATE_DATABASE)
    resultados.show(1)

    print("****** SELECTING THE CORRECT DATABASE:", DATABASE)
    resultados = spark.sql(SQL_USE_DATABASE)
    resultados.show(1)
    print("****** CREATING TABLE SQL_DDL_ENTITIES ")
    resultados = spark.sql(SQL_DDL_ENTITIES)
    resultados.show(1)

    ### Loading Data
    print("\n****** We are going to write into table ENTITIES: ")
    print()
    spark_df_entities_columns.write.format("parquet").mode("append").saveAsTable(DATABASE + ".entities")
    print("\n****** Registers loaded into ENTITIES")
    print()
    print("..............................................")
    print("..............................................")
    print()
    # ###########################################################
    # CLOSING THE SESSION
    #############################################################

    print("\n****** Cerrando sesion spark...\n")

    spark.stop


######################################################################################
######################################################################################
######################################################################################
######################################################################################
# MAIN SECTION
######################################################################################
######################################################################################
######################################################################################

######################################################################################

# We begin saving the "now" moment
photo_now = datetime.now()

# Everything related to the Catalog

status = manageGlossary( URL_atlas, user, pwd, photo_now)
if status == -1:
    print ("******************* ERROR WITH manageGlossary, EXIT******************* ")
    exit (-1)


######################################################################################
# Everything related to the Entities

# ###########################################################
status = manageEntities( URL_atlas, user, pwd, photo_now)
if status == -1:
    print ("******************* ERROR WITH manageEntities, EXIT******************* ")
    exit (-1)

status = manageEntitiesHiveDBs( URL_atlas, user, pwd, photo_now)
if status == -1:
    print ("******************* ERROR WITH manageEntities, EXIT******************* ")
    exit (-1)
# HIVE and ICEBERG TABLES
status = manageEntitiesTables( URL_atlas, user, pwd, photo_now)
if status == -1:
    print ("******************* ERROR WITH manageEntities, EXIT******************* ")
    exit (-1)

# HIVE and Iceberg Columns
status = manageEntitiesColumns( URL_atlas, user, pwd, photo_now)
if status == -1:
    print ("******************* ERROR WITH manageEntities, EXIT******************* ")
    exit (-1)