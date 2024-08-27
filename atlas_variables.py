# ###########################################################
#
# VARIABLES
#
# ###########################################################
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType, TimestampType

URL_atlas="https://YOUR_URI:31443/api/atlas/v2/"

DATABASE="data_governance"  #the database where the tables will be generated
SQL_CREATE_DATABASE="CREATE DATABASE IF NOT EXISTS "+DATABASE + ";"
SQL_USE_DATABASE="USE "+DATABASE+";" #first SQL sentence to select that database
SQL_DDL_CATEGORIES="CREATE TABLE IF NOT EXISTS categories (\
			guid string,\
			qualifiedName string,\
			name string,\
			shortDescription string ,\
			longDescription string ,\
			photo_timestamp TIMESTAMP ) \
            USING PARQUET ;"
#			USING ICEBERG  TBLPROPERTIES ('format-version'='2') ;"
SQL_DDL_TERMS="CREATE TABLE IF NOT EXISTS terms ( \
            catalogGuid string, \
			termGuid string, \
			relationGuid string, \
			displayText string, \
			photo_timestamp TIMESTAMP)\
            USING PARQUET ;"
#			USING ICEBERG  TBLPROPERTIES ('format-version'='2') ;"

SQL_DDL_SUBCATEGORIES="CREATE TABLE IF NOT EXISTS subcategories ( \
            guid string, \
            categoryGuid string, \
			relationGuid string, \
			displayText string, \
			photo_timestamp TIMESTAMP) \
            USING PARQUET;"
#            USING ICEBERG  TBLPROPERTIES ('format-version'='2') ;"

SQL_DDL_ENTITIES= ("CREATE TABLE IF NOT EXISTS entities ( \
            typeName string, \
            guid string, \
			status string, \
			displayText string, \
            classificationNames string, \
            meaningNames string, \
            meanings string, \
            isIncomplete string, \
			labels string, \
            attributes_owner string,\
            attributes_qualifiedName string, \
            attributes_name string, \
            attributes_description string, \
            photo_timestamp TIMESTAMP) \
            USING PARQUET;")
#            USING ICEBERG  TBLPROPERTIES ('format-version'='2') ;"

SCHEMA_ENTITIES =  StructType([
    StructField("typeName", StringType(), True),  # (string, nullable)
    StructField("guid", StringType(), True),  # (string, nullable)
    StructField("status", StringType(), True),  # (string, nullable)
    StructField("displayText", StringType(), True),  # (string, nullable)
    StructField("classificationNames", StringType(), True),  # (string, nullable)
    StructField("meaningNames", StringType(), True),  # (string, nullable)
    StructField("meanings", StringType(), True),  # (string, nullable)
    StructField("isIncomplete", StringType(), True),  # (string, nullable)
    StructField("labels", StringType(), True),  # (string, nullable)
    StructField("attributes_owner", StringType(), True),  # (string, nullable)
    StructField("attributes_qualifiedName", StringType(), True),  # (string, nullable)
    StructField("attributes_name", StringType(), True),  # (string, nullable)
    StructField("attributes_description", StringType(), True),  # (string, nullable)
    StructField("photo_timestamp", TimestampType(), True)  # (timestamp, nullable)
])

SCHEMA_CATEGORIES =  StructType([
    StructField("guid", StringType(), True),  # (string, nullable)
    StructField("qualifiedName", StringType(), True),  # (string, nullable)
    StructField("name", StringType(), True),  # (string, nullable)
    StructField("shortDescription", StringType(), True),  # (string, nullable)
    StructField("longDescription", StringType(), True),  # (string, nullable)
    StructField("photo_timestamp", TimestampType(), True)  # (string, nullable)

])

SCHEMA_TERMS = StructType([
    StructField("catalogGuid", StringType(), True),  # (string, nullable)
    StructField("termGuid", StringType(), True),  # (string, nullable)
    StructField("relationGuid", StringType(), True),  # (string, nullable)
    StructField("displayText", StringType(), True),  # (string, nullable)
    StructField("photo_timestamp", TimestampType(), True)  # (string, nullable)

])

SCHEMA_SUBCATEGORIES = StructType([
    StructField("guid", StringType(), True),  # (string, nullable)
    StructField("categoryGuid", StringType(), True),  # (string, nullable)
    StructField("relationGuid", StringType(), True),  # (string, nullable)
    StructField("displayText", StringType(), True),  # (string, nullable)
    StructField("photo_timestamp", TimestampType(), True)  # (string, nullable)

])