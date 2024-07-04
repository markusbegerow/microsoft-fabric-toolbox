#!/usr/bin/env python
# coding: utf-8

# ## Markus_CSV_READER
# 
# New notebook

# In[ ]:


# Welcome to your new notebook
# Type here in the cell editor to add code

# Import necessary libraries
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, regexp_extract, regexp_replace

# Create SparkSession
spark = SparkSession.builder \
    .appName("CSV to Delta") \
    .getOrCreate()

# Define the directory with CSV files
lakehouse_directory = ""

# Read all CSV files in the directory
df = spark.read.csv(lakehouse_directory, header=True, inferSchema=True) \
    .withColumn("filename", input_file_name())

# Extract and clean filenames
df = df.withColumn("filename", regexp_extract("filename", ".*/([^/?]+)\?.*", 1))
df = df.withColumn("filename", regexp_replace("filename", "\.CSV.*$", ".CSV"))

# Get a list of unique filenames
filename_list = df.select("filename").distinct().collect()
csv_files = [row.filename for row in filename_list]

# Process each file
for filename in csv_files:
    file_path = f"{lakehouse_directory}/{filename}"
    
    # Load data from the file
    df = spark.read.format("csv").option("header", "true").load(file_path)
    
    # Create table name from the filename
    table_name = os.path.splitext(filename)[0]
    
    # Save as Delta table
    df.write.format("csv").mode("overwrite").saveAsTable(table_name)
    
    print(f"Processed file {filename} and saved as table {table_name}.")

spark.stop()

