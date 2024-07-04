# CSV to Delta Table Processor

This project provides a streamlined solution for processing CSV files in a directory and storing them as Delta tables using Spark in a Microsoft Fabric environment. The code reads all CSV files from a specified directory, cleans their filenames, and saves the data into the Spark SQL Metastore as Delta tables.

## Features

- **Automated CSV Processing:** Reads and processes all CSV files from a specified directory.
- **Filename Cleaning:** Extracts and cleans filenames to ensure consistency.
- **Delta Table Storage:** Saves processed data as Delta tables in the Spark SQL Metastore for efficient querying and analysis.
- **Scalable and Efficient:** Utilizes Spark's distributed processing capabilities for handling large datasets.

## Prerequisites

- **Microsoft Fabric Environment:** Ensure you have access to a Microsoft Fabric environment with Spark capabilities.
- **PySpark:** Ensure PySpark is installed in your environment.
