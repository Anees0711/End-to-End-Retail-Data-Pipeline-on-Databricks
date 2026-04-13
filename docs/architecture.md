# Architecture

## High-Level Flow

1. Source files are uploaded into Unity Catalog Volumes
2. Bronze notebooks ingest raw files into Delta tables
3. Silver notebooks clean, validate, and merge transformed data
4. Gold notebooks create reporting-ready tables
5. Power BI consumes curated Gold outputs

## Data Flow

Customer CSV -> Bronze Customer -> Silver Customers  
Product JSON -> Bronze Products -> Silver Products  
Transaction Parquet -> Bronze Transactions -> Silver Orders  
Silver Orders + Silver Products -> Gold Category Sales  
Silver Orders -> Gold Daily Sales

## Storage Note

The original course used DBFS.  
This implementation was adapted to Unity Catalog Volumes to align with the current Databricks environment.