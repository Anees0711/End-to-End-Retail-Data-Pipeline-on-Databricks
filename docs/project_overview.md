# Project Overview

This project demonstrates an end-to-end retail data engineering pipeline in Databricks.

The pipeline follows the Bronze, Silver, and Gold architecture:

- Bronze stores raw ingested data
- Silver stores cleaned and validated data
- Gold stores business-ready reporting outputs

The project handles customer, product, and transaction data from different source formats including CSV, JSON, and Parquet.

The final reporting outputs are designed to support dashboarding in Power BI.

A major practical aspect of this project was adapting the original course logic from legacy DBFS storage to Unity Catalog Volumes in the current Databricks environment.