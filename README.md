**ETL Pipeline On Google Cloud**

The purpose of this project was to build a simple ETL pipeline using Google Cloud Service to ingest live prices of crypto assets to BigQuery

**PROJECT SUMMARY**
- Created a Cloud Function using python to authenticate with the CoinCap API and extract live data from CoinCap. 
 
- Created a schedule trigger to run the Cloud Function on a regular basis using Google Cloud Scheduler. 
 
- Created a BigQuery table to store the data extracted from CoinCap used the BigQuery Data Transfer Service to load the data from the Cloud Function into the BigQuery table. 
 
- Connected Tableau to BigQuery to display the live data.

Here's the link showing a step-by-step process: https://medium.com/@tyebunoluwa/etl-automating-cryptocurrency-price-updates-with-coincap-api-and-google-bigquery-f25a2555d175
