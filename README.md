**ETL Pipeline On Google Cloud**

The purpose of this project is to build a simple ETL pipeline using Google Cloud Service

**PROJECT SUMMARY**
- Created a Cloud Function using python to authenticate with the CoinCap API and extract live data from CoinCap. 
 
- Created a schedule trigger to run the Cloud Function on a regular basis using Google Cloud Scheduler. 
 
- Created a BigQuery table to store the data extracted from CoinCap used the BigQuery Data Transfer Service to load the data from the Cloud Function into the BigQuery table. 
 
- Connected Tableau to BigQuery to display the live data.
