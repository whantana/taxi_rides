#!/bin/bash

AZURE_TENANT_ID="4e4d108a-87d8-470c-a85b-ed8a73cad109"
AZURE_STORAGE_ACOUNT="whantanataxirides"
AZURE_CLIENT_ID="http://taxi-rides-data-ingestion"
AZURE_CLIENT_SECRET=""

python /taxi_rides/python/taxi_rides_ingestion.py pandas \
--taxi-trips-path abfs://taxi-rides-container/data/historical/2017_Yellow_Taxi_Trip_Data.csv \
--taxi-zones-path abfs://taxi-rides-container/data/zones/taxi_zones.csv \
--output-path abfs://taxi-rides-container/data/taxi_rides \
--samplesize 10000 \
--filter-pickup 2017-03-24:2017-03-26 \
--azure-tenant-id $AZURE_TENANT_ID \
--azure-storage-account-name $AZURE_STORAGE_ACOUNT \
--azure-client-id $AZURE_CLIENT_ID \
--azure-client-secret $AZURE_CLIENT_SECRET

