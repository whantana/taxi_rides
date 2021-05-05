#!/bin/bash

AZ_STORAGE_ACCOUNT="whantanataxirides"
APPLICATION_ID="http://taxi-rides-data-ingestion"
SECRET_PATH="/taxi_rides/cert/secret"

if [ -f "${SECRET_PATH}" ]; then
    echo "$SECRET_PATH exists."
    AZ_CLIENT_SECRET=$(cat $SECRET_PATH)
    AZ_TENANT_ID="4e4d108a-87d8-470c-a85b-ed8a73cad109"
else
    echo "$SECRET_PATH does not exist."
    az login --identity
    AZ_CLIENT_SECRET=$(az keyvault secret show \
    --name "taxi-rides-data-ingestion-secret" \
    --vault-name "taxi-rides-key-vault" --query value --output tsv)
    AZ_TENANT_ID=$(az account show --query "tenantId" --output tsv)
    az logout
    mkdir -p $(dirname $SECRET_PATH)
    echo $AZ_CLIENT_SECRET > $SECRET_PATH
fi

DATA_INGESTION_ARGS="${@} --azure-tenant-id ${AZ_TENANT_ID} --azure-storage-account-name ${AZ_STORAGE_ACCOUNT} --azure-client-id ${APPLICATION_ID} --azure-client-secret ${AZ_CLIENT_SECRET}"

source /venv/bin/activate

python /taxi_rides/python/taxi_rides_data_ingestion.py pandas ${DATA_INGESTION_ARGS}