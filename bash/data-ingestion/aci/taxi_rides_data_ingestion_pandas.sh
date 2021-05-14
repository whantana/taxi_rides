#!/bin/bash

AUTHN_PATH="/taxi_rides/authn"
if [ -d "${AUTHN_PATH}" ]; then
  AZ_TENANT_ID=$(cat $AUTHN_PATH/tenant_id)
  AZ_CLIENT_ID=$(cat $AUTHN_PATH/client_id)
  AZ_SECRET=$(cat $AUTHN_PATH/secret)
  AZ_STORAGE_ACCOUNT=$(cat $AUTHN_PATH/storage_account_name)
else
  mkdir -p $AUTHN_PATH
  az login --identity
  AZ_TENANT_ID=$(az keyvault secret show \
    --name "taxi-rides-data-ingestion-tenant-id" \
    --vault-name "taxi-rides-key-vault" --query value --output tsv)
  echo "$AZ_TENANT_ID" > $AUTHN_PATH/tenant_id
  AZ_CLIENT_ID=$(az keyvault secret show \
    --name "taxi-rides-data-ingestion-client-id" \
    --vault-name "taxi-rides-key-vault" --query value --output tsv)
  echo "$AZ_CLIENT_ID" > $AUTHN_PATH/client_id
  AZ_SECRET=$(az keyvault secret show \
    --name "taxi-rides-data-ingestion-secret" \
    --vault-name "taxi-rides-key-vault" --query value --output tsv)
  echo "$AZ_SECRET" > $AUTHN_PATH/secret
  AZ_STORAGE_ACCOUNT=$(az keyvault secret show \
    --name "taxi-rides-data-ingestion-storage-account-name" \
    --vault-name "taxi-rides-key-vault" --query value --output tsv)
  echo "$AZ_STORAGE_ACCOUNT" > $AUTHN_PATH/storage_account_name
  az logout
fi

DATA_INGESTION_ARGS="${@} --azure-tenant-id ${AZ_TENANT_ID} --azure-storage-account-name ${AZ_STORAGE_ACCOUNT} --azure-client-id ${AZ_CLIENT_ID} --azure-client-secret ${AZ_SECRET}"

source /venv/bin/activate
python /taxi_rides/python/taxi_rides_data_ingestion.py pandas ${DATA_INGESTION_ARGS}