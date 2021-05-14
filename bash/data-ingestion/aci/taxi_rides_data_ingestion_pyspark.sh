#!/bin/bash

AUTHN_PATH="/taxi_rides/authn"
if [ -d "${AUTHN_PATH}" ]; then
  AZ_TENANT_ID=$(cat $AUTHN_PATH/tenant_id)
  AZ_CLIENT_ID=$(cat $AUTHN_PATH/client_id)
  AZ_SECRET=$(cat $AUTHN_PATH/secret)
else
  mkdir -p $AUTHN_PATH
  az login --identity
  AZ_TENANT_ID=$(az keyvault secret show \
    --name "taxi-rides-data-ingestion-tenant-id" \
    --vault-name "taxi-rides-key-vault" --query value --output tsv)
  echo $AZ_TENANT_ID > $AUTHN_PATH/tenant_id
  AZ_CLIENT_ID=$(az keyvault secret show \
    --name "taxi-rides-data-ingestion-client-id" \
    --vault-name "taxi-rides-key-vault" --query value --output tsv)
  echo $AZ_CLIENT_ID > $AUTHN_PATH/client_id
  AZ_SECRET=$(az keyvault secret show \
    --name "taxi-rides-data-ingestion-secret" \
    --vault-name "taxi-rides-key-vault" --query value --output tsv)
  echo $AZ_SECRET > $AUTHN_PATH/secret
  az logout
fi

AZ_JARS_PATH="/taxi_rides/jars"
if [ -d "${AZ_JARS_PATH}" ]; then
  AZ_JARS=$(readlink -e $AZ_JARS_PATH/*.jar | xargs | sed 's/ /,/g')
fi

DATA_INGESTION_ARGS="${@} --jars ${AZ_JARS} --azure-tenant-id ${AZ_TENANT_ID} --azure-client-id ${AZ_CLIENT_ID} --azure-client-secret ${AZ_SECRET}"

source /venv/bin/activate
python /taxi_rides/python/taxi_rides_data_ingestion.py pyspark ${DATA_INGESTION_ARGS}