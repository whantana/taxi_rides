#!/bin/bash

export AZ_TENANT_ID="4e4d108a-87d8-470c-a85b-ed8a73cad109"
export APPLICATION_ID="http://taxi-rides-data-ingestion"
export CERT_PATH="/taxi_rides/cert"
export PEM_CERT_PATH="${CERT_PATH}/taxi-rides-data-ingestion.pem"
export PFX_CERT_PATH="${CERT_PATH}/taxi-rides-data-ingestion.pfx"
export SECRET_PATH="/taxi_rides/cert/secret"

# --------------------------------------
# --- AzCopy ---------


export AZCOPY_TENANT_ID=$AZ_TENANT_ID
export AZCOPY_AUTO_LOGIN_TYPE="SPN"
export AZCOPY_SPA_APPLICATION_ID=$APPLICATION_ID
export AZCOPY_SPA_CERT_PATH=$PEM_CERT_PATH

# list
azcopy list  https://whantanataxirides.dfs.core.windows.net/taxi-rides-container


# --------------------------------------
# --- Az CLI ---------

# login with certificate
az login --service-principal \
--username ${APPLICATION_ID} \
--password ${PEM_CERT_PATH} \
--tenant ${AZ_TENANT_ID}
az logout

# login with secret/password
az login --service-principal \
--username ${APPLICATION_ID} \
--password $(cat $SECRET_PATH) \
--tenant ${AZ_TENANT_ID}

# --------------------------------------
# --- Docker push ---------

# login to Azure Container Registry
az acr login --name "taxiridescontainerregistry"

# after building image
docker build whantana/taxi-rides
docker tag whantana/taxi-rides taxiridescontainerregistry.azurecr.io/taxi-rides:latest
docker push taxiridescontainerregistry.azurecr.io/taxi-rides:latest


# Create container
az container create --name "taxi-rides-data-ingestion-container" \
--resource-group "taxi-rides-rg" \
--image "taxiridescontainerregistry.azurecr.io/taxi-rides:latest" \
--assign-identity $(az identity show -n  "taxi-rides-keyvault-secret-reader" -g "taxi-rides-rg" --query "id" -o "tsv") \
--cpu 4 --memory 12 \
--registry-login-server taxiridescontainerregistry.azurecr.io \
--registry-username $(az ad sp list --display-name "taxi-rides-data-ingestion" --query "[0].appId" -o "tsv") \
--registry-password $(cat $SECRET_PATH) \
--restart-policy "Never" \
--command-line "/bin/bash /taxi_rides/bash/taxi-rides-data-ingestion-pandas.sh \\
--taxi-trips-path abfs://taxi-rides-container/data/historical/2017_Yellow_Taxi_Trip_Data.csv \\
--taxi-zones-path abfs://taxi-rides-container/data/zones/taxi_zones.csv \\
--output-path abfs://taxi-rides-container/data/taxi_rides_pandas \\
--filter-pickup 2017 --chunksize 1000000"

## show container
#az container show --name "taxi-rides-data-ingestion-container" --resource-group "taxi-rides-rg"
## start container
#az container start --name "taxi-rides-data-ingestion-container" --resource-group "taxi-rides-rg"
## restart container
#az container restart --name "taxi-rides-data-ingestion-container" --resource-group "taxi-rides-rg"
## attach to logs
#az container attach --name "taxi-rides-data-ingestion-container" --resource-group "taxi-rides-rg"
# stop container
az container stop --name "taxi-rides-data-ingestion-container" --resource-group "taxi-rides-rg"
# stop container
az container delete --name "taxi-rides-data-ingestion-container" --resource-group "taxi-rides-rg" --yes

# Calculate and print data ingestion result size in bytes
total_sz=0
for d in $(az storage fs directory list --file-system "taxi-rides-container" --path "data/taxi_rides" --recursive false \
 --query "[].name" --output tsv --account-name "whantanataxirides"  --auth-mode login ); do
 sz=$(az storage fs file list --file-system "taxi-rides-container" --path "$d" --exclude-dir \
 --query "[].contentLength" --output tsv --account-name "whantanataxirides"  --auth-mode login | awk '{s+=$1} END {print s}')
 echo "${d: -8} : $sz"
 ((total_sz=total_sz+sz))
done
echo "Total : $total_sz"

# log out
az logout