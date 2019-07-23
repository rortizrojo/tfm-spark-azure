#!/bin/bash
# A simple Azure Storage example script

export AZURE_STORAGE_ACCOUNT=$1
export AZURE_STORAGE_KEY=$2

export container_name=contenedordatos1
export blob_name=nombre_blob
export file_to_upload=MuestraDatos.csv
export destination_file=muestraSubido.csv


sas_token=$(az storage account generate-sas --account-key $AZURE_STORAGE_KEY --account-name $AZURE_STORAGE_ACCOUNT --expiry 2020-01-01 --https-only --permissions acuw --resource-types co --services bfqt)

echo "Creating the container...$container_name"
url=https://$AZURE_STORAGE_ACCOUNT.dfs.core.windows.net/$container_name?$sas_token

url="${url//\"/}"
echo $url
./azcopy make $url



#
#az storage container create --name $container_name
#
#echo "Uploading the file..."
#az storage blob upload --container-name $container_name --file $file_to_upload --name $blob_name
#
#echo "Listing the blobs..."
#az storage blob list --container-name $container_name --output table
#
#echo "Downloading the file..."
#az storage blob download --container-name $container_name --name $blob_name --file $destination_file --output table
#
#echo "Done"