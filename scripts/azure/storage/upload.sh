#!/bin/bash
# A simple Azure Storage example script

export AZURE_STORAGE_ACCOUNT=$1
export AZURE_STORAGE_KEY=$2

export container_name=contenedordatos2
export blob_name=nombre_blob
export file_to_upload=$3
export destination_file=$4


end=`date -d "1 year" '+%Y-%m-%dT%H:%M:%SZ'`
echo "Expire time: $end"
sas_token=$(az storage account generate-sas --permissions rwdlacup --account-name $AZURE_STORAGE_ACCOUNT --services bfqt --resource-types sco --expiry $end -otsv)
#sas_token=$(az storage account generate-sas --account-key $AZURE_STORAGE_KEY --account-name $AZURE_STORAGE_ACCOUNT --expiry $end -otsv --permissions rwdlacup --resource-types co --services bfqt)

echo "Creating the container...$container_name"
container_url=https://$AZURE_STORAGE_ACCOUNT.dfs.core.windows.net/$container_name
echo "Container: $container_url"
container_url_sas=$container_url?$sas_token


container_url_sas_clean="${container_url_sas//\"/}"
echo "Container clean: $container_url_sas_clean"
azcopy make $container_url_sas_clean



file_name=$container_url/$destination_file?$sas_token
file_url="${file_name//\"/}"
echo "File clean: $file_url"
azcopy copy $file_to_upload $file_url --recursive=true

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