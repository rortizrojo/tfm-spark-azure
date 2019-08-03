#!/usr/bin/env bash

cluster_info=`az hdinsight list --resource-group  grupoRecursosTfm`
cluster_name=`echo $cluster_info | jq -r ".[0].name"`
sshUser=`echo $cluster_info | jq -r ".[0].properties.computeProfile.roles[0].osProfile.linuxOperatingSystemProfile.username"`
pathAccount=`echo $cluster_info | jq -r ".[0].properties.storageProfile.storageaccounts[0].name"`
contenedor=`echo $cluster_info | jq -r ".[0].properties.storageProfile.storageaccounts[0].fileSystem"`
DATA_LAKE_MAIN_PATH=abfs://${contenedor}@${pathAccount}/
sshHostName=${sshUser}@${cluster_name}-ssh.azurehdinsight.net
sshpass -p 'tfmPassword.2019' ssh -tt $sshHostName -o StrictHostKeyChecking=no

hdfs dfs -ls \$DATA_LAKE_MAIN_PATH
exit