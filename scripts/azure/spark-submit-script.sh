#!/usr/bin/env bash

ficheroInput=$1
outputPath=$1

cluster_info=`az hdinsight list --resource-group  grupoRecursosTfm`
cluster_name=`echo $cluster_info | jq -r ".[0].name"`
sshUser=`echo $cluster_info | jq -r ".[0].properties.computeProfile.roles[0].osProfile.linuxOperatingSystemProfile.username"`
pathAccount=`echo $cluster_info | jq -r ".[0].properties.storageProfile.storageaccounts[0].name"`
DATA_LAKE_MAIN_PATH=abfs://contenedoralmacenamiento@${pathAccount}/
sshHostName=${sshUser}@${cluster_name}-ssh.azurehdinsight.net

command="hdfs dfs -cp $DATA_LAKE_MAIN_PATH$ficheroInput $outputPath"
commandExecuteSparkSubmit="spark-submit --master yarn --deploy-mode cluster --py-files my_arch.zip prueba.py"

sudo zip -r my_arch.zip scripts/python/*.py
echo "Comando: $command"
echo "SSH HOST: $sshHostName"
ssh-keygen -f "/var/lib/jenkins/.ssh/known_hosts" -R "${cluster_name}-ssh.azurehdinsight.net"
sshpass -p 'tfmPassword.2019' scp -o StrictHostKeyChecking=no my_arch.zip $sshHostName:my_arch.zip
sshpass -p 'tfmPassword.2019' scp -o StrictHostKeyChecking=no scripts/python/prueba.py $sshHostName:prueba.py
sshpass -p 'tfmPassword.2019' ssh -tt $sshHostName -o StrictHostKeyChecking=no "$command;$commandExecuteSparkSubmit"

exit