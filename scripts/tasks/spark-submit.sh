#!/usr/bin/env bash

ficheroInput=$1
outputPath=$1

cluster_info=`az hdinsight list --resource-group  grupoRecursosTfm`
cluster_name=`echo $cluster_info | jq -r ".[0].name"`
sshUser=`echo $cluster_info | jq -r ".[0].properties.computeProfile.roles[0].osProfile.linuxOperatingSystemProfile.username"`
pathAccount=`echo $cluster_info | jq -r ".[0].properties.storageProfile.storageaccounts[0].name"`
DATA_LAKE_MAIN_PATH=abfs://contenedoralmacenamiento@${pathAccount}/
sshHostName=${sshUser}@${cluster_name}-ssh.azurehdinsight.net

# Declaración de comandos a ejecutar en cluster
command="hdfs dfs -cp $DATA_LAKE_MAIN_PATH$ficheroInput $outputPath"
#commandExecuteSparkSubmit="spark-submit --conf spark.yarn.maxAppAttempts=1 --master yarn --deploy-mode cluster --class tfm.Main cleaning_lib.jar"
commandExecuteSparkSubmit="echo \"testing\""
echo "Usuario actual : $USER"

sudo chown -v $USER ~/.ssh/known_hosts
sudo chown -v $USER /home/rortizrojo/.ssh/known_hosts

echo "Ejecutando ssh-keygen rortizrojo"
sudo ssh-keygen -f "/home/rortizrojo/.ssh/known_hosts" -R "${cluster_name}-ssh.azurehdinsight.net"
echo "Ejecutando ssh-keygen jenkins"
ssh-keygen -f "/var/lib/jenkins/.ssh/known_hosts" -R "${cluster_name}-ssh.azurehdinsight.net"


# Copia de fichero jar a cluster
echo "Subiendo jar al cluster"
sshpass -p 'tfmPassword.2019' scp -o StrictHostKeyChecking=no target/tfmSpark-1.0-SNAPSHOT.jar $sshHostName:cleaning_lib.jar
#Ejecución de comandos
echo "Ejecutando comando: $command"
echo "Ejecutando comando: $sshHostName"
echo "Ejecutando spark-submit: $commandExecuteSparkSubmit"
sshpass -p 'tfmPassword.2019' ssh -tt $sshHostName -o StrictHostKeyChecking=no "$command;$commandExecuteSparkSubmit"

exit