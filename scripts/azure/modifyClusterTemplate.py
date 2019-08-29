import json

import argparse
def modify(args):

    print(args)

    with open(args.clusterTemplateFile, 'r+') as f:
        data = json.load(f)


        # Cluster identity parameters
        data['resources'][0]['identity']['userAssignedIdentities']={ args.userAssignedIdentities: {}}


        # Cluster storage Options
        data['resources'][0]['properties']['storageProfile']['storageaccounts'][0]['name']=args.storageName + ".dfs.core.windows.net"
        data['resources'][0]['properties']['storageProfile']['storageaccounts'][0]['resourceId']=args.storageId
        data['resources'][0]['properties']['storageProfile']['storageaccounts'][0]['msiResourceId']=args.userAssignedIdentities
        data['resources'][0]['properties']['storageProfile']['storageaccounts'][0]['fileSystem']=args.container

        # Cluster size options
        # Head Nodes
        data['resources'][0]['properties']['computeProfile']['roles'][0]['targetInstanceCount']=int(args.head_node_instances)
        data['resources'][0]['properties']['computeProfile']['roles'][0]['hardwareProfile']['vmSize']=args.head_node_type
        # Worker Nodes
        data['resources'][0]['properties']['computeProfile']['roles'][1]['targetInstanceCount']=int(args.worker_node_instances)
        data['resources'][0]['properties']['computeProfile']['roles'][1]['hardwareProfile']['vmSize']=args.worker_node_type


        f.seek(0)        # <--- should reset file position to the beginning.
        json.dump(data, f, indent=4)
        f.truncate()     # remove remaining part


    with open(args.clusterParametersFile, 'r+') as f:
        data = json.load(f)

        # Cluster identity parameters
        data['parameters']['clusterName']['value']=args.name
        # Cluster worker node number
        data['parameters']['clusterWorkerNodeCount']['value']=int(args.worker_node_instances)

        f.seek(0)        # <--- should reset file position to the beginning.
        json.dump(data, f, indent=4)
        f.truncate()     # remove remaining part

if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument('-ctf', '--clusterTemplateFile')
    parser.add_argument('-cpf', '--clusterParametersFile')
    parser.add_argument('-uai', '--userAssignedIdentities')
    parser.add_argument('-si', '--storageId')
    parser.add_argument('-sn', '--storageName')
    parser.add_argument('-c', '--container')
    parser.add_argument('-n', '--name')
    parser.add_argument('-hnt', '--head_node_type')
    parser.add_argument('-wnt', '--worker_node_type')
    parser.add_argument('-hni', '--head_node_instances')
    parser.add_argument('-wni', '--worker_node_instances')

    #parser.add_argument('-v', dest='verbose', action='store_true')
    args = parser.parse_args()

    modify(args)
