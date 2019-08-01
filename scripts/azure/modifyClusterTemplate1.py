import json

import argparse
def modify(args):

    print(args)

    with open(args.clusterTemplateFile, 'r+') as f:
        data = json.load(f)

       key = "[listKeys('/subscriptions/5a08e9e0-83ee-4188-8fc2-18e16c7db524/resourceGroups/" + args.resource_group + "/providers/Microsoft.Storage/storageAccounts/" + args.storageName + "', '2015-05-01-preview').key1]"

        # Cluster storage Options
        data['resources'][0]['properties']['storageProfile']['storageaccounts'][0]['name']=args.storageName + ".blob.core.windows.net"
        data['resources'][0]['properties']['storageProfile']['storageaccounts'][0]['container']=args.container
        data['resources'][0]['properties']['storageProfile']['storageaccounts'][0]['key']=key

        f.seek(0)        # <--- should reset file position to the beginning.
        json.dump(data, f, indent=4)
        f.truncate()     # remove remaining part


    with open(args.clusterParametersFile, 'r+') as f:
        data = json.load(f)

        # Cluster identity parameters
        data['parameters']['clusterName']['value']=args.name


        f.seek(0)        # <--- should reset file position to the beginning.
        json.dump(data, f, indent=4)
        f.truncate()     # remove remaining part

if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument('-ctf', '--clusterTemplateFile')
    parser.add_argument('-cpf', '--clusterParametersFile')
    parser.add_argument('-sn', '--storageName')
    parser.add_argument('-rg', '--resource_group')
    parser.add_argument('-c', '--container')
    parser.add_argument('-n', '--name')

    #parser.add_argument('-v', dest='verbose', action='store_true')
    args = parser.parse_args()

    modify(args)
