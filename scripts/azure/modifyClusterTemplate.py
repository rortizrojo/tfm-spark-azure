import json

import argparse
def modify(args):

    print(args)

    with open(args.clusterFile, 'r+') as f:
        data = json.load(f)


        # Cluster identity parameters
        data['resources'][0]['identity']['userAssignedIdentities']={ args.userAssignedIdentities: {}}


        # Cluster storage Options
        data['resources'][0]['properties']['storageProfile']['storageaccounts'][0]['name']=args.storageName + ".dfs.core.windows.net"
        data['resources'][0]['properties']['storageProfile']['storageaccounts'][0]['resourceId']=args.storageId
        data['resources'][0]['properties']['storageProfile']['storageaccounts'][0]['msiResourceId']=args.userAssignedIdentities
        data['resources'][0]['properties']['storageProfile']['storageaccounts'][0]['fileSystem']=args.container


        f.seek(0)        # <--- should reset file position to the beginning.
        json.dump(data, f, indent=4)
        f.truncate()     # remove remaining part




if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument('-cf', '--clusterFile')
    parser.add_argument('-uai', '--userAssignedIdentities')
    parser.add_argument('-si', '--storageId')
    parser.add_argument('-sn', '--storageName')
    parser.add_argument('-c', '--container')

    #parser.add_argument('-v', dest='verbose', action='store_true')
    args = parser.parse_args()

    modify(args)
