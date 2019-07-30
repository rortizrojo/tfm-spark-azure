import json

import argparse
def modify(args):

    print(args)

    with open(args.storageFile, 'r+') as f:
        data = json.load(f)
        data['parameters']['storageAccountName']['value']=args.storageName # <--- add `id` value.

        f.seek(0)        # <--- should reset file position to the beginning.
        json.dump(data, f, indent=4)
        f.truncate()     # remove remaining part

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-sf', '--storageFile')
    parser.add_argument('-sn', '--storageName')

    #parser.add_argument('-v', dest='verbose', action='store_true')
    args = parser.parse_args()

    modify(args)