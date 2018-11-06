import requests
import sys

from_node = 'node-secondary'
to_node = 'node-primary'

def migrate_index(index_name):
    no_of_shards = 5

    url = 'http://ocat-dev.altum.com:9200/_cluster/reroute'

    for i in range(0, no_of_shards):
        data = {
            "commands": [
                {
                    "move": {
                        "index": index_name,
                        "shard": i,
                        "from_node": from_node,
                        "to_node": to_node
                    }
                }
            ]
        }

        print 'Migrating shard', i, 'of', index_name, 'from', from_node, 'to', to_node

        response = requests.post(url, json=data)
        print response            

        if response.status_code == 200:
            print 'Success'
        else:
            print (response.status_code)
            print (response.text)



# migrate_index('fda_purple_book')
   
if __name__ == "__main__":
    print "This is the name of the script: ", sys.argv[0]
    print "Number of arguments: ", len(sys.argv)
    print "The arguments are: ", str(sys.argv)

    if len(sys.argv) >= 2:
        name = sys.argv[1]
        migrate_index(name)
    else:
        print 'Enter an index name'
