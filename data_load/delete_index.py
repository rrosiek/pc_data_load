import requests
import sys

LOCAL_SERVER = 'http://localhost:9200'

def delete_index(server, index):
    print 'Deleting index', index
    url = server + '/' + index

    response = requests.delete(url)
    print(str(response.status_code))
    if response.status_code == 200 or response.status_code == 201:
        print(str(response.text))
        return True
    else:
        print(str(response.text))
        return False

if __name__ == "__main__":
    # print "This is the name of the script: ", sys.argv[0]
    # print "Number of arguments: ", len(sys.argv)
    # print "The arguments are: ", str(sys.argv)

    if len(sys.argv) >= 2:
        name = sys.argv[1]
        delete_index(LOCAL_SERVER, name)
    else:
        print 'Enter an index name'
