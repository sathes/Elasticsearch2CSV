from elasticsearch import Elasticsearch
import csv
import io

def esConnect(hostname, port, es_index, out_path=''):
    """Connect with Elasticsearch
       Pull data using search based on elasticsearch index
    """
    try:
        if hostname and port:
            host = str(hostname)
            port = int(port)
            #Connect to es host
            es = Elasticsearch([{'host': hostname, 'port': port}])
            print("Connected to {0}".format(es.info()))
    except Exception as ex:
        print("Connection Error: {0}".format(ex))
        return

    #Get avaialbe indexes
    indexes = list(es.indices.get_alias().keys())
    print("Indexs -- {0}".format(indexes))

    #Get all data for specific index
    if es_index and str(es_index) in indexes:
        data = es.search(index=str(es_index), body={"query": {"match_all": {}}})
    else:
        data = []
        print("Index '{0}' not available ".format(str(es_index)))
    if data:
        data = data['hits']['hits']
        header =[i[0] for i in get_tree(data[0])]
        processed = get_tree(data)
    if out_path:
        return render_csv(header,processed, out_path=out_path)
    else:
        return render_csv(header,processed)

def is_dict(item, ans=[]):
    tree = []
    for k,v in item.items():
        if isinstance(v,dict):
            ans.append(str(k))
            tree.extend(is_dict(v, ans))
            ans=[]
        else:
            if ans:
                ans.append(str(k))
                key = ','.join(ans).replace(',','.')
                tree.extend([(key, str(v))])
                ans.remove(str(k))
            else:
                tree.extend([(str(k),str(v))])
    return tree

def get_tree(item):
    tree = []
    if isinstance(item, dict):
        tree.extend(is_dict(item, ans=[]))
        return tree
    elif isinstance(item, list):
        tree = []
        for i in item:
            tree.append(get_tree(i))
        return tree
    else:
        tree.extend([(key, item)])
    return tree

def render_csv(header, data, out_path='out3.csv'):
    input = []
    with open(out_path, 'w') as f:
        dict_writer = csv.DictWriter(f, fieldnames=header)
        dict_writer.writeheader()
        for i in data:
            input.append(dict(i))
        dict_writer.writerows(input)
    return

if __name__ == '__main__':
    #esConnect(hostname='192.168.74.121', port='9200', es_index='test_demo', out_path='out3.csv')
    esConnect(hostname='192.168.74.121', port='9200', es_index='test_demo')