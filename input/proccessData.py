from pprint import pprint
import json
import pickle

def readData(file):
    data = []
    with open(file) as f:
        for line in f:
            data.append(json.loads(line))
    return data

def prune(data, dim):
    pruned = {}
    if dim == 3:
        for d in data:
            pruned[d['name']] = d['state']+'\t'+str(d['latitude'])+'\t'+str(d['longitude'])+'\t'+str(d['stars'])+'\t'+str(d['review_count'])+'\t'+str(d['categories'])
    elif dim == 2:
        for d in data:
            pruned[d['name']] = d['state']+'\t'+d['postal_code']+'\t'+str(d['stars'])+'\t'+str(d['review_count'])+'\t'+str(d['categories'])
    return pruned

def prune2D(data):
    pruned = {}

    return pruned

def save_obj(obj, name):
    with open('input/'+name, 'w') as f:
        for name in list(obj):
            f.write(name + ": " + obj[name] + '\n')

def main():
    print("python main function")
    data = readData('yelp_business.json')
    pruned = prune(data, 3)
    save_obj(pruned, "3DClustering.txt")
    pruned = prune(data, 2)
    save_obj(pruned, "2DClustering.txt")


if __name__ == '__main__':
    main()
