import json
import sys
import random
import time
import csv
from binascii import hexlify
from pyspark import SparkContext

NUM_HASH = 11
a = random.sample(range(0, sys.maxsize-1), NUM_HASH)
b = random.sample(range(1, sys.maxsize-1), NUM_HASH)


def genHashFunc(numUsers):
    hashList = []
    def hash1(i, j):
        def hashed(param_x):
            p = ((i * param_x + j) % 99999989) % numUsers
            return p
        return hashed

    for i, j in zip(a, b):
        hashList.append(hash1(i, j))

    return hashList


def getPrediction(item, bit_stream):
    hig = []
    if item is not None and item != "":
        item_idx = int(hexlify(item.encode('utf8')),16)
        sig = set([hashed(item_idx) for hashed in hashes])
        if sig.issubset(bit_stream):
            hig.append(1)
        else:
            hig.append(0)
    else:
        hig.append(0)
    return hig


if __name__=="__main__":
    st = time.time()
    sc = SparkContext("local[*]")
    sc.setLogLevel("ERROR")

    output_file = sys.argv[3]

    training_data = sc.textFile(sys.argv[1]).map(lambda x: json.loads(x)).map(lambda x: x["city"]).distinct()\
        .filter(lambda x: x != "").map(lambda x: int(hexlify(x.encode('utf8')), 16))

    len1 = training_data.count()
    hashes = genHashFunc(len1)

    city_hash = training_data.flatMap(lambda x: [hashed(x) for hashed in hashes]).collect()
    bit_stream = set(city_hash)

    predict_test = sc.textFile(sys.argv[2]).map(lambda x: json.loads(x)).map(lambda x: x["city"])\
        .flatMap(lambda x: getPrediction(x, bit_stream)).collect()

    with open(output_file, "w") as f:
        writer = csv.writer(f, delimiter=' ')
        writer.writerow(predict_test)

    total_time = time.time() - st
    print("Duration: ", total_time)
