import sys
import json
import time
import random
import copy
import datetime
from binascii import hexlify
from pyspark import SparkContext
import pyspark.streaming

DURATION = 5
WINDOW_LENGTH = 30
SLIDING_INTERVAL = 10

NUM_HASH = 17
NUM_BIT = 20
idx = 0


def record(tme, gt, est):
    global idx
    idx += 1
    data_rec[str(idx)] = (tme, gt, est)
    with open(output_file, "a", encoding='utf8') as f1:
        f1.write(tme + "," + str(gt) + "," + str(est) + "\n")
    f1.close()


class KMEANS():
    def __init__(self, k, maxIter):
        self.nof_cluster = k
        self.maxIter = maxIter
        self.data_lst = None

    def update_cluster_size(self):
        if len(self.data_lst) < self.nof_cluster:
            self.nof_cluster = self.data_lst

    def _init_centroid(self, seed):
        random.seed(seed)
        self.centroid_log = dict()
        self.clusters = dict()
        self.flag_centroid = dict()
        for idx, val in enumerate(random.sample(self.data_lst, self.nof_cluster)):
            self.centroid_log.setdefault("cluster" + str(idx), float(val))
            self.clusters.setdefault("cluster" + str(idx), list())
            self.flag_centroid.setdefault("cluster" + str(idx), False)

    def update_centroid(self):
        prev_centroid = copy.deepcopy(self.centroid_log)
        for centroid, loc in self.clusters.items():
            if not self.flag_centroid.get(centroid):
                tmp = [self.centroid_log.get(centroid)]
                tmp.extend(loc)

                self.centroid_log[centroid] = float(sum(tmp) / len(tmp))

        return prev_centroid, self.centroid_log

    def tell_change(self, A: dict, B: dict):
        for k in A.keys():
            if round(A.get(k, 1)) != round(B.get(k, 1)):
                self.flag_centroid[k] = False
                return True
            else:
                self.flag_centroid[k] = True
        return False

    def del_cluster(self):
        for k in self.clusters.keys():
            self.clusters[k] = list()

    def fit(self, data_lst, seed=666):
        self.data_lst = data_lst
        self.update_cluster_size()
        self._init_centroid(seed)
        epochs = 1
        while True:
            for item in self.data_lst:
                tmp1 = dict()
                for c in self.centroid_log.keys():
                    tmp1[(c, item)] = abs(self.centroid_log[c] - item)
                assign_log = list(sorted(tmp1.items(), key=lambda x: x[1]))[:1]
                self.clusters[assign_log[0][0][0]].append(assign_log[0][0][1])

            prev_c, curr_c = self.update_centroid()
            if not self.tell_change(prev_c, curr_c) or epochs >= self.maxIter:
                break
            self.del_cluster()
            epochs += 1

        return self.centroid_log, self.clusters


def Flajolet_Martin(iterator):
    tme = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
    unique_city = gt = list(set(iterator.collect()))

    random.seed(666)
    a = random.sample(range(0, sys.maxsize-1), NUM_HASH)
    b = random.sample(range(1, sys.maxsize-1), NUM_HASH)

    est_lst = list()

    for i, j in zip(a, b):
        max_zero = float('-inf')
        for city in unique_city:
            idx1 = int(hexlify(city.encode('utf8')), 16)

            hashify = format(int((i * idx1 + j) % 99999989), 'b').zfill(NUM_BIT)

            num_zero = 0 if hashify == 0 else len(str(hashify)) - len(str(hashify).rstrip("0"))
            max_zero = max(max_zero, num_zero)
        est_lst.append(pow(2, max_zero))

    log, clusters = KMEANS(k=4, maxIter=5).fit(est_lst)

    cluster_avg = sorted(map(lambda x: sum(x) / max(len(x), 1), list(clusters.values())))

    record(tme, len(gt), int(cluster_avg[1]))


if __name__=="__main__":
    st = time.time()
    sc = SparkContext("local[*]")
    sc.setLogLevel("ERROR")
    stc = pyspark.streaming.StreamingContext(sc, DURATION)
    output_file = sys.argv[2]
    port = int(sys.argv[1])

    data_rec = dict()
    data_rec["header"] = ("Time", "Ground Truth", "Estimation")
    with open(output_file, "w", encoding='utf8') as f:
        f.write("Time,Ground Truth,Estimation\n")
    f.close()

    input_stream = stc.socketTextStream("localhost", port)

    stream_data = input_stream.window(WINDOW_LENGTH, SLIDING_INTERVAL).map(lambda x: json.loads(x))\
        .map(lambda x: x["city"]).filter(lambda x: x != '').foreachRDD(Flajolet_Martin)

    stc.start()
    stc.awaitTermination()
