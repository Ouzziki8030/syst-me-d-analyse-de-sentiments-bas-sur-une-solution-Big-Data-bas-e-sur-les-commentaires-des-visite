from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import operator
import numpy as np
import matplotlib.pyplot as plt
def make_plot(counts):
    plt.xlabel('Time Step')
    plt.ylabel('Word Count')
    positive = []
    negative = []
    count = []
    print (counts)
    for timestep in counts:
        positive.append(int(timestep[0]))
        negative.append(int(timestep[1]))
        for i in range(0,len(positive)):
            count.append(i)
            pos1, = plt.plot(count,positive)
            neg1, = plt.plot(count,negative)
            plt.legend([pos1,neg1],['Positive','Negative'])
            plt.axis([0,10,0,300])
            plt.show()
def main():
    conf = SparkConf().setMaster("local[2]").setAppName("Streamer")
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, 10) # Create a streaming context with batch interval of 10 sec
    ssc.checkpoint("checkpoint")
    pwords = load_wordlist("positive.txt")
    nwords = load_wordlist("negative.txt")
    counts = stream(ssc, pwords, nwords, 100)
    make_plot(counts)
def load_wordlist(filename):
    file = open(filename,'rU')
    words = set(line.strip() for line in file)
    return words
def orientation(word,pwords,nwords):
    if word in pwords:
       return ('positive',1)
    elif word in nwords:
       return ('negative',1)
def updateFunction(newValues, runningCount):
    if runningCount is None:
       runningCount = 0
    return sum(newValues, runningCount)