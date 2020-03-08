#skara2_twitterstream
from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import operator
import numpy as np
import matplotlib.pyplot as plt

def main():
    conf = SparkConf().setMaster("local[2]").setAppName("Streamer")
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, 10)   # Create a streaming context with batch interval of 10 sec
    ssc.checkpoint("checkpoint")

    pwords = load_wordlist("positive.txt")
    nwords = load_wordlist("negative.txt")   
    counts = stream(ssc, pwords, nwords, 100)
    make_plot(counts)  


def make_plot(counts):
    """
    Plot the counts for the positive and negative words for each timestep.
    Use plt.show() so that the plot will popup.
    """
    # YOUR CODE HERE
    positive_count = [count[0][1] for count in counts]
    negative_count = [count[1][1] for count in counts]
    plt.plot(positive_count, 'co-' ,marker='o',  markersize=12, label='positive')
    plt.plot(negative_count, 'ms-' ,marker='s',  markersize=12, label='negative')   
    plt.xlabel('Time step')
    plt.ylabel('Word Count')
    plt.title('Twitter Sentiment Analysis')
    plt.legend(loc='upper left')
    plt.show()
    plt.savefig('skara2_plot.png')    

def load_wordlist(filename):
    """ 
    This function should return a list or set of words from the given filename.
    """
     
    # YOUR CODE HERE
    lines = open(filename,'r').read()   
    words = [f.strip().split('\n') for f in lines]   
    return words
     
# Learnt from http://spark.apache.org/docs/latest/streaming-programming-guide.html#spark-streaming-programming-guide 
def updateFunction(newValues, runningCount):
    if runningCount is None:
        runningCount = 0
    return sum(newValues, runningCount)

def stream(ssc, pwords, nwords, duration):
    kstream = KafkaUtils.createDirectStream(
        ssc, topics = ['twitterstream'], kafkaParams = {"metadata.broker.list": 'localhost:9092'})
    tweets = kstream.map(lambda x: x[1])  

    # Each element of tweets will be the text of a tweet.
    # You need to find the count of all the positive and negative words in these tweets.
    # Keep track of a running total counts and print this at every time step (use the pprint function).
    # YOUR CODE HERE  
    tweets_split = tweets.flatMap(lambda tweetline: tweetline.split(" ").lower())
    p_words = tweets_split.map(lambda x: ("positive", 1) if x in pwords else ("positive", 0))
    n_words = tweets_split.map(lambda x: ("negative", 1) if x in nwords else ("negative", 0))
    all_words = p_words.join(n_words)     
    all_words = all_words.reduceByKey(lambda x,y:x+y) 
    runningCount = all_words.updateStateByKey(updateFunction)
    runningCount.pprint()
    
    # Let the counts variable hold the word counts for all time steps
    # You will need to use the foreachRDD function.
    # For our implementation, counts looked like:
    #   [[("positive", 100), ("negative", 50)], [("positive", 80), ("negative", 60)], ...]
    counts = []
    all_words.foreachRDD(lambda t,rdd: counts.append(rdd.collect()))
    
    ssc.start()                         # Start the computation
    ssc.awaitTerminationOrTimeout(duration)
    ssc.stop(stopGraceFully=True)

    return counts

if __name__=="__main__":
    main()
