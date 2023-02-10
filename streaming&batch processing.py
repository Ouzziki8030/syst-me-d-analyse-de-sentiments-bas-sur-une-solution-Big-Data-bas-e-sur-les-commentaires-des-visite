def stream(ssc, pwords, nwords, duration):
    kstream = KafkaUtils.createDirectStream(
    ssc, topics = ['twitterstream'], kafkaParams = {"metadata.broker.list": 'localhost:9092'})
    tweets = kstream.map(lambda x: x[1].encode("ascii","ignore"))
    print (function)
    words = tweets.flatMap(lambda k: k.split(" "))
    wordsType = words.map(lambda k: orientation(k,pwords,nwords))
    ct = wordsType.reduceByKey(lambda a, b : a + b)
    # Let the counts variable hold the word counts for all time steps
    # You will need to use the foreachRDD function.
    # For our implementation, counts looked like:
    # [[("positive", 100), ("negative", 50)], [("positive", 80), ("negative", 60)], ...]
    counts = []
    cts = ct.updateStateByKey(updateFunction)
    ct.foreachRDD(lambda t,rdd : counts.append(rdd.collect()))
    cts.pprint()
  
    ssc.start() # Start the computation
    ssc.awaitTerminationOrTimeout(duration)
    ssc.stop(stopGraceFully=True)
    return counts
    if __name__=="__main__":
        main()
