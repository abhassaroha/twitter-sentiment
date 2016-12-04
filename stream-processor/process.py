from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
count = 0
ssc = None

def terminate(rdd):
    global count
    count = count + 1
    if count == 10:
        ssc.stop()

def run(sc):
    global ssc
    ssc = StreamingContext(sc, 1)
    directKafkaStream = KafkaUtils.createDirectStream(ssc, ["twitter"],
            {"metadata.broker.list":"localhost:9092", 
                "auto.offset.reset": "smallest"}) 
    lines = directKafkaStream.map(lambda tuple: tuple[1]) 
    lines.persist()
    wordTuples = lines.flatMap(lambda line: line.split())
    pairs = words.map(lambda word: (word, 1))
    wordCounts = pairs.reduceByKey(lambda x, y: x + y)
    wordCounts.pprint()
    directKafkaStream.foreachRDD(terminate)
    ssc.start()
    ssc.awaitTermination()

