from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import sys
import os.path
import time

def updateFunction(currentState , previousState) :
	if previousState is None :
		previousState = [0]*3 
	if len(currentState) != 0 :
		previousState[0] += currentState[0][0]
		previousState[1] += currentState[0][1]
		previousState[2]  = previousState[0] / (previousState[1] * 1.0)
	if previousState[0] > 100 :
		previousState = None
	return previousState

def createStreamingContext(sc, kafka_topic, window_size, broker_list, checkpoint_path) :
	ssc = StreamingContext(sc,window_size)
	kafkaDstream = KafkaUtils.createDirectStream(ssc,[kafka_topic],{"metadata.broker.list":broker_list,"auto.offset.reset":"smallest" ,"enable.auto.commit" : "false" })
	countsRDD = kafkaDstream.map(lambda (k,v) : v.split(",") )				\
				.map(lambda list1 : (list1[0], (int(list1[1]),1) )  )		\
				.updateStateByKey(updateFunction)
	countsRDD.pprint()
	ssc.checkpoint(checkpoint_path)
	return ssc

def iskillflag():
	if os.path.exists('/home/hemlatha/Documents/Bigdata/rnd/killflag') :
		return True
	return False

if __name__ == "__main__" :
	if len(sys.argv) != 5 :
		print('Usage : pass parameters Kafka-topic, Broker-list, Checkpoint-path, Batch-window')
		exit(-1)

	kafka_topic, window_size, broker_list, checkpoint_path  = sys.argv[1:]
	stopFlag = False
	sc = SparkContext()
	ssc = StreamingContext.getOrCreate(checkpoint_path, lambda : createStreamingContext(sc, kafka_topic, int(window_size), broker_list, checkpoint_path ) )

	ssc.start()
#	ssc.awaitTermination()
	while not stopFlag :
		stopFlag = iskillflag()
		if stopFlag :		
			ssc.stop(True,True)
			sc.stop()
		else :
			time.sleep(2)
