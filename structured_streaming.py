from pyspark.sql import SparkSession

if __name__ == '__main__' :
	spark=SparkSession.builder.appName("structured Streaming").getOrCreate()
	kafkaStream=spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", "foobar").option("startingOffsets","earliest").load()
	kafkaStream.printSchema()

#	outStream = kafkaStream.selectExpr("CAST(value AS STRING)").writeStream.format("csv").option("path","/home/hemlatha/Documents/Bigdata/rnd/out")    \
#				.option("checkpointLocation","/home/hemlatha/Documents/Bigdata/rnd/ss1/")					\
#				.start()
#	aggrStream = kafkaStream.selectExpr("CAST(value AS STRING)").groupBy("value", window("start_time","1 minute")).count()		\

	aggrStream = kafkaStream.selectExpr("CAST(value AS STRING)").groupBy("value").count()
	outStream  = aggrStream.writeStream							\
				.outputMode("complete")											\
				.format("console")											\
				.start()
	outStream.awaitTermination()


