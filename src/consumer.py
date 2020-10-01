#!/usr/bin/env python
import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2 pyspark-shell'

#Stream processing
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils


sparkConf = SparkContext(appName="StreamProcessorPyspark").getOrCreate()
sparkConf.setLogLevel("WARN")

streamingContext = StreamingContext(sparkConf, 10)
streamingContext.checkpoint('.checkpoints')

kafkaConf = {"metadata.broker.list": 'localhost:9092',
            "zookeeper.connect": 'localhost:2181',
            "zookeeper.connection.timeout.ms": '1000',
            "auto.offset.reset": 'smallest'}


messages = KafkaUtils.createDirectStream(streamingContext, ['access-log'], kafkaConf)
messages.pprint()


streamingContext.start()
streamingContext.awaitTermination(timeout=10000)