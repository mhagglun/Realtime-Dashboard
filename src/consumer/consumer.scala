package sparkstreaming

import java.util.HashMap
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka._
import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.storage.StorageLevel
import java.util.{Date, Properties}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, ProducerConfig}
import scala.util.Random
import scala.util.matching

import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector._
import com.datastax.driver.core.{Session, Cluster, Host, Metadata}
import com.datastax.spark.connector.streaming._
import org.apache.spark.rdd.RDD

object KafkaSpark {
  
  val APACHE_ACCESS_LOG_PATTERN = """^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+)\s?(\S+)?\s?(\S+)?\" (\d{3}|-) (\d+|-)\s?"?([^"]*)"?\s?"?([^"]*)?"?$""".r

  val MONTH_MAP = Map("Jan" -> 1, "Feb" -> 2, "Mar" -> 3, "Apr" -> 4, "May" -> 5, "Jun" -> 6, "Jul" -> 7, "Aug" -> 8,
                      "Sep" -> 9, "Oct" -> 10, "Nov" -> 11, "Dec" -> 12)

  // Helper classes for parsing apache logs
  case class Cal(year: Int, month: Int, day: Int, hour: Int, minute: Int, second: Int)

  case class Row(host: String, clientID: String, userID: String, dateTime: Cal, method: String, endpoint: String,
                protocol: String, responseCode: Int, contentSize: Long, userAgent: String) 

  def parseApacheTime(s: String): Cal = {
    return Cal(s.substring(7, 11).toInt, MONTH_MAP(s.substring(3, 6)), s.substring(0, 2).toInt, 
               s.substring(12, 14).toInt, s.substring(15, 17).toInt, s.substring(18, 20).toInt)
  }

  def parseApacheLogLine(logline: String): (Either[Row, String], Int) = {
    val ret = APACHE_ACCESS_LOG_PATTERN.findAllIn(logline).matchData.toList
    if (ret.isEmpty)
        return (Right(logline), 0)

    val r = ret(0)
    val sizeField = r.group(9)

    var size: Long = 0
    if (sizeField != "-")
        size = sizeField.toLong

    return (Left(Row(r.group(1), r.group(2), r.group(3), parseApacheTime(r.group(4)), r.group(5), r.group(6),
                     r.group(7), r.group(8).toInt, size, r.group(10))), 1)
  }

  def main(args: Array[String]) {
    
    // connect to Cassandra and make a keyspace and table
    val cluster = Cluster.builder().addContactPoint("127.0.0.1").build()
    val session = cluster.connect()
    session.execute("CREATE KEYSPACE IF NOT EXISTS access_log WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };")
    session.execute("CREATE TABLE IF NOT EXISTS access_log.searches (keyword text PRIMARY KEY, count float);")
    session.execute("CREATE TABLE IF NOT EXISTS access_log.orders (word text PRIMARY KEY, count float);")

    // Need minimum of 2 threads, one for reading input and one for processing
    val sparkConf = new SparkConf().setAppName("KafkaSparkWordCount").setMaster("local[2]")
    val streamingContext = new StreamingContext(sparkConf, Seconds(5))
    streamingContext.checkpoint(".checkpoints/")

    val kafkaConf = Map(
      "metadata.broker.list" -> "localhost:9092",
      "zookeeper.connect" -> "localhost:2181",
      "group.id" -> "kafka-spark-streaming",
      "zookeeper.connection.timeout.ms" -> "1000")
    
    
    val topic = Set("access-log")
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](streamingContext, kafkaConf, topic)
    
    val rawLogs = messages.map(x => x._2) // Received logs are on the format (null, log)
    val parsedLogs = rawLogs.map(parseApacheLogLine _)  // Parse logs
    val accessLogs = parsedLogs.filter(x => x._2 == 1).map(x => x._1.left.get) // Filter out any logs which may have not been correctly parsed
    

    val searches = accessLogs.filter(x => x.endpoint.contains("/store/search?name="))
    val keywords = searches.map(x => (x.endpoint.split("=")(1), 1))
    

    // measure the number of orders for each key in a stateful manner
    def mapSearches(key: String, value: Option[Int], state: State[Int]): (String, Int) = {
      if (state.exists) {
        val oldState = state.get()
        val newState = (oldState + value.get)
        state.update(newState)
        return (key, newState)
      } else {
        val newState = value.get
        state.update(newState)
        return (key, newState)
      }
    }

    val stateDstream = keywords.mapWithState(StateSpec.function(mapSearches _))
    stateDstream.saveToCassandra("access_log", "searches", SomeColumns("keyword", "count"))

    streamingContext.start()
    streamingContext.awaitTermination()
    session.close()
  }
}
