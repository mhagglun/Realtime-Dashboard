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
import scala.util.matching
import java.time.LocalDateTime
import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector._
import com.datastax.driver.core.{Session, Cluster, Host, Metadata}
import com.datastax.spark.connector.streaming._
import com.datastax.spark.connector.writer.{TimestampOption, TTLOption, WriteConf}
import org.apache.spark.rdd.RDD

object KafkaSpark {
  
  val APACHE_ACCESS_LOG_PATTERN = """^(\S+) (\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+)\s?(\S+)?\s?(\S+)?\" (\d{3}|-) (\d+|-)\s?"?([^"]*)"?\s?"?([^"]*)?"?$""".r

  val MONTH_MAP = Map("Jan" -> 1, "Feb" -> 2, "Mar" -> 3, "Apr" -> 4, "May" -> 5, "Jun" -> 6, "Jul" -> 7, "Aug" -> 8,
                      "Sep" -> 9, "Oct" -> 10, "Nov" -> 11, "Dec" -> 12)

  val USERID_PATTERN = """userId=([^&#]*)"""
  val ITEMID_PATTERN = """itemId=([^&#]*)"""

  // Helper classes for parsing apache logs
  case class Cal(year: Int, month: Int, day: Int, hour: Int, minute: Int, second: Int)

  case class Row(countryCode: String, host: String, clientID: String, userID: String, dateTime: Cal, method: String, endpoint: String,
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
    val sizeField = r.group(10)

    var size: Long = 0
    if (sizeField != "-")
        size = sizeField.toLong

    return (Left(Row(r.group(1), r.group(2), r.group(3), r.group(4), parseApacheTime(r.group(5)), r.group(6), r.group(7),
                     r.group(8), r.group(9).toInt, size, r.group(11))), 1)
  }

  def parseQueryParam(pattern: String, endpoint: String) : String = {
    val ret = pattern.r.findAllIn(endpoint).matchData.toList
    val r = ret(0)
    val param = r.group(0).split("=")(1)

    return param
  }

  def main(args: Array[String]) {
    
    // connect to Cassandra and make a keyspace and table
    val cluster = Cluster.builder().addContactPoint("127.0.0.1").build()
    val session = cluster.connect()
    session.execute("CREATE KEYSPACE IF NOT EXISTS access_log WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };")
    session.execute("CREATE TABLE IF NOT EXISTS access_log.searches (keyword text, timestamp timestamp, count int, PRIMARY KEY (keyword, timestamp)) WITH CLUSTERING ORDER BY (timestamp DESC);")
    session.execute("CREATE TABLE IF NOT EXISTS access_log.orders (id int, timestamp timestamp, count int, PRIMARY KEY (id, timestamp));")
    session.execute("CREATE TABLE IF NOT EXISTS access_log.countries (country_code text, timestamp timestamp, count int, PRIMARY KEY (country_code, timestamp)) WITH CLUSTERING ORDER BY (timestamp DESC);")
    session.execute("CREATE TABLE IF NOT EXISTS access_log.requests (response_code int, timestamp timestamp, count int, PRIMARY KEY (response_code, timestamp)) WITH CLUSTERING ORDER BY (timestamp DESC);")

    // Need minimum of 2 threads, one for reading input and one for processing
    val sparkConf = new SparkConf().setAppName("DashboardConsumer").setMaster("local[2]")
    val streamingContext = new StreamingContext(sparkConf, Seconds(10))
    streamingContext.checkpoint(".checkpoints/")

    val kafkaConf = Map(
      "metadata.broker.list" -> "localhost:9092",
      "zookeeper.connect" -> "localhost:2181",
      "group.id" -> "kafka-spark-streaming",
      "zookeeper.connection.timeout.ms" -> "1000")

    // Create stream from topic to read data from
    val inputTopic = Set("access-log")
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](streamingContext, kafkaConf, inputTopic)
    
    val rawLogs = messages.map(x => x._2) // Received logs are on the format (null, log)
    val parsedLogs = rawLogs.map(parseApacheLogLine _)  // Parse logs
    val accessLogs = parsedLogs.filter(x => x._2 == 1).map(x => x._1.left.get) // Filter out any logs which may have not been correctly parsed
    

    // Filter out search words and orders from the access logs
    val searches = accessLogs.filter(x => x.endpoint.contains("/store/search?name="))
    val keywords = searches.map(x => (x.endpoint.split("=")(1), 1))
    val orderRequests = accessLogs.filter(x => x.endpoint.contains("/store/checkout"))
    
    // Count occurences of search words and number of orders for the time window
    val keywordsCount = keywords.reduceByKeyAndWindow((a:Int, b:Int) => (a + b) , Seconds(10), Seconds(10))
    val orderCount = orderRequests.countByWindow(Seconds(10), Seconds(10))

    // Count number of requests by response code and country code for the time window
    val countriesCount = accessLogs.map(x => (x.countryCode, 1)).reduceByKeyAndWindow( (a:Int, b:Int) => (a + b), Seconds(10), Seconds(10))
    val requestCount = accessLogs.map(x => (x.responseCode, 1)).reduceByKeyAndWindow( (a:Int, b:Int) => (a + b), Seconds(10), Seconds(10))
    
    // Set timestamps
    val timestampedRequestCount = requestCount.map(x => (x._1, LocalDateTime.now().toString, x._2))
    val timestampedCountriesCount = countriesCount.map(x => (x._1, LocalDateTime.now().toString, x._2))
    val timestampedKeywordsCount = keywordsCount.map(x => (x._1, LocalDateTime.now().toString, x._2))
    val timestampedOrdersCount = orderCount.map(x => (0, LocalDateTime.now().toString, x))

    timestampedRequestCount.saveToCassandra("access_log", "requests", writeConf = WriteConf(ttl = TTLOption.constant(3600)))
    timestampedOrdersCount.saveToCassandra("access_log", "orders", writeConf = WriteConf(ttl = TTLOption.constant(3600)))
    timestampedKeywordsCount.saveToCassandra("access_log", "searches", writeConf = WriteConf(ttl = TTLOption.constant(3600)))
    timestampedCountriesCount.saveToCassandra("access_log", "countries", writeConf = WriteConf(ttl = TTLOption.constant(3600)))

    streamingContext.start()
    streamingContext.awaitTermination()
    session.close()
  }
}
