import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import twitter4j.conf.ConfigurationBuilder
import twitter4j.auth.OAuthAuthorization
import twitter4j.Status
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming._

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}


val appName = "TwitterData"
val ssc = new StreamingContext(sc, Seconds(10)) 


val consumerKey = "" 
val consumerSecret = ""
val accessToken ="" 
val accessTokenSecret = ""
val topic = "llamada"


val cb = new ConfigurationBuilder
cb.setDebugEnabled(true).setOAuthConsumerKey(consumerKey).setOAuthConsumerSecret(consumerSecret).setOAuthAccessToken(accessToken).setOAuthAccessTokenSecret(accessTokenSecret)

val auth = new OAuthAuthorization(cb.build)
val tweets = TwitterUtils.createStream(ssc, Some(auth)) 
val englishTweets = tweets.filter(_.getLang() == "en") 

val statuses = englishTweets.map(status => (status.getText(),status.getUser.getName(),status.getUser.getScreenName(),status.getCreatedAt.toString))


statuses.foreachRDD { (rdd, time) =>

rdd.foreachPartition { partitionIter =>
val props = new Properties()
val bootstrap = "" //Conexión del cluster Kafka -- ip publica y el puerto, example: 10.0.0.1:9092
props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
props.put("bootstrap.servers", bootstrap)
val producer = new KafkaProducer[String, String](props)
partitionIter.foreach { elem =>
val dat = elem.toString()
val data = new ProducerRecord[String, String]("llamada", null, dat)
  producer.send(data)
}
    producer.flush()
    producer.close()
}
}
ssc.start()
ssc.awaitTermination()