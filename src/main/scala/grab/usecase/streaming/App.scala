package grab.usecase.streaming

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.ConsumerStrategies._
import org.apache.spark.streaming.kafka010.LocationStrategies._

/**
 * Hello world!
 *
 */
object App  {


  def main(args: Array[String]): Unit = {

    println("Spark Streaming Context Initiailsation")
    val appName = "SparkStreamingTest"
    val conf = new SparkConf().setAppName(appName).setMaster("yarn")
    val ssc = new StreamingContext(conf, Seconds(180))


    println("kafka Params Set")
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "ec2-3-93-79-187.compute-1.amazonaws.com:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "test_spark_streaming",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (true: java.lang.Boolean)
    )

    val topics = Array("testTopic","driverData")

    println("Stream Reading")
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    println("Stream Printing")
    stream.map(x => x.value()).foreachRDD(x => x.foreach(println))

    println("Stream Start & AwaitTermination")
    ssc.start()
    ssc.awaitTermination()
  }
}
