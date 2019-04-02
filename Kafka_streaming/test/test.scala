package example

import java.util.Properties
import java.util.concurrent.TimeUnit

import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}

// brings into scope the implicit conversions between the Scala and Java classes.
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream._


object WordCountScalaExample extends App {

  import Serdes._

  val config: Properties = {
    val p = new Properties()
    p.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-scala-application")
    p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    //handled through implicit SerDes in Scala APIs
    p
  }

  val builder: StreamsBuilder = new StreamsBuilder()
  val textLines: Kstream[String, String] = builder.stream[String, String]("input-topic")
  val wordCounts: KTable[String, Long] = textLines.flatMapValues(text => text.tolowerCase.split("\\W+"))
                                                  .groupBy( (_, word) => word)
                                                  .count(Materialized.as("count-store"))

  wordCounts.toStream.to("output-topic")
  val streams: KafkaStreams = new KafkaStreams(builder.build(), config)

  // Always (and unconditionally) clean local state prior to starting the processing topology.
  // We opt for this unconditional call here because this will make it easier for you to play around with the example
  // when resetting the application for doing a re-run (via the Application Reset Tool,
  // http://docs.confluent.io/current/streams/developer-guide.html#application-reset-tool).
  //
  // The drawback of cleaning up local state prior is that your app must rebuilt its local state from scratch, which
  // will take time and will require reading all the state-relevant data from the Kafka cluster over the network.
  // Thus in a production scenario you typically do not want to clean up always as we do here but rather only when it
  // is truly needed, i.e., only under certain conditions (e.g., the presence of a command line flag for your app).
  // See `ApplicationResetExample.java` for a production-like example.
  streams.cleanUp()

  streams.start()

  // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
  sys.ShutdownHookThread {
    streams.close(10, TimeUnit.SECONDS)
  }

}
