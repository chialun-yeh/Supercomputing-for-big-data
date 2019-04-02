package lab3

import java.util.{Properties, Calendar, TimeZone}
import java.util.concurrent.TimeUnit
import java.text.{DateFormat, SimpleDateFormat};

import org.apache.kafka.streams.kstream.{Transformer}
import org.apache.kafka.streams.processor._
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.state._
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig, KeyValue, Topology}

import scala.collection.JavaConversions._


object GDELTStream extends App {
  import Serdes._

  val props: Properties = {
    val p = new Properties()
    p.put(StreamsConfig.APPLICATION_ID_CONFIG, "gdelt-stream")
    p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    p
  }

  val builder: StreamsBuilder = new StreamsBuilder
  val topicStoreBuilder = Stores.keyValueStoreBuilder(
    Stores.inMemoryKeyValueStore("topic_count"),
    Serdes.String,
    Serdes.Long
  )
  .withLoggingDisabled

  val timeStoreBuilder = Stores.keyValueStoreBuilder(
    Stores.inMemoryKeyValueStore("time_topic"),
    Serdes.String,
    Serdes.String
  )
  .withLoggingDisabled

  builder.addStateStore(topicStoreBuilder)
  builder.addStateStore(timeStoreBuilder)

  // Filter this stream to a stream of (key, name). This is similar to Lab 1,
  // only without dates! After this apply the HistogramTransformer. Finally, 
  // write the result to a new topic called gdelt-histogram. 
  val records: KStream[String, String] = builder.stream[String, String]("gdelt")
  val filtered_record: KStream[String, String] = records.mapValues(data => data.split("\t"))
                                                        .filter( (k,v) => (v.size > 23) )
                                                        .filter( (k,v) => !v(23).isEmpty )
                                                        .flatMapValues( data => data(23).split(";").map(_.split(",")(0)).filter(x => !x.contains("ParentCategory"))) 
  val publishable_count: KStream[String, Long] = filtered_record.transform(new HistogramTransformer, "topic_count", "time_topic")
  publishable_count.to("gdelt-histogram")

  val streams: KafkaStreams = new KafkaStreams(builder.build(), props)
  streams.cleanUp()
  streams.start()

  sys.ShutdownHookThread {
    println("Closing streams.")
    streams.close(10, TimeUnit.SECONDS)
  }

  System.in.read()
  System.exit(0)
}

// This transformer should count the number of times a name occurs 
// during the last hour. This means it needs to be able to 
//  1. Add a new record to the histogram and initialize its count;
//  2. Change the count for a record if it occurs again; and
//  3. Decrement the count of a record an hour later.
// You should implement the Histogram using a StateStore (see manual)
class HistogramTransformer extends Transformer[String, String, (String, Long)] {
  var context: ProcessorContext = _
  var count_state: KeyValueStore[String, Long] = _
  var time_state: KeyValueStore[String, String] = _

  // Initialize Transformer object
  def init(context: ProcessorContext) {
    this.context = context
    this.count_state = context.getStateStore("topic_count").asInstanceOf[KeyValueStore[String, Long]]
    this.time_state = context.getStateStore("time_topic").asInstanceOf[KeyValueStore[String, String]]

    // schedule update per second
    context.schedule(1000, PunctuationType.WALL_CLOCK_TIME, timestamp => {
      val timeFormat = new SimpleDateFormat("yyyyMMddHHmmss")
      timeFormat.setTimeZone(TimeZone.getTimeZone("Etc/UTC"));
      val time = timeFormat.format(timestamp);
      val cal = Calendar.getInstance;
      cal.setTime(timeFormat.parse(time));
      cal.add(Calendar.HOUR, -1);
      val beforeAnHour = cal.getTime()

      // check all the entries
      val iter = this.time_state.all
      while (iter.hasNext) {
        val entry = iter.next()
        val entryTime = timeFormat.parse(entry.key.split("-")(0))
        if (entryTime.before(beforeAnHour)) {
          val topics = entry.value.split(",")
          for (t <- topics){
            val c = this.count_state.get(t)
            if (c == 1L)
              this.count_state.delete(t)
            else 
              this.count_state.put(t, c-1L)
          }

          this.time_state.delete(entry.key)
        }
      }
       iter.close
       context.commit
    })
  }

  // Should return the current count of the name during the _last_ hour
  def transform(key: String, name: String): (String, Long) = {

    val existTopic = Option(this.count_state.get(name))
    val existTime = Option(this.time_state.get(key))
    var count = 0L
    var topics = ""
    
    // update topic-count store  
    if (existTopic == None){
      count = 1L
    }
    else{
      val c = this.count_state.get(name)
      count = c + 1L
    }
    this.count_state.put(name, count)

    // update time-topic store
    if(existTime == None){
      topics = name
    }
    else{
      topics = this.time_state.get(key) + "," + name
    }
    this.time_state.put(key, topics)

    context.commit
    (name, count)
  }
  
  // Close any resources if any
  def close() {
  }
}
