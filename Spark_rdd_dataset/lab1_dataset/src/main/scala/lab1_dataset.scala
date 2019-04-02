import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType, TimestampType, _}
import org.apache.spark.sql.{SparkSession, _}
import org.apache.log4j.{Level, Logger}
import java.sql.Timestamp
import org.apache.spark.sql.functions._



object Project {
  case class GDeltData ( id: String, timestamp: Timestamp, sourceId: Long, sourceName: String, 
    docId: String, counts: String, V2Counts: String, theme: String, V2Themes: String, 
    location: String, V2Locations: String, persons: String, V2Persons: String, org: String, 
    V2Org: String, V2Tone: String, dates: String, gcam: String, sharingImg: String, 
    relatedImg: String, socialImg: String, socialVideo: String, quotations: String, 
    allNames: String, amounts: String, translationInfo: String, extras: String)


  def main(args: Array[String]) {

    // Defining the Schema of the CSV to be read.
    val schema = StructType(Array( StructField("id", StringType, nullable=true), StructField("timestamp", TimestampType, nullable=true), StructField("sourceId", LongType, nullable=true), StructField("sourceName", StringType, nullable=true), StructField("docId", StringType, nullable=true), StructField("counts", StringType, nullable=true), StructField("V2Counts", StringType, nullable=true), StructField("theme", StringType, nullable=true), StructField("V2Themes", StringType, nullable=true), StructField("location", StringType, nullable=true), StructField("V2Locations", StringType, nullable=true), StructField("persons", StringType, nullable=true), StructField("V2Persons", StringType, nullable=true), StructField("org", StringType, nullable=true), StructField("V2Org", StringType, nullable=true), StructField("V2Tone", StringType, nullable=true), StructField("dates", StringType, nullable=true), StructField("gcam", StringType, nullable=true), StructField("sharingImg", StringType, nullable=true), StructField("relatedImg", StringType, nullable=true), StructField("socialImg", StringType, nullable=true), StructField("socialVideo", StringType, nullable=true), StructField("quotations", StringType, nullable=true), StructField("allNames", StringType, nullable=true), StructField("amounts", StringType, nullable=true), StructField("translationInfo", StringType, nullable=true), StructField("extras", StringType, nullable=true)))
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    // Initializing Spark Session.
    val spark = SparkSession
      .builder
      .appName("GDELThist")
      .config("spark.master", "local")
      .getOrCreate()


    import spark.implicits._

    // Defining path to access CSVs. Assuming all CSVs are inside this folder.
    val pathToFile = "src/main/resources/"

    // CSV Read and init
    val ds = spark.read
      .schema(schema)
      .option("timestampFormat", "YYYYMMDDhhmmss")  // Parsing timestamp from the CSV
      .option("delimiter", "\t")  // Data points in the CSV are seperated by Tabs 
      .csv(pathToFile + "*.csv")  // Select all CSV files present in the resources folder
      .as[GDeltData]  // Naming the table
      .select("timestamp", "allNames")  // Selecting the columns of interest. 
      .filter("allNames is not null") // Filtering null values from the allNames column
      .filter ("timestamp is not null") // Filtering null values from the timestamp column


    // Function to "process" the allNames fields.
    val flatten = udf((inp: Seq[Seq[String]]) => inp.flatten
                                                    
                                                    // Filtering values error values
                                                    .filter(x => ((x != "Type ParentCategory") && (x != "CategoryType ParentCategory")) )
                                                    
                                                    // Translating all topics to tuple of format (topic, 1)
                                                    .map{case x => (x, 1)}

                                                    // Grouping by topic
                                                    .groupBy(y => y._1)

                                                    // Parsing each topic into format (topic, frequency in array)
                                                    .map {case (x, y) => (x -> y.size)}
                                                    
                                                    // Parsing into an array to sort and return 10 most frequent words
                                                    .toArray

                                                    // Sorting by Frequency. Default sorted in ascending order
                                                    .sortBy(x => x._2)

                                                    // Reversing the sorted array in descending order
                                                    .reverse
                                                    .take (10)
                      )


    // Accumulating the final output in d
    val d = ds.map (p => (
                              // Splling the timestamp to extract only dates
                              p(0).toString.split(" ")(0),
                              // Splitting the topics present in one entry
                              p(1).toString.split(";").map(_.split(",")(0))
                            )
                      )

                  // Grouping results in terms of date
                  .groupBy("_1")

                  // Aggrigating with processed tuples of the final result. 
                  .agg(flatten(collect_list("_2")) as ("Topics"))


   // Show the final result
    d.show ()

    // Stopping the spark context
    spark.stop
  }
}
