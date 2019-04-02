
// Created on Sep 23, 2018
// An RDD implementation of the word count of the GDelt Dataset

package lab1_rdd

import org.apache.spark.sql._
import org.apache.log4j.{Level, Logger}

object GDeltAnalysis {
 
  def main(args: Array[String]) {
    
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    val spark = SparkSession
      .builder
      .appName("GDELThist")
      .config("spark.master", "local")
      .getOrCreate()
      
    spark.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = spark.sparkContext

    val t0 = System.currentTimeMillis
    // directory path
    val pathToFile = "C:/Users/sharo/Desktop/SBD/SBD-2018/data/segment/"

    // read in all the files into RDD, parse files and filter incomplete data
    val rdd_filter = sc.textFile(pathToFile)
                       .map(_.split("\t"))
                       .filter(_.size > 23)
                       .filter( p => !p(23).isEmpty)

    // extract date and words and count words
    val rdd_topics = rdd_filter.flatMap( p => {
      val date = p(1).substring(0,8)  // discard time and use date only
      for  ( word <- (p(23).split("[,;]").filter(x => !(x forall Character.isDigit) && !x.contains("Category"))) )
      yield ((date, word) ,1) } ) . reduceByKey(_+_)

    // group by date
    val topic_by_date = rdd_topics.map( { case((d,w),c) => (d, (w,c)) } ).groupByKey()

    // sort the counts and extract the top 10 topics
    val top10_topics = topic_by_date.map( x => (x._1, x._2.toList.sortWith(_._2 > _._2).take(10) ))
    
    top10_topics.foreach(println)
    
    val t = (System.currentTimeMillis - t0)
    System.err.println("Time taken for RDD = %d ms".format(t))

    spark.stop
  }
}