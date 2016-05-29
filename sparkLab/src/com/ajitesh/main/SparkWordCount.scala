package com.ajitesh.main

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark._
import java.time.LocalTime

object SparkWordCount {

  def main(args: Array[String]) {
    val sc = new SparkContext("local", "Word Count", "/usr/local/spark-1.6.1-bin-hadoop2.6", Nil,
      Map(), Map())
    /* local = master URL; Word Count = application name; */
    /* /usr/local/spark = Spark Home; Nil = jars; Map = environment */
    /* Map = variables to work nodes */
    /*creating an inputRDD to read text file (in.txt) through Spark context*/
    val input = sc.textFile("/home/jitu/SCALAWORKS/SPARKWORKS/sparkWordCount.txt")

    /* Transform the inputRDD into countRDD */
    val count = input.flatMap(line => line.split(" "))
                  .map(word => (word, 1))
                  .reduceByKey(_ + _)
    /* saveAsTextFile method is an action that effects on the RDD */
    count.saveAsTextFile("/home/jitu/SCALAWORKS/SPARKWORKS/outfile"+ LocalTime.now())
    System.out.println("OK");
   // sc.stop();
    while(true){
      /**
       *  infinite loop to see task status on browser http://localhost:4040 ....
       */
    }
  }

}