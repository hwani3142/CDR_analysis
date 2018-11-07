package kr.ac.yonsei.delab.skt_cdr

import org.apache.hadoop.mapred.InvalidInputException
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer

object main {
  def main (args:Array[String]) = {
    println("Start application")
//    val cdrFile="sample3.txt"
    val conf = new SparkConf().setAppName("Build configuration").setMaster("local")
    val sc = new SparkContext(conf)

    val startTime = System.currentTimeMillis()

    Constant.CDR_FILE_LIST.foreach {
      fileName =>
        try {
          println()
          println("# Start "+fileName)

          // 0) Open file
          val file = sc.textFile(fileName, Constant.NUM_OF_PARTITION)

          // 1) Parse
          print("## Phase1) Start parsing ")
          val startParseTime = System.currentTimeMillis()
          val parsedResult:RDD[Record] = Parser.parse(file, sc)
          val endParseTime = System.currentTimeMillis()
          println("-- Done. [" + (endParseTime - startParseTime)+ "ms]")

          // 2) Validate
          print("## Phase2) Start validating ")
          val startValidateTime = System.currentTimeMillis()
          Validator.validate(parsedResult, sc)
          val endValidateTime = System.currentTimeMillis()
          println("## Phase2) Start validating -- Done. ["+ (endValidateTime - startValidateTime)  + "ms]")

          // 3) Statistics
//          println("## Phase3) Get statistics ")
//          val statistics = Statistics(fileName, parsedResult)
//          statistics.getStatistics
        } catch {
          case e : InvalidInputException => e.printStackTrace
        }
    }
    val endTime = System.currentTimeMillis()
    println("# End "+ ". [Total time: "+ (endTime - startTime) + "ms]")
    sc.stop()
    println()
    println("End application")
  }
}
object Constant {
  val DEFAILT_RECORD_LENGTH = 450
  val NUM_OF_PARTITION = 8

  // Set by user
//  val CDR_FILE_LIST = {
//    val res = ListBuffer[String]()
//    val prefix = "A_"
//    val directories = List("HD_VOICE1", "HD_VOICE2", "HD_VOICE3", "HD_VOICE4")
//    val range = List((101, 130), (131, 164), (165, 180), (181, 199))
//    for (i <- 0 until directories.size ) {
//      val list = List.range(range(i)._1, range(i)._2)
//      list.foreach( number => res += (directories(i)+"/"+prefix + number.toString ) )
//    }
//    res.toList
//  }
  val CDR_FILE_LIST = List("HD_VOICE1", "HD_VOICE2", "HD_VOICE3", "HD_VOICE4")
//  val CDR_FILE_LIST = List("A_165", "sample3.txt")
  val EXCEPT_FOR_EMPTY_STRING = false
//  val EXCEPT_FOR_EMPTY_STRING = true

}
