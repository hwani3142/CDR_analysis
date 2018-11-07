package kr.ac.yonsei.delab.skt_cdr

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import scala.collection.mutable.ListBuffer

// Parse & Make array
//object Parser {
//  def parse (input: String) : List[Record] = {
//    val result = ListBuffer[Record]()
//    def parseLength(subString: String) : Int = {
//        subString.substring(0, 4).trim.toInt
//    }
//    def parseRow(subString:String) : Unit = {
//      val size = subString.size
//      if(size != 0) {
//        val length = parseLength(subString)
//        if (length > subString.size) {
//          // Raw data loss..
//          // Throw exception
//        } else {
////          println("length: "+length)
//          result += Record(subString.substring(0, parseLength(subString)))
//          parseRow(subString.substring(length, size))
//        }
//      }
//    }
//    parseRow(input)
//    println("ListBuffer size: "+result.size)
//    result.toList
//  }
//}

object Parser {
  def parse(data:RDD[String], sc:SparkContext) :RDD[Record]  = {
    var lengthErrorFlag = false
//    val group = data.first().grouped(Constant.DEFAILT_RECORD_LENGTH).map{
//      x:String =>
////        if (x.substring(0, 4).trim.toInt != Constant.DEFAILT_RECORD_LENGTH) lengthErrorFlag = true
//        Record(x)
//    }.toArray
    val group = data.flatMap(x => x.grouped(Constant.DEFAILT_RECORD_LENGTH)).map(x => Record(x))

    // TO DO, Check whether length of one CDR data is 450
    if (lengthErrorFlag)
      throw new Exception ("[ERROR][SKT-CDR] There is CDR that more or less than 450 character")
//    sc.parallelize(group, Constant.NUM_OF_PARTITION)
    group
  }
}