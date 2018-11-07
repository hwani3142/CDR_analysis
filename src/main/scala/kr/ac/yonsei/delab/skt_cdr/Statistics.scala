package kr.ac.yonsei.delab.skt_cdr

import org.apache.spark.rdd.RDD

case class Statistics (fileName:String, records:RDD[Record]) {
  def getStatistics() = {
    val incorrectResult = records.filter((record => record.correctRecord == false))
    val incorrectResultSize = incorrectResult.count
    println("## "+fileName+" statistics: ")
    println("###  1) The number of correct CDR: "+ (records.count - incorrectResultSize))
    println("###  2) The number of incorrect CDR: " + incorrectResultSize)
    if (incorrectResultSize != 0){
      println("###  2-1) Fields: ")
      incorrectResult.foreach(record => println("###    "+record.incorrectFieldName))
    }
  }
}
