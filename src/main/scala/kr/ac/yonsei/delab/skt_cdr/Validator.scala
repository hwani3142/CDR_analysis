package kr.ac.yonsei.delab.skt_cdr

import org.apache.spark.SparkContext

import scala.collection.mutable.ListBuffer
import org.apache.spark.rdd.RDD

object Validator {
  // Ret: (correctResult:RDD[Record], incorrectResult:RDD[Record])
  def validate (records: RDD[Record], sc: SparkContext) : Unit = {
    val correctAcuum = sc.longAccumulator("Correct Accumulator")
    val incorrectAcuum = sc.longAccumulator("Incorrect Accumulator")
    var (correct, incorrect) = (0, 0)
    records.foreachPartition {
      partition => // partition:Iterator[RDD[Record]]
        partition.foreach {
          record =>
            var correctOrIncorrect = true
            // 1) For integer constraints
            FieldConstraint.IntConstraintList.foreach {
              field =>
//                println("## IntDebug + "+field + ", \"" + record.getFieldData(field)+"\"")
                val data = record.getFieldData(field).trim
                // Permit empty string for constraint
                if (Constant.EXCEPT_FOR_EMPTY_STRING && data.isEmpty) {
                  // INFO logging
                }
                else if (data.isEmpty) {
//                  throw new NumberFormatException("[ERROR][SKT-CDR] Constraint has empty value.\n " +
//                    "If you ignore this exception, set ExceptForEmptyString to true")
                  record.setIncorrectRecord(field)
                  correctOrIncorrect = false
                }
                else {
                  if (!(FieldConstraint.getContstraint(field).contains(data.toInt))) {
                    record.setIncorrectRecord(field)
                    correctOrIncorrect = false
                  }
                }

            }
            // 2) For character constraints
            FieldConstraint.CharConstraintList.foreach {
              field =>
//                if (field == "ACR_START_LOST" || field == "ACR_STOP_LOST")
//                  println("## CharDebug + "+field+ ", \"" + record.getFieldData(field)+"\"")
                val data = record.getFieldData(field).trim
                if (Constant.EXCEPT_FOR_EMPTY_STRING && data.isEmpty) {
                  // INFO logging
                }
//                else if (data.isEmpty) {
//                  throw new NumberFormatException("[ERROR][SKT-CDR] Constraint has empty value.\n " +
//                    "If you ignore this exception, set ExceptForEmptyString to true")
//                  record.setIncorrectRecord(field)
//                  correctOrIncorrect = false
//                }
                else {
                  if (!(FieldConstraint.getContstraint(field).contains(data))) {
                    record.setIncorrectRecord(field)
                    correctOrIncorrect = false
                  }
                }
            }
            if (correctOrIncorrect) {
              correctAcuum.add(1)
              correct += 1
              // SparkSQL [INSERT]
            } else {
              incorrectAcuum.add(1)
              incorrect += 1
            }
        }
        println("[Result] correct: "+ correct+", incorrect: "+ incorrect)
    }
    println("[Total Result] correct: "+ correctAcuum.value + ", incorrect: "+ incorrectAcuum.value)
  }
}
object FieldConstraint {

  // 9 constraints
  val IntConstraintList = List("SERVER", "SERVICE_TYPE", "SUPP_SERVICE", "ACCOUNTING_RECORD_TYPE",
    "ROLE_OF_NODE", "CAUSE_FOR_REC_CLOSING", "ACR_INTERIM_LOST", "CHARGING_INDICATOR", "RAT_TYPE"
  )
  // 7 constraints
  val CharConstraintList = List("ACR_START_LOST", "ACR_STOP_LOST", "CONNECTION_TYPE", "PPS_TYPE",
    "INTERNATIONAL_ROAMING", "ORIG_SERVICE_DOMAIN_CODE", "TERM_SERVICE_DOMAIN_CODE"
  )

  // 16 constrainst
  val Constraints = Map(
    // 2 [Int]
    "SERVER" -> List(11, 12, 13, 14, 15, 16, 17, 18, 19),
    // 7 [Int]
    "SERVICE_TYPE" -> List(101, 102, 103,
    /* 201, 202, */
      203, 205, 207,
      301, 302, 303, 304, 305, 306, 307, 309, 310,
      401, 801),
    // 9 [Int]
    "SUPP_SERVICE" -> List(0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
      10, 11,
      90, 91),
    // 10 [int]
    "ACCOUNTING_RECORD_TYPE" -> List(1, 5),
    // 18 [int]
    "ROLE_OF_NODE" -> List(1, 2, 5),
    // 28
    // 29
    // 30 [int]
    "CAUSE_FOR_REC_CLOSING" -> List(0, 1, 2, 3, 4, 5, 6),
    // 31, 32 [char]
    // start-lost, stop-lost
    "ACR_START_LOST" -> List("T", "F"),
    "ACR_STOP_LOST" -> List("T", "F"),
    // 33 [int]
    "ACR_INTERIM_LOST" -> List(0, 1, 2),
    // 34
    // 35
    // 36
    // 37 [char]
    "CONNECTION_TYPE" -> List("01", "02", "03", "00"),
    // 38 [char]
    "PPS_TYPE" -> List("0", // Add 0 because many data has 0
      "00", "01", "02", "03", "04", "05", "06", "07", "08", "09",
      "11", "12", "13", "14", "15", "16", "17", "18", "19",
      "20"/*, "21"*/, "22", "23", "24", "25", "26", "27"/*, "28"*/, "29",
      "30", "31", "32", "33", "34"/*, "35", "36", "37"*/, "38", "39",
      "40", "41", "42", "43", "44", "45", "46", "47", "48",
      "50", "51", "52", "53", "54", "55", "56", "57", "58", "59",
      "60", "61",
      "97", "98", "99"),
    // 39 [char]
    "INTERNATIONAL_ROAMING" -> List("000", "001"),
    // 44 [char]
    "ORIG_SERVICE_DOMAIN_CODE" -> List("011", "016", "019", "774", "770", "502", "900", "901", "999"),
    // 45 [char]
    "TERM_SERVICE_DOMAIN_CODE" -> List("011", "016", "019", "111", "901", "999", "502",
      "203", "204", "205", "206", "207", "209", "208", "210", "211",
      "220", "221", "222", "223", "224", "225", "226", "227"),
    // 46 [int]
    "CHARGING_INDICATOR" -> List(0, 1, 2),
    // 47 [int]
    "RAT_TYPE" -> List(1, 2, 3)
  )
  def getContstraint(fieldName:String): List[Any] = {
    if (Constraints.get(fieldName) == None)
      throw new IllegalArgumentException("[ERROR][SKT-CDR] No such field name("+fieldName+") in constraints list..")
    Constraints(fieldName)
  }
}
