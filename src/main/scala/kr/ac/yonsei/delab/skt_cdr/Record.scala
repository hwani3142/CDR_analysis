package kr.ac.yonsei.delab.skt_cdr

import scala.collection.immutable.ListMap

case class Record (/*size: Int,*/ data: String) extends Serializable {
  lazy val fieldData:Map[String, String] = Field.divideRawData(data)
  var correctRecord = true
  var incorrectFieldName = ""
  def getRecordSize() = {
    fieldData("LENGTH").trim.toInt
  }
  def getFieldData(fieldName:String) :String = {
    if (fieldData.get(fieldName) == None) {
      throw new IllegalArgumentException("[ERROR][SKT-CDR] No such field name(" + fieldName + ") in record data..")
    }
    fieldData(fieldName)
  }
  def setIncorrectRecord (incorrectFieldName:String) = {
    correctRecord = false
    this.incorrectFieldName = incorrectFieldName
  }
}
object Field {
  val fieldLength: ListMap[String, Int] = ListMap(
  "LENGTH" -> 4,"SERVER" -> 2, "SERVER_SYSTEM_NO" -> 2, "SERVICE_TYPE" -> 3, "SUB_SERVICE_TYPE" -> 5,
  "SUPP_SERVICE" -> 2, "ACCOUNTING_RECORD_TYPE" -> 1, "SERVICE_ID" -> 46, "NUMBER_OF_PARTICIPANT" -> 10, "CALLING_PARTY_ADDRESS" -> 16,
  "CALLED_PARTY_ADDRESS" -> 24, "CHARGING_PARTY_ADDRESS" -> 24, "DIALED_NUMBER_ADDRESS" -> 32, "CALL_FORWARD_ADDRESS" -> 24, "ROLE_OF_NODE" -> 1,
  "START_TIME" -> 15, "END_TIME" -> 15, "DELIVERY_TIIME" -> 15, "CHARGING_DURATION" -> 10, "SIP_BODY_SIZE_UPLINK" -> 10,
  "SIP_BODY_SIZE_DOWNLINK" -> 10, "CONTENT_SIZE_UPLINK" -> 10, "CONTENT_SIZE_DOWNLINK" -> 10, "RECEIVER_COUNT" -> 5, "SIP_CODE" -> 3,
  "DETAIL_CODE" -> 4, "CAUSE_FOR_REC_CLOSING" -> 3, "ACR_START_LOST" -> 1, "ACR_STOP_LOST" -> 1, "ACR_INTERIM_LOST" -> 1,
  "TERMINAL_IP_ADDRESS" -> 15, "TERMINAL_TYPE" -> 10, "TERMINAL_MODEL" -> 16, "CONNECTION_TYPE" -> 2, "PPS_TYPE" -> 5,
  "INTERNATIONAL_ROAMING" -> 3, "P2P_MESSAGE_COUNT" -> 5, "P2W_MESSAGE_COUNT" -> 5, "W2P_MESSAGE_COUNT" -> 5, "W2W_MESSAGE_COUNT" -> 5,
  "ORIG_SERVICE_DOMAIN_CODE" -> 3, "TERM_SERVICE_DOMAIN_CODE" -> 3, "CHARGING_INDICATOR" -> 1, "RAT_TYPE" -> 1, "CELL_ID" -> 7,
  "PCS_SFI" -> 6, "SGSN_MCC_MNC" -> 6 ,"RESERVED" -> 43
  )
  var startIndex = Map[String, Int]()
  def calculateStartIndex(fieldName: String): Int = {
    var res = 0
    Field.fieldLength.foreach {
      var flag = false // If flag is true, do not accumulate length
      nameLengthPair =>
        if (!flag) {
          if (nameLengthPair._1 == fieldName) flag = true
          else res += nameLengthPair._2
        }
    }
    res
  }
  def getFieldData(data:String, fieldName: String):String = {
    if (startIndex.get(fieldName) == None) {
      startIndex += (fieldName -> calculateStartIndex(fieldName))
    }
    data.substring(startIndex(fieldName),  startIndex(fieldName)+ Field.fieldLength(fieldName)) // ((startIndex) ~ (startIndex + length))
  }
  def divideRawData(data:String): Map[String, String] = {
    var res = Map[String, String]()
    Field.fieldLength.foreach {
      var currentIndex = 0
      nameLengthPair =>
        if(currentIndex != Constant.DEFAILT_RECORD_LENGTH) {
//          println(nameLengthPair._1)
          res += (nameLengthPair._1 -> data.substring(currentIndex, currentIndex + nameLengthPair._2))
        }
        currentIndex += nameLengthPair._2
    }
    res
  }
}