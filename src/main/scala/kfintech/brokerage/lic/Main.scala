package kfintech.brokerage.lic

import kfintech.brokerage.lic.FYTReplication
import kfintech.brokerage.lic.LTTReplication
import kfintech.brokerage.lic.BrmsRate
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.sql.functions.{coalesce, col, lit, to_date}

object Main {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("AxisVariation")
      .config("spark.master", "local")
      .getOrCreate()

    Logger.getLogger("org").setLevel(Level.OFF)
    val log: Logger = org.apache.log4j.LogManager.getLogger("Spark ETL Log")
    log.setLevel(Level.INFO)

    var input = Seq(
      ("108", "SIN" , "aa", "2", "2016/01/01", "2018/10/22", "B-15", "R", "FYT","2018/10/02","SIN","Existing Folio SIP Hello","SIN"," ","-2","null"),
      ("108", "ASDS", "aa", "2", "2016/02/01", "2018/11/22", "B-30", "R", "FYT","2018/07/24","STPA","New Purchase SIP","STPA"," ","-3","null"),
      ("110", "STPA", "ll", "3", "2015/01/01", "2018/10/22", "B-15", "A", "LTT","2017/04/22","STPI","New Purchase SIP","SIN"," ","-5","null"),
      ("109", "STIP", "lm", "3", "2014/01/01", "2018/10/22", "B-15", "B", "LTT","2016/05/22","NEW","New Purchase SIP","STPA"," ","-3","null"),
      ("109", "NOP" , "bb", "2", "2016/03/01", "2019/03/22", "B-15", "R", "FYT","2019/10/24","SIN","Existing Folio SIP Hello","SIN"," ","-2","aaaa"),
      ("108", "AA"  , "tm", "2", "2016/01/23", "2020/07/22", "B-15", "R", "FYT","2020/10/27","SIN","Existing Folio SIP Hello","STPA"," ","-2","null"),
      ("109", "NOP" , "kj", "2", "2015/02/21", "2018/10/22", "B-30", "B", "FYT","2018/12/28","STPA","Existing Folio SIP Hello","STPA"," ","-2","null"),
      ("107", "SIN" , "aa", "2", "2017/02/01", "2018/12/22", "B-15", "R", "FYT","2018/10/29","NEW","Existing Folio SIP Hello","SIN"," ","-3","dddd"),
      ("108", "SIN" , "aa", "2", "2016/01/01", "2018/10/22", "B-15", "R", "FYT","2018/10/22","STPI","Existing Folio SIP Hello","SIN"," ","-2","ffff"),
      ("108", "SIN" , "aa", "2", "2016/01/01", "2018/10/17", "B-30", "A", "FYT","2018/10/22","STPI","Existing Folio SIP Hello","STPA"," ","-4","null"),
      ("108", "SIN" , "aa", "2", "2016/01/01", "2017/10/22", "B-15", "B", "FYT","2018/10/22","SIN","New Purchase SIP","STPA"," ","-1","null"),
      ("108", "SIN" , "aa", "2", "2016/01/01", "2014/11/22", "B-30", "B", "FYT","2018/10/22","NEW","New Purchase SIP","STPA"," ","-2","null"),
      ("107", "SIN" , "aa", "2", "2016/01/01", "2020/10/22", "B-15", "R", "FYT","2018/10/22","STPA","New Purchase SIP","SIN"," ","-3","null"),
      ("108", "SIN" , "aa", "2", "2016/01/01", "2021/10/24", "B-30", "A", "FYT","2018/10/22","NEW","New Purchase with SIP","SIN"," ","-4","wwww"),
      ("106", "SIN" , "aa", "2", "2016/01/01", "2019/10/25", "B-15", "R", "FYT","2018/10/22","STPI","New Purchase SIP","SIN"," ","2","null"),
      ("108", "SIN" , "aa", "2", "2016/01/01", "2018/10/29", "B-30", "B", "FYT","2018/10/22","STPI","New Purchase SIP","SIN"," ","1","null"),
      ("108", "SIN" , "aa", "2", "2016/01/01", "2015/06/22", "B-15", "R", "FYT","2018/10/22","NEW","New Purchase with SIP","SIN"," ","1","null"),
      ("108", "SIN" , "aa", "2", "2016/01/01", "2016/03/22", "B-30", "A", "FYT","2018/10/22","STPA","New Purchase SIP","SIN"," ","1","null")
    )

    var input2 = Seq(
      ("2", "FYT", "2018-05-01", "2018-11-01", "SIN", "COMBO", "M", "60", "MS", "ARN-0155"),
      ("2", "FYT", "2018-09-01", "2018-11-01", "STPA", "MULTI", "M", "70", "EG", "ARN-0155"),
      ("2", "FYT", "2018-11-23", "2018-11-23", "SIN", "COMBO SIP", "M", "30", "AR", "ARN-0155"),
      ("2", "FYT", "2018-11-23", "2018-11-23", "STPA", "MULTI SIP", "M", "70", "EF", "ARN-0155"),
      ("2", "FYT", "2018-05-01", "2018-05-01", "TRFI", "COMBO", "N", "60", "ES", "ARN-0155")
    )

    import spark.implicits._

    var inputDF = input.toDF("trs_fund", "trans_type", "word", "agenttype", "OrgSipRegDate", "trs_trdt", "InvCityCategory", "TerCategory", "BrokType" ,"SipRegDate","PurTrtype","remarks","TRFI_Parent_Trtype","SipRemarks","Rate","AppliedRate")
      inputDF.show()
    var inputDF2 = input2.toDF("brokerage_id", "brokerage_type", "sip_regdt", "trs_trdt", "trs_purtrtype", "subtrtype", "FrequencyType", "SIPInstallments", "trs_scheme", "trs_agent")

    //Function Calling - RepleicationFYT

    FYTReplication.FYTReplication(spark, inputDF)
    BrmsRate.Brmsrate(spark, inputDF)
    LTTReplication.LTTReplication(spark, inputDF)
//  RateACCtotrancdt.RateACCtotrancdt(spark, inputDF)
    updtFytLtt.updtFytLtt(spark, inputDF)

//    defagentcat.defagentcat(spark, inputDF)

  }
}



















