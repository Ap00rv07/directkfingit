package kfintech.brokerage.mirae
import kfintech.brokerage.mirae.ARNCamacode
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.sql.functions.{coalesce, col, lit, to_date}


object main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("AxisVariation")
      .config("spark.master", "local")
      .getOrCreate()

    Logger.getLogger("org").setLevel(Level.OFF)
    val log: Logger = org.apache.log4j.LogManager.getLogger("Spark ETL Log")
    log.setLevel(Level.INFO)

    var input = Seq(
      ("108", "SIN", "aa", "2", "2016/01/01", "2018/10/22", "B-15", "R", "FYT", "2018/10/02", "SIN", "Existing Folio SIP Hello", "SIN", " ", "-2", "null", "ARN-0123"),
      ("108", "ASDS", "aa", "2", "2016/02/01", "2018/11/22", "B-30", "R", "FYT", "2018/07/24", "STPA", "New Purchase SIP", "STPA", " ", "-3", "null", "ARN-0124"),
      ("108", "SIN", "aa", "2", "2016/01/01", "2018/10/22", "B-15", "R", "FYT", "2018/10/02", "IPO", "Existing Folio SIP Hello", "SIN", " ", "-2", "null", "ARN-0123"),
      ("108", "ASDS", "aa", "3", "2016/02/01", "2018/11/22", "B-30", "R", "LTT", "2018/07/24", "NFO", "New Purchase SIP", "STPA", " ", "-3", "null", "ARN-0123"),
      ("108", "SIN", "aa", "2", "2016/01/01", "2018/10/22", "B-15", "R", "FYT", "2018/10/02", "IPO", "Existing Folio SIP Hello", "SIN", " ", "-2", "null", "ARN-0124"),
      ("108", "ASDS", "aa", "2", "2016/02/01", "2018/11/22", "B-30", "R", "FYT", "2018/07/24", "NFO", "New Purchase SIP", "STPA", " ", "-3", "null", "ARN-0123"),
      ("110", "STPA", "ll", "3", "2015/01/01", "2018/10/22", "B-15", "A", "LTT", "2017/04/22", "STPI", "New Purchase SIP", "SIN", " ", "-5", "null", "ARN-0123"),
      ("109", "STIP", "lm", "3", "2014/01/01", "2018/10/22", "B-15", "B", "LTT", "2016/05/22", "NEW", "New Purchase SIP", "STPA", " ", "-3", "null", "ARN-0123"),
      ("109", "NOP", "bb", "3", "2016/03/01", "2019/03/22", "B-15", "R", "LTT", "2019/10/24", "IPO", "Existing Folio SIP Hello", "SIN", " ", "-2", "aaaa", "ARN-0124"),
      ("108", "AA", "tm", "2", "2016/01/23", "2020/07/22", "B-15", "R", "FYT", "2020/10/27", "SIN", "Existing Folio SIP Hello", "STPA", " ", "-2", "null", "ARN-0123"),
      ("109", "NOP", "kj", "2", "2015/02/21", "2018/10/22", "B-30", "B", "FYT", "2018/12/28", "STPA", "Existing Folio SIP Hello", "STPA", " ", "-2", "null", "ARN-0123"),
      ("107", "SIN", "aa", "2", "2017/02/01", "2018/12/22", "B-15", "R", "FYT", "2018/10/29", "NEW", "Existing Folio SIP Hello", "SIN", " ", "-3", "dddd", "ARN-0123"),
      ("108", "SIN", "aa", "2", "2016/01/01", "2018/10/22", "B-15", "R", "FYT", "2018/10/22", "STPI", "Existing Folio SIP Hello", "SIN", " ", "-2", "ffff", "ARN-0123"),
      ("108", "SIN", "aa", "2", "2016/01/01", "2018/10/17", "B-30", "A", "FYT", "2018/10/22", "NFO", "Existing Folio SIP Hello", "STPA", " ", "-4", "null", "ARN-0124"),
      ("108", "SIN", "aa", "2", "2016/01/01", "2017/10/22", "B-15", "B", "FYT", "2018/10/22", "SIN", "New Purchase SIP", "STPA", " ", "-1", "null", "ARN-0123"),
      ("108", "SIN", "aa", "2", "2016/01/01", "2014/11/22", "B-30", "B", "FYT", "2018/10/22", "NEW", "New Purchase SIP", "STPA", " ", "-2", "null", "ARN-0123"),
      ("107", "SIN", "aa", "2", "2016/01/01", "2020/10/22", "B-15", "R", "FYT", "2018/10/22", "STPA", "New Purchase SIP", "SIN", " ", "-3", "null", "ARN-0123"),
      ("108", "SIN", "aa", "2", "2016/01/01", "2021/10/24", "B-30", "A", "FYT", "2018/10/22", "NEW", "New Purchase with SIP", "SIN", " ", "-4", "wwww", "ARN-0123"),
      ("106", "SIN", "aa", "2", "2016/01/01", "2019/10/25", "B-15", "R", "FYT", "2018/10/22", "STPI", "New Purchase SIP", "SIN", " ", "2", "null", "ARN-0123"),
      ("108", "SIN", "aa", "2", "2016/01/01", "2018/10/29", "B-30", "B", "FYT", "2018/10/22", "STPI", "New Purchase SIP", "SIN", " ", "1", "null", "ARN-0124"),
      ("108", "SIN", "aa", "2", "2016/01/01", "2015/06/22", "B-15", "R", "FYT", "2018/10/22", "NEW", "New Purchase with SIP", "SIN", " ", "1", "null", "ARN-0124"),
      ("108", "SIN", "aa", "3", "2016/01/01", "2016/03/22", "B-30", "A", "LTT", "2018/10/22", "STPA", "New Purchase SIP", "SIN", " ", "1", "null", "ARN-0123")
    )
    import spark.implicits._

    var inputDF = input.toDF("trs_fund", "trans_type", "word", "AgentType", "OrgSipRegDate", "trs_trdt", "InvCityCategory", "TerCategory", "BrokType" ,"SipRegDate","PurTrtype","remarks","TRFI_Parent_Trtype","SubBrokCode","Rate","AppliedRate","trs_agent")

//    import spark.implicits._

//    val inputDf2 = input2.toDF("")

    ARNCamacode.ARNCamacode(spark, inputDF)

  }
}
