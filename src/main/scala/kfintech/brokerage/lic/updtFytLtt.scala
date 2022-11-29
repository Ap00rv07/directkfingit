package kfintech.brokerage.lic
import org.apache.spark.sql.functions.{coalesce, col, lit, to_date , _}
import org.apache.spark.sql.functions.concat
import org.apache.hudi.org.apache.jetty.server.session.Session
import org.apache.spark.sql.{DataFrame, SparkSession}
object updtFytLtt {

  def updtFytLtt (spark: SparkSession , inputDF : DataFrame) : Unit ={
//    var i_Fund = 108

    val trtype = Seq("SIN" ,"STPA" ,"STPN")
    val Atype = Seq("2" ,"3")

    val update = inputDF.filter(col("trs_fund") === lit("108")
////      && col("agenttype").isin(Atype: _*)
//      && col("purtrtype").isin(trtype: _*)
//      && col("SipRegDate").lt(to_date(lit("2018/10/22") , "yyyy/mm/dd"))
//      && col("trs_trdt").geq(to_date(lit("2018/10/22") , "yyyy/mm/dd"))
      && coalesce(col("Rate") , lit("0")).lt(lit("0")))
      .withColumn("Rate" , lit("0"))
      .withColumn("AppliedRate" , concat(coalesce(col("AppliedRate") , lit(" ")) , lit("|| Apply ZERO - For Broktype 2 and 3 Rate Negative cases") ))

    var lumpsumUpdate = inputDF.filter(col("trs_fund") === lit("108")
      && col("agenttype").isin(Atype: _*)
      && !inputDF("purtrtype").isin(trtype: _*) //not in krna hai
      && col("trs_trdt").geq(to_date(lit("2018/10/22"), "yyyy/mm/dd"))
      && coalesce(col("Rate"), lit("0")).lt(lit("0")))
      .withColumn("Rate", lit("0"))
      .withColumn("AppliedRate", concat(coalesce(col("AppliedRate"), lit(" ")), lit("|| Apply ZERO - For Broktype 2 and 3 Rate Negative cases")))

    update.show()

  }

}

