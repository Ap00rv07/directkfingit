// having errors in date type column
package kfintech.brokerage.lic
import org.apache.spark.sql.functions.{coalesce, col, lit, to_date}
import org.apache.spark.sql.{DataFrame, SparkSession}

object RateACCtotrancdt {

  def RateACCtotrancdt (spark: SparkSession , inputDF : DataFrame): Unit ={

        val trancdateapply = inputDF
          .filter(col("PurTrtype").isInCollection(List("SIN" ,"STPA" ,"STPI"))
          && coalesce(col("trs_trdt") , lit("1900/01/01")).gt(to_date(lit("2018/10/21"), "yyyy/mm/dd"))
            && coalesce(col("SipRegDate") , lit("1900/01/01")).lt(to_date(lit("2018/10/22") , "yyyy/mm/dd")))
          .withColumn("SipRegDate" , col("trs_trdt"))


        val trancdateapply2 = inputDF
      .filter(col("PurTrtype").isInCollection(List("SIN", "STPA", "STPI"))
        && coalesce(col("trs_trdt"), lit("1900/01/01")).gt(to_date(lit("2018/10/21"), "yyyy/mm/dd"))
        && coalesce(col("SipRegDate"), lit("1900/01/01")).lt(to_date(lit("2018/10/22"), "yyyy/mm/dd")))
      .withColumn("SipRegDate", col("trs_trdt"))
    trancdateapply.show()
  }
}





