package kfintech.brokerage.mirae
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{coalesce, col, lit, to_date}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SparkSession}

object SEBIRegulations {

  def SEBIRegulations(spark : SparkSession , inputDf2 : DataFrame) : Unit = {

        var i_fund = 108
        val updtsipregdt = inputDf2
          .filter(col("trs_fund") === i_fund &&
          col("PurTrtype").isInCollection(List("SIN" , "STPA" , "STPN")) &&
          coalesce(col("SipRegDate") , col("trs_trdt")).geq(to_date(lit("01-04-2019") , "dd-mm-yyyy")))
          .withColumn("SipRegDate" , col("trs_trdt"))

    val updtsipregdt2 = inputDf2
      .filter(col("trs_fund") === i_fund &&
        col("TRFI_Parent_TrType ") === lit("SIN") &&
        coalesce(col("SipRegDate"), col("trs_trdt")).geq(to_date(lit("01-04-2019"), "dd-mm-yyyy")))
      .withColumn("SipRegDate", col("trs_trdt"))
  }

}
