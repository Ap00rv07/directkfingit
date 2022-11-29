package kfintech.brokerage.lic

import org.apache.spark.sql.functions.{coalesce, col, lit, to_date}
import org.apache.spark.sql.{DataFrame, SparkSession}

object FYTReplication {

  def FYTReplication(spark: SparkSession, inputDF: DataFrame): Unit = {

    val addannual = inputDF.filter(col("agenttype") === lit("2"))
                          .withColumn("agenttype", lit("11"))

    val B30AddAnnual = inputDF.filter(col("agenttype") === lit("2")
                            && coalesce(col("OrgSipRegDate"),col("trs_trdt")).geq(to_date(lit("2016/01/01"), "yyyy/mm/dd"))
                            && col("trs_trdt").geq(to_date(lit("2018/10/22"), "yyyy/mm/dd"))
                            && coalesce(col("InvCityCategory"),lit(" ")) === lit("B-15")
                            && coalesce(col("TerCategory"),lit(" ")) === lit("R")
                            )
      .withColumn("agenttype",lit("94"))

    val B30Annual = inputDF.filter(col("agenttype") === lit("2")
      && coalesce(col("OrgSipRegDate"), col("trs_trdt")).geq(to_date(lit("2016/01/01"), "yyyy/mm/dd"))
      && col("trs_trdt").geq(to_date(lit("2018/10/22"), "yyyy/mm/dd"))
      && coalesce(col("InvCityCategory"), lit(" ")) === lit("B-15")
      && coalesce(col("TerCategory"), lit(" ")) === lit("R")
    )
      .withColumn("agenttype", lit("92"))

    val replicatedSets: Seq[DataFrame] = Seq(addannual, B30AddAnnual, B30Annual)

    val outputDF = replicatedSets.reduce(_ unionByName _)

    outputDF.show()

  }
}
