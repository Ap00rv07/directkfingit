package kfintech.brokerage.mirae
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.sql.functions.{coalesce, col, lit, to_date, when}

object ARNCamacode {

  def ARNCamacode(spark: SparkSession , input2: DataFrame): Unit = {
    var input3 = Seq(
      ("ARN-0123", "ARN-0123"),
      ("ARN-0124", "ARN-0123"),
      ("ARN-0123", "ARN-0123"),
      ("ARN-0124", "ARN-0124"),
      ("ARN-0123", "ARN-0123"),
      ("ARN-0124", null),
      ("ARN-0123", "ARN-0123"),
      ("ARN-0124", "ARN-0124"),
      ("ARN-0124", null),
      ("ARN-0124", "ARN-0124"),
      ("ARN-0124",null),
      ("ARN-0124", null),
      ("ARN-0123", "ARN-0123"))

    import spark.implicits._

    val ab_master = input3.toDF("abm_agent","abm_camacode")

    val updtinput2df = input2
                      .filter(!(col("PurTrtype")isInCollection(List("IPO","NFO"))))

    val updtab_masterDF = ab_master
                    .filter(coalesce(col("abm_camacode"), lit(" ")) =!= lit(" "))
                    .dropDuplicates("abm_agent")
                    .select(col("abm_agent"), col("abm_camacode"))

    val updtab_masterDF2 = ab_master
      .filter(coalesce(col("abm_camacode"), lit(" ")) =!= lit(" "))
      .select(col("abm_camacode")).distinct()

    val camacode = updtinput2df
                    .join(updtab_masterDF,
                    updtinput2df("trs_agent") === updtab_masterDF("abm_camacode"),"left")
                    //.select(updtinput2df("*"))
      .withColumn("SubBrokCode" , col("trs_agent"))
      .withColumn("trs_agent" , col("abm_camacode"))
      .withColumn("AgentType" , when(col("AgentType") === lit("2") , lit("26"))
        .when(col("AgentType") === lit("3") , lit("27"))
        .otherwise(col("AgentType")))

    camacode.show()

    var updCamCode1 = camacode.filter(!(col("AgentType").isInCollection(List("26","27"))))
    var updCamCode2 = updCamCode1.join(updtab_masterDF2,updCamCode1("trs_agent") === updtab_masterDF2("abm_camacode"), "leftanti")

    updCamCode2.show()

    val replicatedSets: Seq[DataFrame] = Seq(input2, updCamCode1)

    val outputDF = replicatedSets.reduce(_ unionByName _)

    outputDF.show()




  }

}
