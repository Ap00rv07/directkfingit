package kfintech.brokerage.lic
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.{DataFrame, SparkSession}

object LTTReplication {
  def LTTReplication(spark: SparkSession, inputDF: DataFrame): Unit = {

    var broktypemaster = Seq(
      ("LTT", "12"),
      ("LTT", "12"),
      ("FYT", "13"),
      ("FYT", "14"),
      ("LTT", "14"),
      ("FYT", "12"),
      ("FYT", "12")
    )

    import spark.implicits._
    val broktypemasterDF = broktypemaster.toDF("btm_subbrokdesc", "btm_code")
    val brokmasterdata = broktypemasterDF.filter(col("btm_code") === "12").select("btm_subbrokdesc")


    val TempBrokTypeID11_12 = inputDF.filter(col("agenttype") === "3")
      .join(brokmasterdata,inputDF("brokType") === brokmasterdata("btm_subbrokdesc"),"leftouter")
      .withColumn("agenttype", lit("12"))
      .withColumn("brokType",col("btm_subbrokdesc"))
      .drop("btm_subbrokdesc")

    TempBrokTypeID11_12.show()

  }
}