package kfintech.brokerage.lic
import org.apache.spark.sql.functions.{coalesce, col, lit, to_date}
import org.apache.spark.sql.{DataFrame, SparkSession}

object BrmsRate {

    def Brmsrate (spark: SparkSession, inputDF: DataFrame): Unit = {
      val i_fund = "108"

      inputDF.printSchema()

      val backupRemark = inputDF.filter(col("trs_fund") === lit("108")
                   &&(coalesce(col("remarks") , lit(" ")).like("Existing Folio%SIP%")
                   || coalesce(col("remarks") , lit(" ")).like("New Purchase%SIP%")))
                  .withColumn("SipRemarks", col("remarks"))

      val updateRemark = inputDF.filter(col("trs_fund") === lit("108")
        && (coalesce(col("remarks"), lit(" ")).like("Existing Folio%SIP%")
        || coalesce(col("remarks"), lit(" ")).like("New Purchase%SIP%")))
        .withColumn("remarks", lit("New Purchase with SIP"))

      val updateParentTrtype = inputDF.filter(col("PurTrtype") === lit("SIN")
                               && coalesce(col("TRFI_Parent_Trtype") , lit(" ")) != lit("SIN"))
        .withColumn("TRFI_Parent_Trtype", col("PurTrtype"))

      val updateParentTrtypeSIN = inputDF.filter(col("PurTrtype") === lit("NEW")
                                && coalesce(col("remarks") , lit(" ")) === lit("New Purchase with SIP"))
                                .withColumn("TRFI_Parent_Trtype" , lit("SIN"))

      val updateParentTrtypeSTPA = inputDF.filter(col("PurTrtype") === lit("STPA")
                                          && coalesce(col("TRFI_Parent_Trtype") , lit(" ")) != lit("STPA"))
        .withColumn("TRFI_Parent_Trtype" , col("PurTrtype"))

      backupRemark.show()
      updateRemark.show()
      updateParentTrtype.show()
      updateParentTrtypeSIN.show()
      updateParentTrtypeSTPA.show()


    }

}




