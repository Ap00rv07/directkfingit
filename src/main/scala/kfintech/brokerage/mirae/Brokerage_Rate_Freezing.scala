package kfintech.brokerage.mirae
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{coalesce, col, collect_list, lit, to_date}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SparkSession}

object Brokerage_Rate_Freezing {

      def brokerage_Rate_Freezing(spark :SparkSession , inputDf2 : DataFrame) : Unit= {

        var i_fund = "108"

        val updtbrkg = inputDf2
                      .filter(col("trs_fund") === i_fund &&
                        col("PurTrtype").isInCollection(List("SIN" ,"STPA" ,"STPN")) &&
                        col("trs_scheme").isInCollection(List("TS","MC" ,"FF")) &&
                        col("Orgsipregdate").between(to_date(lit("01-04-2019") , "dd-mm-yyyy") ,to_date(lit("05-03-2021") , "dd-mm-yyyy")) &&
                        col("trs_trdt").gt(to_date(lit("05-03-2021") , "dd-mm-yyyy")) &&
                        !col("TrxnAgent").isInCollection(List("ARN-1390" ,"ARN-OD71527" ,"ARN-OD82644" ,"ARN-OD121946","ARN-0164")))
                      .withColumn("Sipregdate",to_date(lit("28-02-2021") , "dd-mm-yyyy"))


        val updtbrkg2 = inputDf2
                      .filter(col("trs_fund") === i_fund
            && col("trs_scheme").isInCollection(List("TS", "MC", "FF"))
            && col("TRFI_Parent_TrType") === lit("SIN" )
            && col("Orgsipregdate").between(to_date(lit("01-04-2019"), "dd-mm-yyyy"), to_date(lit("05-03-2021"), "dd-mm-yyyy"))
            && !col("TrxnAgent").isInCollection(List("ARN-1390", "ARN-OD71527", "ARN-OD82644", "ARN-OD121946", "ARN-0164")))
          .withColumn("Sipregdate", to_date(lit("28-02-2021"), "dd-mm-yyyy"))

        val updtKotakcutoffdt =inputDf2
          .filter(col("trs_fund") === i_fund
          && col("PurTrtype").isInCollection(List("SIN" , "STPA" ,"STPN"))
          && col("trs_scheme").isInCollection (List("TS" , "MS" , "FF"))
          && col("Orgsipregdate").between(to_date(lit("01-04-2019"), "dd-mm-yyyy"), to_date(lit("15-03-2021"), "dd-mm-yyyy"))
          && col("trs_trdt").gt(to_date(lit("15-03-2021") , "dd-mm-yyyy"))
          && col("TrxnAgent").isInCollection(List("ARN-1390" ,"ARN-OD71527" ,"ARN-OD82644" ,"ARN-OD121946","ARN-0164")))
          .withColumn("Sipregdate" , to_date(lit("28-02-2021") , "dd-mm-yyyy"))

        val updtKotakcutoffdt2 = inputDf2
          .filter(col("trs_fund") === i_fund
          && col("trs_scheme").isInCollection(List("TS", "MS", "FF"))
          && col("TRFI_Parent_TrType") === lit("SIN")
          && col("Orgsipregdate").between(to_date(lit("01-04-2019"), "dd-mm-yyyy"), to_date(lit("15-03-2021"), "dd-mm-yyyy"))
          && col("trs_trdt").gt(to_date(lit("15-03-2021"), "dd-mm-yyyy"))
          && col("TrxnAgent").isInCollection(List("ARN-1390", "ARN-OD71527", "ARN-OD82644", "ARN-OD121946", "ARN-0164")))
          .withColumn("Sipregdate", to_date(lit("28-02-2021"), "dd-mm-yyyy"))
      }
}
