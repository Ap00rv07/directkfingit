//package kfintech.brokerage.lic
//
//import org.apache.hudi.org.apache.jetty.websocket.common.frames.DataFrame
//import org.apache.spark.sql.SparkSession
//import org.apache.spark.sql.SparkSession
//import org.apache.spark.sql.expressions.Window
//import org.apache.spark.sql.functions._
//import org.apache.spark.sql.types.StringType
//
//object NJMirae7prxybranch {
//          def NJMirae7prxybranch(spark: SparkSession , inputDf2 : DataFrame , Splbrk_MTrans_Scheme : DataFrame ,splbrk_NJindiaExchangeTransactions : DataFrame): Unit ={
//
//            val upd7prxybranchDF = inputDf2.alias("a")
//              .join(Splbrk_MTrans_Scheme.alias("b") ,
//                col("a.trs_fund") === col("b.trs_fund") &&
//                  col("a.trs_scheme") === col("b.trs_scheme") &&
//                  col("a.trs_plan") === col("b.trs_plan") &&
//                  col("a.trs_trno") === col("b.trs_trno") &&
//                  col("a.trs_branch") === col("b.trs_branch") &&
//                  col("a.trs_acno") === col("b.trs_acno") ,
//                  "inner")
//              //join type
//              .filter(coalesce(col("b.trs_regslno") , lit(" ")) != lit(" "))
//                // emty string("") or (" ")
//                .withColumn("a.trs_regslno" , col("b.trs_regslno").cast("integer"))
//            // asInstanceOF[]  ?? , convert(numeric(22,0),b.trs_regslno)
//            .withColumn("a.prxybranch" , col("b.trs_prxybranch"))
//              .withColumn("a.trs_subtrtype" , col("b.trs_subtrtype"))
//
//
//            val upd7prxybranchDF2 =inputDf2.alias("a")
//              .join(splbrk_NJindiaExchangeTransactions.alias("b") ,
//                col("a.trs_fund ") === lit(" ") && //ifund
//                col("a.trs_scheme") === col("b.njx_scheme ") &&
//                  col("a.trs_plan") === col("b.njx_plan") &&
//                  col("a.trs_regslno") === col("b.njx_SIPregSlno") &&
//                  col("a.trs_acno") === col("b.njx_acno"),
//                  "inner")
//
//              )
//
//
//          }
//}
