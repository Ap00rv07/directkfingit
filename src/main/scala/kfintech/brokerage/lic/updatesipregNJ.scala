//package kfintech.brokerage.lic
//
//import org.apache.hudi.org.apache.jetty.websocket.common.frames.DataFrame
//import org.apache.spark.sql.SparkSession
//import org.apache.spark.sql.expressions.Window
//import org.apache.spark.sql.functions.{col, _}
//import org.apache.spark.sql.types.StringType
//import org.apache.spark.sql.{DataFrame, SparkSession}
//
//object updatesipregNJ {
//
//     def updatesipregNJ(spark: SparkSession , inputDf2 : DataFrame , Splbrk_NJIndiaExchangeSIPRegdateloadDF : DataFrame): Unit ={
//
//       val Splbrk_NJIndiaExchangeSIPRegdateDF = Splbrk_NJIndiaExchangeSIPRegdateloadDF.selectExpr("SipRegdt","fund","scheme","plan","branch","trno")
//
//       val updtinputdf2 = inputDf2.alias("a")
//         .join( Splbrk_NJIndiaExchangeSIPRegdateDF.alias("b"),
//           col("a.trs_fund") === col("b.fund") &&
//             col("a.trs_scheme") === col("b.scheme") &&
//             col("a.trs_plan") === col("b.plan") &&
//             col("a.trs_branch") === col("b.branch") &&
//             col("a.trs_trno") === coalesce(col("b.trno"),lit("0")) ,
//          "inner"
//         )
//         //jointype?
//              .filter(col("a.trs_branch").isInCollection(List("BS88","NS88","IX88")))
//            .withColumn(col("a.SipRegDate") , col("b.SipRegdt"))
////             .withColumn(col("a.SIPRegSource") ,concat(coalesce(col("SIPRegSource") , lit(" ")) , lit("") , lit("NJIndiaSIPREGDATE") , lit("| NJ:") ,
//
//
//       val updtinputdf3 = inputDf2.alias("a")
//         .join(Splbrk_NJIndiaExchangeSIPRegdateDF.alias("b"),
//             col("a.trs_fund") === col("b.fund") &&
//             col("a.trs_plan") === col("b.plan") &&
//               col("a.trs_scheme") === Splbrk_NJIndiaExchangeSIPRegdateDF("b.scheme") &&
//             col("a.trs_branch") === col("b.branch") &&
//             coalesce(col("a.TRFI_Parent_RegSlno"), lit("0")) === Splbrk_NJIndiaExchangeSIPRegdateDF("b.SIPregSlnorefno"))
//             .filter(col("a.trs_branch").isInCollection(List("BS88", "NS88", "IX88")))
//       //             .withColumn(inputDf2("SIPRegSource") ,concat(coalesce(col("SIPRegSource") , lit(" ")) , lit("") , lit("NJIndiaSIPREGDATE") , lit("| NJ:") ,
//
//
//     }
//}
