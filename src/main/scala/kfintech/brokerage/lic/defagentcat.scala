//package kfintech.brokerage.lic
//import org.apache.spark.sql.functions.{coalesce, col, lit, to_date, _}
//import org.apache.spark.sql.functions.concat
//import org.apache.hudi.org.apache.jetty.server.session.Session
//import org.apache.spark.sql.{DataFrame, SparkSession}
//
//object defagentcat {
//
//
//    def defagentcat(spark: SparkSession, inputDF2: DataFrame): Unit = {
//
//        val Replica_BrokType56_57 = inputDF2
//          .filter(col("brokerage_id").isInCollection(List("2" , "3"))
//          && col("trs_purtrtype").isInCollection(List("NEW", "ADD","SIN" ,"STPA"))
//          && col("trs_branch") === lit("WB99")
//          && col("sip_regdt").between((to_date(lit("2021-01-02") ,"yyyy-mm-dd")) , (to_date(lit("2021-03-31") ,("yyyy-mm-dd")))) )
//          .withColumn("brokerage_id",when( col("brokerage_id") === lit("2") , lit("56"))
//                .otherwise(lit("57")))
//
//      Replica_BrokType56_57.show()
//
//    }
//}
