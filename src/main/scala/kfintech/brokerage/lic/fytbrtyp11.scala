//package kfintech.brokerage.lic
//import org.apache.spark.sql.functions.{coalesce, col, lit, to_date , _}
//import org.apache.spark.sql.functions.concat
//import org.apache.hudi.org.apache.jetty.server.session.Session
//import org.apache.spark.sql.{DataFrame, SparkSession}
//
//
//object fytbrtyp11 {
//
//  def fytbrtyp11 (spark: SparkSession, inputDF: DataFrame): Unit = {
//
//    val TempBrokTypeID11 = inputDF
//      .filter(col("agenttype") === lit("2"))
//      .withColumn("")
//  }
//
//}
