package com.spark.report

import com.spark.config.ApplicationConfig
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}

/**
 * @Author White Liu
 * @Description 广告区域统计： ads_region_analysis
 * @Date 2021/1/21 15:20
 * @Version 1.0
 */
object AdsRegionAnalysis {

  def doReport(dataFrame: DataFrame) = {
    val spark: SparkSession = dataFrame.sparkSession
    import spark.implicits._

    //注册为临时视图
    dataFrame.createOrReplaceTempView("tem_view_pmt")
    //编写SQL分析并执行
    val reportDF: DataFrame = spark.sql(ReportSQLConstant.reportAdsRegionSQL("tem_view_pmt"))
    reportDF.show(20, truncate = false)
    //保存数据至MYSQL表中
    reportDF
      .coalesce(1)
      .write
      .mode(SaveMode.Append)
      .format("jdbc")
      .option("driver",ApplicationConfig.MYSQL_JDBC_DRIVER)
      .option("url",ApplicationConfig.MYSQL_JDBC_URL)
      .option("user",ApplicationConfig.MYSQL_JDBC_USERNAME)
      .option("password",ApplicationConfig.MYSQL_JDBC_PASSWORD)
      .option("dbtable","ads_region_analysis")
      .save()
  }

}
