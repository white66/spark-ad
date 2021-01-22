package com.spark.report

import com.spark.utils.SparkUtils
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.apache.spark.storage.StorageLevel
/**
 * @Author White Liu
 * @Description 针对广告点击数据，依据需求进行报表开发，具体说明如下：
 *             - 各地域分布统计： region_stat_analysis
 *             - 广告区域统计： ads_region_analysis
 *             - 广告APP统计： ads_app_analysis
 *             - 广告设备统计： ads_device_analysis
 *             - 广告网络类型统计：ads_network_analysis
 *             - 广告运营商统计： ads_isp_analysis
 *             - 广告渠道统计： ads_channel_analysis
 * @Date 2021/1/21 14:26
 * @Version 1.0
 */
object PmtReportRunner {
  def main(args: Array[String]): Unit = {
    //1.创建session对象
    val spark: SparkSession = SparkUtils.createSparkSession(this.getClass)
    import spark.implicits._

    //2.加载Hive表中ETL数据
    val etlDF: Dataset[Row] = spark.read
      .format("hive")
      .table("itcast_ads.pmt_ads_info")
      .where($"date_str".equalTo(date_sub(current_date(), 1).cast(StringType)))

    //判断hive表中是否存在数据，不存在则退出程序
    if (etlDF.count()==0) System.exit(-1)

    //从hive表中读取数据后要做7个需求报表，所以对etlDF做缓存处理
    etlDF.persist(StorageLevel.MEMORY_AND_DISK)
    //3.处理分析数据，开发需求报表,保存数据至MYSQL表中

    //3.1 各地域分布统计： region_stat_analysis
    //RegionStatAnalysis.doReport(etlDF)
    //3.2 广告区域统计： ads_region_analysis
    AdsRegionAnalysis.doReport(etlDF)
    //3.3 广告APP统计： ads_app_analysis
    //AdsAppAnalysis.doReport(etlDF)
    //3.4 广告设备统计： ads_device_analysis
    //AdsDeviceAnalysis.doReport(etlDF)
    //3.5 广告网络类型统计：ads_network_analysis
    //AdsNetworkAnalysis.doReport(etlDF)
    //3.6 广告运营商统计： ads_isp_analysis
    //AdsIspAnalysis.doReport(etlDF)
    //3.7 广告渠道统计： ads_channel_analysis
    //AdsChannelAnalysis.doReport(etlDF)
    etlDF.unpersist()

    //4.关闭资源
    spark.stop()
  }

}
