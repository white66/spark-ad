package com.spark.etl

import com.spark.config.ApplicationConfig
import com.spark.utils.{IpUtils, SparkUtils}
import javafx.application.Application
import org.apache.spark.SparkFiles
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructType}
import org.lionsoul.ip2region.{DbConfig, DbSearcher}

/**
 * @Author White Liu
 * @Description 广告数据ETL处理
 *             1.加载json数据
 *             2.解析IP地址为省份和城市
 *             3.数据保存至Hive表中
 * @Date 2021/1/20 9:39
 * @Version 1.0
 */
object PmtEtlRunner{


  def main(args: Array[String]): Unit = {
    //TODO: 1.创建sparksession对象，导入隐式转换和函数库
    val spark: SparkSession = SparkUtils.createSparkSession(this.getClass)
    import spark.implicits._

    //TODO: 2.从文件系统加载业务数据
    val pmtDF: DataFrame = spark.read.json(ApplicationConfig.DATAS_PATH)

    //TODO: 3.解析IP为省份和城市
    val etlDF: DataFrame = processDF(pmtDF)
    etlDF.show(20,truncate = false)

    //TODO: 4.保存至hive分区表中
    saveAsHiveTable(etlDF)

    //TODO: 关闭资源
    spark.stop()
  }

  /**
   * 保存etlDF至hive分区表中
   * @param dataFrame
   * @return
   */
  def saveAsHiveTable(dataFrame: DataFrame): Unit = {
    dataFrame.coalesce(1)
      .write
      .format("hive")
      .mode(SaveMode.Append)
      .partitionBy("date_str")
      .saveAsTable("itcast_ads.pmt_ads_info")
  }


  /**
   * 处理DataFrame，解析Ip成省份和城市
   * @param dataFrame
   * @return DataFrame
   */
  def processDF(dataFrame: DataFrame):DataFrame = {
    val session = dataFrame.sparkSession
    //TODO：优化性能，将字典数据进行分布式缓存
    //session.sparkContext.addFile(ApplicationConfig.IPS_DATA_REGION_PATH)

    val rowDF: RDD[Row] = dataFrame.rdd.mapPartitions { iter =>
      //创建dbSearch对象，从缓存中获取字典数据并传递
      val dbSearcher: DbSearcher = new DbSearcher(new DbConfig(), ApplicationConfig.IPS_DATA_REGION_PATH)
      iter.map { row =>
        //提取Ip信息解析成省份和城市
        val ipValue = row.getAs[String]("ip")
        //解析Ip成省份和城市
        val region: Region = IpUtils.convertIpToRegion(ipValue, dbSearcher)
        val newSeq: Seq[Any] = row.toSeq :+ region.province :+ region.city
        Row.fromSeq(newSeq)
      }
    }

    val newSchema: StructType = dataFrame.schema
      .add("province",StringType,nullable = true)
      .add("city",StringType,nullable = true)
    //创建新的dataFrame
    val df = session.createDataFrame(rowDF, newSchema)
    df.withColumn("date_str", date_sub(current_date(),1).cast(StringType))
  }
}
