package com.spark.utils

import com.spark.config.ApplicationConfig
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

/**
 * @Author White Liu
 * @Description 构建SparkSession实例对象工具类，加载配置属性
 * @Date 2021/1/18 15:53
 * @Version 1.0
 */
object SparkUtils extends Logging{

  /**
   * 构建SparkSession实例对象
   * @return SparkSession实例对象
   */
  def createSparkSession(clazz: Class[_]): SparkSession ={
    //1.创建SparkSession.Builder对象，设置应用名称
    val builder: SparkSession.Builder =SparkSession
      .builder()
      .appName(clazz.getSimpleName.stripSuffix("$"))
      //设置文件输出算法
      .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version","2")
      .config("spark.debug.maxToStringFields","20000")
    //2.判断是否是本地模式
    if(ApplicationConfig.APP_LOCAL_MODE){
      builder
        .master(ApplicationConfig.APP_SPARK_MASTER)
        .config("spark.sql.shuffle.partitions","4")
      logInfo("设置本地模式..............")
    }
    //3.判断是否与Hive集成，如果是，设置MetaStore URIS地址
    if(ApplicationConfig.APP_IS_HIVE){
      builder
        .enableHiveSupport()
        .config("hive.metastore.uris",ApplicationConfig.APP_HIVE_META_STORE_URLS)
        .config("hive.exec.dynamic.partition.mode","nonstrict")
      logWarning("spark与Hive集成.................")
    }
    //4.获取SparkSession实例对象
    builder.getOrCreate()
  }
}
