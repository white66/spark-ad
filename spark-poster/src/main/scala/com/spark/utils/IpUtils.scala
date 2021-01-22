package com.spark.utils

import com.spark.etl.Region
import org.lionsoul.ip2region.{DataBlock, DbConfig, DbSearcher}

/**
 * @Author White Liu
 * @Description IP解析工具类
 * @Date 2021/1/18 16:42
 * @Version 1.0
 */
object IpUtils {
  /**
   * @param ip  ip地址
   * @param dbSearcher  DbSearcher对象
   * @return Region 省份城市信息
   */
    def convertIpToRegion(ip: String,dbSearcher: DbSearcher): Region = {
      //1.创建DbSearch对象，传递字典数据
      //val dbSearcher: DbSearcher = new DbSearcher(new DbConfig(), ApplicationConfig.IPS_DATA_REGION_PATH)
      //2.传递IP地址，进行解析
      val dataBlock: DataBlock = dbSearcher.binarySearch(ip)
      val region: String = dataBlock.getRegion
      //3.提取省份和城市-> 中国|0|北京|北京市|阿里云
      val Array(_,_,province,city,_) = region.split("\\|")
      //4.封装省份和城市及IP地址至Region对象中
      Region(ip,province,city)
    }
}
