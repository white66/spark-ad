package com.spark.report

import java.sql.{Connection, DriverManager, PreparedStatement}

import com.spark.config.ApplicationConfig
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
/**
 * @Author White Liu
 * @Description 报表开发，按照地域维度（省份和城市）分组统计广告点击次数
 *             保存至MYSQL的region_stat_analysis表中
 * @Date 2021/1/21 15:20
 * @Version 1.0
 */
object RegionStatAnalysis {


  def doReport(dataFrame: DataFrame) = {
    val spark = dataFrame.sparkSession
    import spark.implicits._

    //使用DSL来分析
    val reportDF = dataFrame
      .groupBy($"province", $"city")
      .count()
      .orderBy($"count".desc)
      .withColumn("report_date",date_sub(current_date(),1).cast(StringType))
    reportDF.show(20,truncate = false)
    reportDF
      .coalesce(1)
      .foreachPartition(iter =>
        saveToMYSQL(iter)
      )
  }

  /**
   * 自定义JDBC，保存数据至MYSQL表中
   * @param datas
   */
  def saveToMYSQL(datas:Iterator[Row]) = {
    //1.加载驱动类
    Class.forName(ApplicationConfig.MYSQL_JDBC_DRIVER)
    var conn:Connection = null
    var pstmt: PreparedStatement = null

    try {
      //2.获取连接
      conn = DriverManager.getConnection(
        ApplicationConfig.MYSQL_JDBC_URL,
        ApplicationConfig.MYSQL_JDBC_USERNAME,
        ApplicationConfig.MYSQL_JDBC_PASSWORD
      )
      val insertSql =
        """|INSERT INTO
           |itcastads.region_stat_analysis(report_date,province,city,count)
           |VALUES(?,?,?,?) ON DUPLICATE KEY UPDATE count=VALUES(count);
           |""".stripMargin
      pstmt = conn.prepareStatement(insertSql)

      //获取当前数据库事务
      val autoCommit = conn.getAutoCommit
      conn.setAutoCommit(false)

      //插入数据
      datas.foreach { row =>
        pstmt.setString(1, row.getAs[String]("report_date"))
        pstmt.setString(2, row.getAs[String]("province"))
        pstmt.setString(3, row.getAs[String]("city"))
        pstmt.setLong(4, row.getAs[Long]("count"))
        //加入批次
        pstmt.addBatch()
      }
      //批量插入
      pstmt.executeBatch()
      //手动提交
      conn.commit()
      conn.setAutoCommit(autoCommit) //还原事务状态
    }catch {
      case e: Exception => e.printStackTrace()
    }finally {
      //关闭连接
      if (null !=pstmt) pstmt.close()
      if (null !=conn) conn.close()
    }
  }

}
