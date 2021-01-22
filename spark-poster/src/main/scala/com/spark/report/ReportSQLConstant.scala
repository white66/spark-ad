package com.spark.report

/**
 * @Author White Liu
 * @Description 详情
 * @Date 2021/1/22 10:06
 * @Version 1.0
 */
object ReportSQLConstant {
  def reportAdsRegionRateSQL(temViewName: String): String = {
    s"""
       |SELECT
       |  t.*,
       |  round(t.success_rtx_cnt / t.join_rtx_cnt,2) AS success_rtx_rate,
       |  round(t.ad_click_cnt / t.ad_show_cnt,2) AS ad_click_rate,
       |  round(t.media_click_cnt / t.media_show_cnt,2) AS media_click_rate
       |FROM
       |  $temViewName t
       |WHERE
       |  t.join_rtx_cnt !=0 AND t.success_rtx_cnt !=0
       |  AND t.ad_click_cnt !=0 AND t.ad_show_cnt !=0
       |  AND t.media_click_cnt !=0 AND t.media_show_cnt !=0
       |""".stripMargin
  }


  /**
   * 统计广告点击数、媒体点击数等： ads_region_analysis
   *
   * @param temViewName
   * @return
   */
  def reportAdsRegionSQL(temViewName: String): String = {
    s"""
       |WITH tem AS(
       |SELECT
       |  cast(date_sub(current_date(),1) AS string) AS report_date,
       |  t.province, t.city,
       |  SUM(
       |    CASE
       |      WHEN (t.requestmode = 1 and t.processnode>=1)
       |      THEN 1
       |      ELSE 0
       |      END
       |  ) AS orginal_req_cnt,
       |   SUM(
       |    CASE
       |      WHEN (t.requestmode = 1 and t.processnode>=2)
       |      THEN 1
       |      ELSE 0
       |      END
       |  ) AS valid_req_cnt,
       |  SUM(
       |    CASE
       |      WHEN (t.requestmode = 1 and t.processnode=3)
       |      THEN 1
       |      ELSE 0
       |      END
       |  ) AS ad_req_cnt,
       |  SUM(
       |    CASE
       |      WHEN (t.adplatformproviderid >=100000
       |            and t.iseffective = 1
       |            and t.isbilling = 1
       |            and t.isbid = 1
       |            and t.adorderid != 0)
       |      THEN 1
       |      ELSE 0
       |      END
       |  ) AS join_rtx_cnt,
       |  SUM(
       |    CASE
       |      WHEN (t.adplatformproviderid >=100000
       |            and t.iseffective = 1
       |            and t.isbilling = 1
       |            and t.iswin = 1)
       |      THEN 1
       |      ELSE 0
       |      END
       |  ) AS success_rtx_cnt,
       |  SUM(
       |    CASE
       |      WHEN (t.requestmode = 2
       |            and t.iseffective = 1)
       |      THEN 1
       |      ELSE 0
       |      END
       |  ) AS ad_show_cnt,
       |  SUM(
       |     CASE
       |      WHEN (t.requestmode = 3
       |            and t.iseffective = 1)
       |      THEN 1
       |      ELSE 0
       |      END
       |  ) AS ad_click_cnt,
       |  SUM(
       |     CASE
       |      WHEN (t.requestmode = 2
       |            and t.iseffective = 1
       |            and t.isbilling = 1)
       |      THEN 1
       |      ELSE 0
       |      END
       |  ) AS media_show_cnt,
       |  SUM(
       |     CASE
       |      WHEN (t.requestmode = 3
       |            and t.iseffective = 1
       |            and t.isbilling = 1)
       |      THEN 1
       |      ELSE 0
       |      END
       |  ) AS media_click_cnt,
       |  SUM(
       |    CASE
       |      WHEN (t.adplatformproviderid >=100000
       |            and t.iseffective = 1
       |            and t.isbilling = 1
       |            and t.iswin = 1
       |            and t.adorderid > 200000
       |            and t.adcreativeid > 200000)
       |      THEN floor(t.winprice / 1000)
       |      ELSE 0
       |      END
       |  ) AS dsp_pay_money,
       |  SUM(
       |    CASE
       |      WHEN (t.adplatformproviderid >=100000
       |            and t.iseffective = 1
       |            and t.isbilling = 1
       |            and t.iswin = 1
       |            and t.adorderid > 200000
       |            and t.adcreativeid > 200000)
       |      THEN floor(t.adpayment / 1000)
       |      ELSE 0
       |      END
       |  ) AS dsp_cost_money
       |  FROM
       |    $temViewName t
       |  GROUP BY
       |    t.province, t.city)
       |SELECT
       |  tt.*,
       |  round(tt.success_rtx_cnt / tt.join_rtx_cnt,2) AS success_rtx_rate,
       |  round(tt.ad_click_cnt / tt.ad_show_cnt,2) AS ad_click_rate,
       |  round(tt.media_click_cnt / tt.media_show_cnt,2) AS media_click_rate
       |FROM
       |  TEM tt
       |WHERE
       |  tt.join_rtx_cnt !=0 AND tt.success_rtx_cnt !=0
       |  AND tt.ad_click_cnt !=0 AND tt.ad_show_cnt !=0
       |  AND tt.media_click_cnt !=0 AND tt.media_show_cnt !=0
       |""".stripMargin
  }

}
