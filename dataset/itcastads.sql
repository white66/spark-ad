/*
 Navicat Premium Data Transfer

 Source Server         : localhost
 Source Server Type    : MySQL
 Source Server Version : 50635
 Source Host           : localhost:3306
 Source Schema         : itcastads

 Target Server Type    : MySQL
 Target Server Version : 50635
 File Encoding         : 65001

 Date: 22/01/2021 16:21:15
*/

SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

-- ----------------------------
-- Table structure for ads_region_analysis
-- ----------------------------
DROP TABLE IF EXISTS `ads_region_analysis`;
CREATE TABLE `ads_region_analysis`  (
  `report_date` text CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `province` text CHARACTER SET utf8 COLLATE utf8_general_ci NULL,
  `city` text CHARACTER SET utf8 COLLATE utf8_general_ci NULL,
  `orginal_req_cnt` bigint(20) NULL DEFAULT NULL,
  `valid_req_cnt` bigint(20) NULL DEFAULT NULL,
  `ad_req_cnt` bigint(20) NULL DEFAULT NULL,
  `join_rtx_cnt` bigint(20) NULL DEFAULT NULL,
  `success_rtx_cnt` bigint(20) NULL DEFAULT NULL,
  `ad_show_cnt` bigint(20) NULL DEFAULT NULL,
  `ad_click_cnt` bigint(20) NULL DEFAULT NULL,
  `media_show_cnt` bigint(20) NULL DEFAULT NULL,
  `media_click_cnt` bigint(20) NULL DEFAULT NULL,
  `dsp_pay_money` bigint(20) NULL DEFAULT NULL,
  `dsp_cost_money` bigint(20) NULL DEFAULT NULL,
  `success_rtx_rate` double NULL DEFAULT NULL,
  `ad_click_rate` double NULL DEFAULT NULL,
  `media_click_rate` double NULL DEFAULT NULL
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Compact;

-- ----------------------------
-- Table structure for region_stat_analysis
-- ----------------------------
DROP TABLE IF EXISTS `region_stat_analysis`;
CREATE TABLE `region_stat_analysis`  (
  `report_date` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `province` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `city` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `count` bigint(20) NULL DEFAULT NULL,
  PRIMARY KEY (`report_date`, `province`, `city`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Compact;

SET FOREIGN_KEY_CHECKS = 1;
