package com.zhugeio.etl.id.adtoufang.common

import com.zhugeio.etl.adtoufang.common.Utm

/**
  * 回传事件记录信息 写入表 toufang_convert_event
  */
class ConvertMessage {

  var zg_id: Long = 0 //用户id
  var zg_eid: Long = 0 //事件id
  var event_name: String = "" //事件
  var lid: Long = 0 //链接id
  var channel_id: Long = 0 //渠道id
  var zg_appid: Integer = 0 //应用id
  var channel_adgroup_id: Long = 0 // 广告计划id
  var channel_adgroup_name: String = "" //广告计划名称
  var click_time: Long = 0 //广告点击事件的时间
  var event_time: Long = 0 //事件时间
  var channel_event: String = "" //渠道事件类型
  var match_json: String = "" // 匹配条件
  var frequency: Integer = 0 //频次
  var utm_campaign: String = "" //活动名称
  var utm_source: String = "" //广告来源
  var utm_medium: String = "" //广告媒介
  var utm_term: String = "" //关键词
  var utm_content: String = "" //广告内容
  var uuid: String = ""

  def getUtm(utm: Utm) = {
    this.utm_campaign = utm.utm_campaign
    this.utm_source = utm.utm_source
    this.utm_medium = utm.utm_medium
    this.utm_term = utm.utm_term
    this.utm_content = utm.utm_content
  }

  def toConvertJson() = {
    "{\"zg_id\":\""+zg_id+
      "\",\"zg_eid\":\""+zg_eid+
      "\",\"event_name\":\""+event_name+
      "\",\"lid\":\""+lid+
      "\",\"channel_id\":\""+channel_id+
      "\",\"zg_appid\":\""+zg_appid+
      "\",\"channel_adgroup_id\":\""+channel_adgroup_id+
      "\",\"channel_adgroup_name\":\""+channel_adgroup_name+
      "\",\"click_time\":\""+click_time+
      "\",\"event_time\":\""+event_time+
      "\",\"channel_event\":\""+channel_event+
      "\",\"match_json\":\""+match_json+
      "\",\"frequency\":\""+frequency+
      "\",\"utm_campaign\":\""+utm_campaign+
      "\",\"utm_source\":\""+utm_source+
      "\",\"utm_medium\":\""+utm_medium+
      "\",\"utm_term\":\""+utm_term+
      "\",\"utm_content\":\""+utm_content+
      "\",\"uuid\":\""+uuid+"\"}"

  }

}
