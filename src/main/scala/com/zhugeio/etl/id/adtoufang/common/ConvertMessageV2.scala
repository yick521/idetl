package com.zhugeio.etl.id.adtoufang.common

import com.alibaba.fastjson.JSONObject
import com.alibaba.fastjson.annotation.JSONField

/**
  * 回传行为数据，发至kafka，由后端回传给广告平台
  */
class ConvertMessageV2 {
  @JSONField
  var zg_id: Long = 0 //用户id
  @JSONField
  var callback:String=""
  @JSONField
  var ados:Integer=0 //操作系统平台 os
  @JSONField
  var muid:String="" //muid
  @JSONField
  var lid:Long=0 //链接id
  @JSONField
  var callback_url:String="" //回调链接
  @JSONField
  var imei:String="" //安卓设备ID md5

  //从缓存获取 select link_id,channel_event from ads_link_event where is_delete = 0
  @JSONField
  var event_type:String="" //渠道事件
  @JSONField
  var idfa:String="" //iOS 6+的设备ID
  // TODO 注意：该字段并未赋值
  @JSONField
  var plat:String="" // 设备类型 ANDROID("1"), IOS("2")

  //投放五期新增字段
  @JSONField
  var channel_type:String=""
  @JSONField
  var push_type:String=""
  @JSONField
  var click_time:Long=0L
  @JSONField
  var akey:String=""
  @JSONField
  var token:String=""
  @JSONField
  var company:String =""
  @JSONField
  var auth_account:String =""
  @JSONField
  var zg_appid:Long = 0L

  @JSONField
  var hw_app_id:String ="" //华为商城应用id
  @JSONField
  var action_time:String ="" //华为广告深度回传用 （取回传事件的事件触发时间）
  @JSONField
  var oaid:String = "" //安卓Q及更高版本的设备号
  @JSONField
  var creative_id:String = "" //vivo监测回传必备字段-创意id
  @JSONField
  var channel_click_id:String = "" //回传必备字段-clickId
  @JSONField
  var channel_account_id:String = "" //回传必备字段-账号id
  def toJsonString(): String = {
    "{\"callback\":\"" + callback +
      "\",\"os\":" + ados +
      ",\"muid\":\"" + muid +
      "\",\"lid\":" + lid +
      ",\"callback_url\":\"" + callback_url +
      "\",\"imei\":\"" + imei +
      "\",\"event_type\":\"" + event_type +
      "\",\"idfa\":\"" + idfa +
      "\",\"plat\":\"" + plat +
      "\",\"channel_type\":\"" + channel_type +
      "\",\"push_type\":\"" + push_type +
      "\",\"click_time\":" + click_time +
      ",\"akey\":\"" + akey +"\""+
      ",\"token\":\"" + token +"\""+
      ",\"company\":\"" + company +"\""+
      ",\"hw_app_id\":\"" + hw_app_id +"\""+
      ",\"oaid\":\"" + oaid +"\""+
      ",\"action_time\":\"" + action_time +"\""+
      ",\"auth_account\":\"" + auth_account +"\""+
      ",\"creative_id\":\"" + creative_id +"\""+
      ",\"channel_click_id\":\"" + channel_click_id +"\""+
      ",\"channel_account_id\":\"" + channel_account_id +"\""+
      ",\"zg_appid\":" + zg_appid +
      ",\"zg_id\":" + zg_id +
      "}"
  }

  def setFeilds(adMessageJson:JSONObject):Unit={
    this.callback = String.valueOf(if (adMessageJson.get("callback") == null) "" else adMessageJson.get("callback"))
    this.ados = String.valueOf(adMessageJson.get("ados")).toInt
    this.muid = String.valueOf(if (adMessageJson.get("muid") == null) "" else adMessageJson.get("muid"))
    this.imei = String.valueOf(if (adMessageJson.get("imei") == null) "" else adMessageJson.get("imei"))
    this.idfa = String.valueOf(if (adMessageJson.get("idfa") == null) "" else adMessageJson.get("idfa"))
    this.channel_type = String.valueOf(if (adMessageJson.get("channel_type") == null) "" else adMessageJson.get("channel_type"))
    this.push_type = String.valueOf(if (adMessageJson.get("push_type") == null) "" else adMessageJson.get("push_type"))
    this.channel_type = String.valueOf(if (adMessageJson.get("channel_type") == null) "" else adMessageJson.get("channel_type"))
    this.click_time = String.valueOf(if (adMessageJson.get("click_time") == null) 0 else adMessageJson.get("click_time")).toLong
    this.akey = String.valueOf(if (adMessageJson.get("akey") == null) "" else adMessageJson.get("akey"))
    this.token = String.valueOf(if (adMessageJson.get("token") == null) "" else adMessageJson.get("token"))
    this.company = String.valueOf(if (adMessageJson.get("company") == null) "" else adMessageJson.get("company"))
    this.auth_account = String.valueOf(if (adMessageJson.get("auth_account") == null) "" else adMessageJson.get("auth_account"))
    this.creative_id = String.valueOf(if (adMessageJson.get("creative_id") == null) "" else adMessageJson.get("creative_id"))
    this.channel_click_id = String.valueOf(if (adMessageJson.get("channel_click_id") == null) "" else adMessageJson.get("channel_click_id"))
    this.channel_account_id = String.valueOf(if (adMessageJson.get("channel_account_id") == null) "" else adMessageJson.get("channel_account_id"))
    this.zg_appid = String.valueOf(if (adMessageJson.get("zg_appid") == null) 0 else adMessageJson.get("zg_appid")).toLong
    this.hw_app_id = String.valueOf(if (adMessageJson.get("hw_app_id") == null) "" else adMessageJson.get("hw_app_id"))
    this.oaid = String.valueOf(if (adMessageJson.get("oaid") == null) "" else adMessageJson.get("oaid"))
  }

}
