package com.zhugeio.etl.id.adtoufang.common

import com.alibaba.fastjson.JSONObject

import java.util


class AdMessage {

  var zg_appid = 0L //应用id
  var lid = 0L //链接id
  var csite = 0L //广告投放位置
  var ados = -1 //操作系统平台
  var channel_id = 0L //渠道id
  var channel_account_id = "" //广告主id 由数值型改成字符串
  var convert_id = 0L //转化id
  var channel_campaign_id = 0L //广告组id
  var ctype = 0L //创意样式
  var channel_ad_id = 0L //广告创意ID
  var click_time = 0L //广告点击事件的时间
  var channel_adgroup_id = 0L //广告计划id
  var ct = 0L //ct

  var lname = "" //连接名称
  var utm_campaign = "" //活动名称
  var utm_source = "" //广告来源
  var utm_medium = "" //广告媒介
  var utm_term = "" //关键词
  var utm_content = "" //广告内容
  var channel_ad_name = "" //广告创意名称
  var channel_campaign_name = "" //广告组名称
  var ip = "" //用户终端的公共ip地址
  var ua = "" //用户代理
  var mac = "" //移动设备的mac地址,去掉：
  var mac1 = "" //移动设备的mac地址
  var callback_url = "" //回传接口URL
  var geo = "" //位置信息
  var imei = "" //安卓设备ID md5
  var idfa = "" //iOS 6+的设备ID
  var idfa_md5 = "" //iOS 6+的设备ID md5
  var sl = "" //这次请求的语言
  var model = "" //手机型号
  var union_site = "" //对外广告位编码
  var android_id = "" //安卓原值id的md5
  var oaid = "" //安卓Q及更高版本的设备号
  var oaid_md5 = "" //安卓Q及更高版本的设备号的md5
  var channel_adgroup_name = "" //广告计划名称
  var requers_id = "" //请求下发ID
  var app_key = "" //应用key
  var muid = "" //muid

  var callback = ""
  var channel_type:String=""//渠道类型必须
  var push_type:String=""//推广类型必须
  var akey:String="" //百度账号akey(监测链接里获取)
  var token:String=""//百度账号token(监测链接里获取)

  var channel_keyword_id = 0L
  var qaid_caa=""
  var ip_ua_key:String =""
  var muid_key:String =""

  var idfa_key:String =""
  var imei_key:String =""
  var android_id_key:String =""
  var oaid_key:String =""
  var channel_click_id_key:String =""

  var company:String =""
  var auth_account:String =""
  //idfa_key  imei_key  android_id_key oaid_key channel_click_id_key

  // 华为广告 新增字段
  var hw_app_id:String ="" //华为商城应用id
  //vivo新增字段
  var creative_id:String = ""

  var is_delete:String ="false"
  var channel_click_id:String =""
  def toJsonString(): String = {
    "{\"zg_appid\":\""+zg_appid+"\""+
      ",\"lid\":\""+lid+"\""+
      ",\"csite\":\""+csite+"\""+
      ",\"ados\":\""+ados+"\""+
      ",\"channel_id\":\""+channel_id+"\""+
      ",\"channel_account_id\":\""+channel_account_id+"\""+
      ",\"convert_id\":\""+convert_id+"\""+
      ",\"channel_campaign_id\":\""+channel_campaign_id+"\""+
      ",\"ctype\":\""+ctype+"\""+
      ",\"channel_ad_id\":\""+channel_ad_id+"\""+
      ",\"click_time\":\""+click_time+"\""+
      ",\"channel_adgroup_id\":\""+channel_adgroup_id+"\""+
      ",\"ct\":\""+ct+"\""+
      ",\"lname\":\""+lname+"\""+
      ",\"utm_campaign\":\""+utm_campaign+"\""+
      ",\"utm_source\":\""+utm_source+"\""+
      ",\"utm_medium\":\""+utm_medium+"\""+
      ",\"utm_term\":\""+utm_term+"\""+
      ",\"utm_content\":\""+utm_content+"\""+
      ",\"channel_ad_name\":\""+channel_ad_name+"\""+
      ",\"channel_campaign_name\":\""+channel_campaign_name+"\""+
      ",\"ip\":\""+ip+"\""+
      ",\"ua\":\""+ua+"\""+
      ",\"mac\":\""+mac+"\""+
      ",\"mac1\":\""+mac1+"\""+
      ",\"callback_url\":\""+callback_url+"\""+
      ",\"geo\":\""+geo+"\""+
      ",\"imei\":\""+imei+"\""+
      ",\"idfa\":\""+idfa+"\""+
      ",\"idfa_md5\":\""+idfa_md5+"\""+
      ",\"sl\":\""+sl+"\""+
      ",\"model\":\""+model+"\""+
      ",\"union_site\":\""+union_site+"\""+
      ",\"android_id\":\""+android_id+"\""+
      ",\"oaid\":\""+oaid+"\""+
      ",\"oaid_md5\":\""+oaid_md5+"\""+
      ",\"channel_adgroup_name\":\""+channel_adgroup_name+"\""+
      ",\"requers_id\":\""+requers_id+"\""+
      ",\"app_key\":\""+app_key+"\""+
      ",\"muid\":\""+muid+"\"" +
      ",\"callback\":\""+callback+"\""+
      ",\"channel_type\":\""+channel_type+"\""+
      ",\"push_type\":\""+push_type+"\""+
      ",\"akey\":\""+akey+"\""+
      ",\"token\":\""+token+"\""+
      ",\"channel_keyword_id\":\""+channel_keyword_id+"\""+
      ",\"qaid_caa\":\""+qaid_caa+"\""+
      ",\"ip_ua_key\":\""+ip_ua_key+"\""+
      ",\"muid_key\":\""+muid_key+"\""+
      ",\"idfa_key\":\""+idfa_key+"\""+
      ",\"imei_key\":\""+imei_key+"\""+
      ",\"android_id_key\":\""+android_id_key+"\""+
      ",\"oaid_key\":\""+oaid_key+"\""+
      ",\"channel_click_id_key\":\""+channel_click_id_key+"\""+
      ",\"is_delete\":\""+is_delete+"\""+
      ",\"company\":\""+company+"\""+
      ",\"auth_account\":\""+auth_account+"\""+
      ",\"creative_id\":\""+creative_id+"\""+
      ",\"hw_app_id\":\""+hw_app_id+"\""+
      ",\"channel_click_id\":\""+channel_click_id+"\""+
      "}"

    //idfa_key  imei_key  android_id_key oaid_key channel_click_id_key
  }

  def setFeilds(props:java.util.Map[String, java.lang.Object]): Unit ={
    this.csite = String.valueOf(if (props.get("$csite") == null) 0 else props.get("$csite")).toLong
    //app端广告信息中 $ados ： android 0 、 ios 1 、其他 3
    this.ados = String.valueOf(if (props.get("$ados") == null) 3 else props.get("$ados")).toInt
    this.channel_id = String.valueOf(if (props.get("$channel_id") == null) 0 else props.get("$channel_id")).toLong
    this.channel_account_id = String.valueOf(if (props.get("$channel_account_id") == null) 0 else props.get("$channel_account_id"))
    this.convert_id = String.valueOf(if (props.get("$convert_id") == null) 0 else props.get("$convert_id")).toLong
    this.channel_campaign_id = String.valueOf(if (props.get("$channel_campaign_id") == null) 0 else props.get("$channel_campaign_id")).toLong
    this.ctype = String.valueOf(if (props.get("$ctype") == null) 0 else props.get("$ctype")).toLong
    this.channel_ad_id = String.valueOf(if (props.get("$channel_ad_id") == null) 0 else props.get("$channel_ad_id")).toLong



    this.channel_adgroup_id = String.valueOf(if (props.get("$channel_adgroup_id") == null) 0 else props.get("$channel_adgroup_id")).toLong
    this.lname = String.valueOf(if (props.get("$lname") == null) "" else props.get("$lname"))
    this.utm_campaign = String.valueOf(if (props.get("$utm_campaign") == null) "" else props.get("$utm_campaign"))
    this.utm_source = String.valueOf(if (props.get("$utm_source") == null) "" else props.get("$utm_source"))
    this.utm_medium = String.valueOf(if (props.get("$utm_medium") == null) "" else props.get("$utm_medium"))
    this.utm_term = String.valueOf(if (props.get("$utm_term") == null) "" else props.get("$utm_term"))
    this.utm_content = String.valueOf(if (props.get("$utm_content") == null) "" else props.get("$utm_content"))
    this.channel_ad_name = String.valueOf(if (props.get("$channel_ad_name") == null) "" else props.get("$channel_ad_name"))
    this.channel_campaign_name = String.valueOf(if (props.get("$channel_campaign_name") == null) "" else props.get("$channel_campaign_name"))
    this.ip = String.valueOf(if (props.get("$ip") == null) "" else props.get("$ip"))
    this.ua = String.valueOf(if (props.get("$ua") == null) "" else props.get("$ua"))
    this.mac = String.valueOf(if (props.get("$mac") == null) "" else props.get("$mac"))
    this.mac1 = String.valueOf(if (props.get("$mac1") == null) "" else props.get("$mac1"))
    this.callback_url = String.valueOf(if (props.get("$callback_url") == null) "" else props.get("$callback_url"))
    this.geo = String.valueOf(if (props.get("$geo") == null) "" else props.get("$geo"))
    this.imei = String.valueOf(if (props.get("$imei") == null) "" else props.get("$imei"))
    this.idfa = String.valueOf(if (props.get("$idfa") == null) "" else props.get("$idfa"))
    this.idfa_md5 = String.valueOf(if (props.get("$idfa_md5") == null) "" else props.get("$idfa_md5"))
    this.sl = String.valueOf(if (props.get("$sl") == null) "" else props.get("$sl"))
    this.model = String.valueOf(if (props.get("$model") == null) "" else props.get("$model"))
    this.union_site = String.valueOf(if (props.get("$union_site") == null) "" else props.get("$union_site"))
    this.android_id = String.valueOf(if (props.get("$android_id") == null) "" else props.get("$android_id"))
    this.oaid = String.valueOf(if (props.get("$oaid") == null) "" else props.get("$oaid"))
    this.oaid_md5 = String.valueOf(if (props.get("$oaid_md5") == null) "" else props.get("$oaid_md5"))
    this.channel_adgroup_name = String.valueOf(if (props.get("$channel_adgroup_name") == null) "" else props.get("$channel_adgroup_name"))
    this.requers_id = String.valueOf(if (props.get("$requers_id") == null) "" else props.get("$requers_id"))
    this.muid = String.valueOf(if (props.get("$muid") == null) "" else props.get("$muid"))
    this.callback = String.valueOf(if (props.get("$callback") == null) "" else props.get("$callback"))
    this.channel_type = String.valueOf(if (props.get("$channel_type") == null) "" else props.get("$channel_type"))
    this.push_type = String.valueOf(if (props.get("$push_type") == null) "" else props.get("$push_type"))
    this.channel_keyword_id = String.valueOf(if (props.get("$channel_keyword_id") == null) 0 else props.get("$channel_keyword_id")).toLong
    this.qaid_caa = String.valueOf(if (props.get("$qaid_caa") == null) "" else props.get("$qaid_caa"))
    this.akey = String.valueOf(if (props.get("$akey") == null) "" else props.get("$akey"))
    this.token = String.valueOf(if (props.get("$token") == null) "" else props.get("$token"))
    this.company = String.valueOf(if (props.get("$company") == null) "" else props.get("$company"))
    this.auth_account = String.valueOf(if (props.get("$auth_account") == null) "" else props.get("$auth_account"))
    this.creative_id = String.valueOf(if (props.get("$creative_id") == null) "" else props.get("$creative_id"))
    this.channel_click_id = String.valueOf(if (props.get("$channel_click_id") == null) "" else props.get("$channel_click_id"))
    this.hw_app_id = String.valueOf(if (props.get("$hw_app_id") == null) "" else props.get("$hw_app_id"))



  }

  /**
    * 为web端广告信息赋值
    */
  def setFeildsWithout(props:java.util.Map[String, java.lang.Object]): Unit ={
    this.csite = String.valueOf(if (props.get("csite") == null) 0 else props.get("csite")).toLong
    //app端广告信息中 ados ： android 0 、 ios 1 、其他 3
    this.ados = String.valueOf(if (props.get("ados") == null) 3 else props.get("ados")).toInt
    this.channel_id = String.valueOf(if (props.get("channel_id") == null) 0 else props.get("channel_id")).toLong
    this.channel_account_id = String.valueOf(if (props.get("channel_account_id") == null) 0 else props.get("channel_account_id"))
    this.convert_id = String.valueOf(if (props.get("convert_id") == null) 0 else props.get("convert_id")).toLong
    this.channel_campaign_id = String.valueOf(if (props.get("channel_campaign_id") == null) 0 else props.get("channel_campaign_id")).toLong
    this.ctype = String.valueOf(if (props.get("ctype") == null) 0 else props.get("ctype")).toLong
    this.channel_ad_id = String.valueOf(if (props.get("channel_ad_id") == null) 0 else props.get("channel_ad_id")).toLong



    this.channel_adgroup_id = String.valueOf(if (props.get("channel_adgroup_id") == null) 0 else props.get("channel_adgroup_id")).toLong
    this.lname = String.valueOf(if (props.get("lname") == null) "" else props.get("lname"))
    this.utm_campaign = String.valueOf(if (props.get("utm_campaign") == null) "" else props.get("utm_campaign"))
    this.utm_source = String.valueOf(if (props.get("utm_source") == null) "" else props.get("utm_source"))
    this.utm_medium = String.valueOf(if (props.get("utm_medium") == null) "" else props.get("utm_medium"))
    this.utm_term = String.valueOf(if (props.get("utm_term") == null) "" else props.get("utm_term"))
    this.utm_content = String.valueOf(if (props.get("utm_content") == null) "" else props.get("utm_content"))
    this.channel_ad_name = String.valueOf(if (props.get("channel_ad_name") == null) "" else props.get("channel_ad_name"))
    this.channel_campaign_name = String.valueOf(if (props.get("channel_campaign_name") == null) "" else props.get("channel_campaign_name"))
    this.ip = String.valueOf(if (props.get("ip") == null) "" else props.get("ip"))
    this.ua = String.valueOf(if (props.get("ua") == null) "" else props.get("ua"))
    this.mac = String.valueOf(if (props.get("mac") == null) "" else props.get("mac"))
    this.mac1 = String.valueOf(if (props.get("mac1") == null) "" else props.get("mac1"))
    this.callback_url = String.valueOf(if (props.get("callback_url") == null) "" else props.get("callback_url"))
    this.geo = String.valueOf(if (props.get("geo") == null) "" else props.get("geo"))
    this.imei = String.valueOf(if (props.get("imei") == null) "" else props.get("imei"))
    this.idfa = String.valueOf(if (props.get("idfa") == null) "" else props.get("idfa"))
    this.idfa_md5 = String.valueOf(if (props.get("idfa_md5") == null) "" else props.get("idfa_md5"))
    this.sl = String.valueOf(if (props.get("sl") == null) "" else props.get("sl"))
    this.model = String.valueOf(if (props.get("model") == null) "" else props.get("model"))
    this.union_site = String.valueOf(if (props.get("union_site") == null) "" else props.get("union_site"))
    this.android_id = String.valueOf(if (props.get("android_id") == null) "" else props.get("android_id"))
    this.oaid = String.valueOf(if (props.get("oaid") == null) "" else props.get("oaid"))
    this.oaid_md5 = String.valueOf(if (props.get("oaid_md5") == null) "" else props.get("oaid_md5"))
    this.channel_adgroup_name = String.valueOf(if (props.get("channel_adgroup_name") == null) "" else props.get("channel_adgroup_name"))
    this.requers_id = String.valueOf(if (props.get("requers_id") == null) "" else props.get("requers_id"))
    this.muid = String.valueOf(if (props.get("muid") == null) "" else props.get("muid"))
    this.callback = String.valueOf(if (props.get("callback") == null) "" else props.get("callback"))
    this.channel_type = String.valueOf(if (props.get("channel_type") == null) "" else props.get("channel_type"))
    this.push_type = String.valueOf(if (props.get("push_type") == null) "" else props.get("push_type"))
    this.channel_keyword_id = String.valueOf(if (props.get("channel_keyword_id") == null) 0 else props.get("channel_keyword_id")).toLong
    this.qaid_caa = String.valueOf(if (props.get("qaid_caa") == null) "" else props.get("qaid_caa"))
    this.akey = String.valueOf(if (props.get("akey") == null) "" else props.get("akey"))
    this.token = String.valueOf(if (props.get("token") == null) "" else props.get("token"))
    this.company = String.valueOf(if (props.get("company") == null) "" else props.get("company"))
    this.auth_account = String.valueOf(if (props.get("auth_account") == null) "" else props.get("auth_account"))
    this.creative_id = String.valueOf(if (props.get("creative_id") == null) "" else props.get("creative_id"))
    this.channel_click_id = String.valueOf(if (props.get("channel_click_id") == null) "" else props.get("channel_click_id"))

    this.hw_app_id = String.valueOf(if (props.get("hw_app_id") == null) "" else props.get("hw_app_id"))


  }
}
