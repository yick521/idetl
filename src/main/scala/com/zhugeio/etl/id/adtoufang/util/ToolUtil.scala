package com.zhugeio.etl.id.adtoufang.util


import org.apache.log4j.Logger

import java.time.format.DateTimeFormatter
import java.math.BigInteger
import java.net.URLDecoder
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.time.{LocalDateTime, ZoneOffset}
import java.util
import java.util.regex.Pattern

object ToolUtil {
  val logger = Logger.getLogger(this.getClass)
  var  DATE_TS_STR:String = "yyyy-MM-dd'T'HH:mm'Z'"
  var DATE_TS_FORMATTER =DateTimeFormatter.ofPattern(DATE_TS_STR)
  var  DATE_TS_STR_SE:String = "yyyy-MM-dd'T'HH:mm:ss'Z'"
  var DATE_TS_FORMATTER_SE =DateTimeFormatter.ofPattern(DATE_TS_STR_SE)

  def main(args: Array[String]): Unit = {
    //    println("0:" + getMD5Str("0"))
    //    println("NULL:" + getMD5Str("NULL"))
    //    println("null:" + getMD5Str("null"))
    //    println(":" + getMD5Str(""))
    //println(urlParseToMap("http://localhost:63342/%E6%99%AE%E9%80%9AJS%E6%95%B0%E6%8D%AE%E4%B8%8A%E4%BC%A0%E9%A1%B5%E9%9D%A2/data-upload.html?_ijt=lshbjf76s9tdat3661du68a6dq&_ij_reload=RELOAD_ON_SAVE"))
    //println(urlParseToMap("http://localhost:63342/%E6%99%AE%E9%80%9AJS%E6%95%B0%E6%8D%AE%E4%B8%8A%E4%BC%A0%E9%A1%B5%E9%9D%A2/data-upload.html?_ijt=lshbjf76s9tdat3661du68a6dq"))
    //852AC2E8-C6F9-45C4-A603-D3A97389649D
//    var str = "(null)"
//    println(getMD5Str(str))
//    println(dateUsStrToTimestamp("2023-01-04T09:59Z"))
//    println(dateUsStrToTimestamp("2020-08-04T17:17:00Z"))
//    println(getMD5Str("E699A6869AE640AB85044D5F226FA10D9137cb4e8df23bae677f2694e4c3b15b"))
//    println(getMD5Str("E699A6869AE640AB85044D5F226FA10D9137cb4e8df23bae677f2694e4c3b15b"))
    println(uaAnalysis("com.zhihu.android/Futureve/8.54.1 Mozilla/5.0 (Linux; Android 12; PEPM00 Build/SP1A.210812.016; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/97.0.4692.98 Mobile Safari/537.36"))
    println(uaAnalysis("Dalvik/2.1.0 (Linux; U; Android 4.4.2; PEPM00 Build/SP1A.210812.016)"))
    println(uaAnalysis("Mozilla/5.0 (Linux; Android 4.4.2; Luka Build/1.1.20) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/30.0.0.0 Mobile Safari/537.36"))
  }

  /**
    * 对字符串进行md5加密，返回 固定长度的字符 （32位），如：a167d52277c8cec9e5876c10dd43dfe0
    */
  def getMD5Str(str: String): String = {
    val md5 = MessageDigest.getInstance("MD5")
    val bytes = str.getBytes(StandardCharsets.UTF_8)
    md5.update(bytes, 0, bytes.length)
    return new BigInteger(1, md5.digest()).toString(16).toLowerCase()
  }

  /**
    * url参数解析为json
    * http://localhost:63342/%E6%99%AE%E9%80%9AJS%E6%95%B0%E6%8D%AE%E4%B8%8A%E4%BC%A0%E9%A1%B5%E9%9D%A2/data-upload.html?_ijt=lshbjf76s9tdat3661du68a6dq&_ij_reload=RELOAD_ON_SAVE
    */
  def urlParseToMap(url: String): util.Map[String, Object] = {
    var decodeUrl = URLDecoder.decode(url)
    val map: util.Map[String, Object] = new util.HashMap[String, Object]()
    if (decodeUrl.contains("?")) {
      val feilds = decodeUrl.split("\\?")
      val keyValues = feilds(1)
      if (keyValues.contains("&")) {
        val keyValuesArr = keyValues.split("&")
        keyValuesArr.foreach(keyValue => {
          if (keyValue.contains("=")) {
            val keyValueArr = keyValue.split("=")
            map.put(keyValueArr(0), if (keyValueArr.length > 1) keyValueArr(1) else "")
          }
        })
      } else if (keyValues.contains("=")) {
        val keyValueArr = keyValues.split("=")
        map.put(keyValueArr(0), if (keyValueArr.length > 1) keyValueArr(1) else "")
      }
    }
    map
  }

  /**
    * ua解析
    */
  def uaAnalysis(ua: String): String = {
    var ppp = "[^a-zA-Z0-9 ]";
    //TODO linux正则
    var pattern = Pattern.compile("linux;.*(android).([\\w.,/\\-]+)", Pattern.CASE_INSENSITIVE);
    //TODO ios正则
    if(!ua.contains("Android")) {
      pattern =Pattern.compile("(ip[honead]+)(?:.*os.([\\w.,/\\-]+).like|;\\sopera)",Pattern.CASE_INSENSITIVE)
    }
    var name = ""
    var version = ""
    var matcher = pattern.matcher(ua);
    if (matcher.find()) {
      name = matcher.group(1)
      version = matcher.group(2).replaceAll(ppp, ".")
//      System.out.println(i + " : " + matcher.group(0));
//      System.out.println("name : " + name);
//      System.out.println("version : " + version );
    }
    s"${name}:${version}"
  }

  /**
    * 日期解析 按US 的时区
    * 2023-01-04T08:11Z  2023-01-04 16:12:36
    * 2023-01-04T09:59Z  2023-01-04 17:59:57 差异8小时
    */
  def dateUsStrToTimestamp(dateStr:String):Long ={
    var time=0L
    try {
      if (dateStr.length > 17) {
        //time= LocalDateTime.parse(dateStr, DATE_TS_FORMATTER_SE).toInstant(ZoneOffset.of("+8")).toEpochMilli
        time= LocalDateTime.parse(dateStr, DATE_TS_FORMATTER_SE).toInstant(ZoneOffset.of("+0")).toEpochMilli
      }else{
        //time= LocalDateTime.parse(dateStr, DATE_TS_FORMATTER).toInstant(ZoneOffset.of("+8")).toEpochMilli
        time= LocalDateTime.parse(dateStr, DATE_TS_FORMATTER).toInstant(ZoneOffset.of("+0")).toEpochMilli
      }

    } catch {
      case e: Exception => {
        logger.info(dateStr+"转时间搓异常",e)
      }
    }
    return time
  }
}
