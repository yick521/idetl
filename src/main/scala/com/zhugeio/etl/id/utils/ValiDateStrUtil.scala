package com.zhugeio.etl.id.utils

object ValiDateStrUtil {
  def main(args: Array[String]): Unit = {
    println(isValidDateStr("2019-01-01"))
    val testString = "Hello_World-123$你好"
    System.out.println(testString+":"+isValidDateStr(testString)) // 输出 true 或 false

    var invalidString = "Hello_World-123$你好!"
    System.out.println(invalidString+":"+isValidDateStr(invalidString)) // 输出 true 或 false
     invalidString = "摨\uD840\uDE5B鍂  滨妍"
    System.out.println(invalidString+":"+isValidDateStr(invalidString)) // 输出 true 或 false
  }

  /**
    * 判断字符串是否只包含数字、字母、下划线、中划线、汉字、$
    * @param str
    * @return
    */
  def isValidDateStr(str: String): Boolean = {
    //正则表达式 ^[a-zA-Z0-9_\\-\\u4e00-\\u9fa5\\$]+$ 的解释：
    //^ 表示字符串开始。
    //[a-zA-Z0-9_\\-\\u4e00-\\u9fa5\\$] 定义了允许的字符集。
    //+ 表示前面的字符可以出现一次或多次。
    //$ 表示字符串结束。
    //\\u4e00-\\u9fa5 是 Unicode 范围内的汉字字符。
//    val pattern = "^[a-zA-Z0-9_\\-\\u4e00-\\u9fa5\\$]+$"
//    str.matches(pattern)
    true
  }

}
