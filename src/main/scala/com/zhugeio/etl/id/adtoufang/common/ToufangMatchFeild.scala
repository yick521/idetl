package com.zhugeio.etl.id.adtoufang.common

import scala.collection.mutable

object ToufangMatchFeild {
  //注意事项：因广告点击监测时第三方平台传递数据可能传递“0”、NULL、“”，过滤掉“”、0、NULL和他们的MD5小写值
  //巨量引擎:muid（需要自行截取，原值）	idfa、imei
  //问题：1、IOS 是否能取到 qaid_caa 做精确匹配  2、腾讯web的唯一id 取什么字段

  var originExcludeSet=new mutable.HashSet[String]
  var md5ExcludeSet=new mutable.HashSet[String]
  originExcludeSet.add("")
  originExcludeSet.add("0")
  originExcludeSet.add("NULL")
  originExcludeSet.add("null")
  originExcludeSet.add("(null)") //--新增
  md5ExcludeSet.add("cfcd208495d565ef66e7dff9f98764da")
  md5ExcludeSet.add("6c3e226b4d4795d518ab341b0824ec29")
  md5ExcludeSet.add("37a6259cc0c1dae299a7866489dff0bd")
  md5ExcludeSet.add("d41d8cd98f00b204e9800998ecf8427e")
  md5ExcludeSet.add("a4d2f177eb466a7d08f8f2b340b77129")
  //0:cfcd208495d565ef66e7dff9f98764da
  //NULL:6c3e226b4d4795d518ab341b0824ec29
  //null:37a6259cc0c1dae299a7866489dff0bd
  //:d41d8cd98f00b204e9800998ecf8427e
}
