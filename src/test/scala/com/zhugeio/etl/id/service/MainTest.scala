package com.zhugeio.etl.id.service

import com.zhugeio.tool.commons.JsonUtil

import java.io.{BufferedReader, InputStreamReader}
import com.zhugeio.etl.id.ZGMessage
import org.junit._

import java.text.SimpleDateFormat
import java.util.Date
import scala.collection.mutable.ListBuffer

/**
  * Created by ziwudeng on 10/22/16.
  */
class MainTest {

  @Test
  def testMain(): Unit = {
    var msgs = new ListBuffer[ZGMessage]()
    val ir = new BufferedReader(new InputStreamReader(this.getClass.getClassLoader.getResourceAsStream("sample")))
    var line: String = null
    val iterator = ir.lines().iterator()
    while (iterator.hasNext) {
      msgs += new ZGMessage("1", 1, 1, "", iterator.next())
    }
    MainService.process(msgs)
    msgs.foreach(msg => {
      val json = JsonUtil.toJson(msg.data);
      println(json)
    })
    ir.close()
    //    JsonService.saveJson(msgs)
    //    msgs.foreach(s => println(s.json))
  }


  @Test
  def getNowDateString() = {
    val formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    println(formatter.format(new Date()))
  }
}
