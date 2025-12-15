package com.zhugeio.etl.id.service


import java.io.{BufferedReader, InputStreamReader}
import com.zhugeio.etl.id.{Config, ZGMessage}
import com.zhugeio.etl.id.adtoufang.service.{AdService, ConvertEventService, LidAndUserFirstEndService}
import com.zhugeio.etl.id.check.CheckService
import com.zhugeio.etl.id.log.IdLogger
import org.apache.commons.lang.time.StopWatch

import scala.collection.mutable
import scala.collection.mutable.ListBuffer


/**
  * Created by ziwudeng on 9/13/16.
  */
object MainService {
  var test = Config.getBoolean("is_test",false)
  var openToufangLog = Config.getProp(Config.OPEN_TOUFANG_LOG)
  var openToufang = Config.getProp(Config.OPEN_TOUFANG)

  def process(msgs: ListBuffer[ZGMessage]): Unit = {
    if (msgs.isEmpty) {
      return
    }
    val a = new java.util.Random().nextInt(10000)

    val watch = new StopWatch()

    watch.start()
    //TODO 初始化 批次处理前更新部分必须的缓存数据 会影响id模块消费速度
    //FrontCache.handleEventAttrAliasMap //更新事件属性别名
    if ("true".equals(openToufang)) {
      val tengxunWebKeySet = mutable.HashSet[String]()
      AdService.addSet(msgs, tengxunWebKeySet)
      if (tengxunWebKeySet.nonEmpty) {
        AdService.addMessage(msgs, tengxunWebKeySet)
      }
    }

    //todo 1.检查是否是json格式数据
    JsonService.parseJson(msgs)

//    IdLogger.info(s"timeC parse:${a}:" + watch.getTime())

    //todo 2.检查数据满足需求(文件夹 :/basicSchema.json)
    CheckService.checkMsgs(msgs)
//    IdLogger.info(s"timeC check:${a}:" + watch.getTime())

    //todo 3.根据ak 获取 appid
    //todo sql: select id from company_app where app_key = ? and is_delete = 0 and stop = 0 and id not in (select id from tmp_transfer where status = 2)
    AppService.parseAppId(msgs)
    IdLogger.info(s"timeC parseAppId:${a}:" + watch.getTime())

    //todo 4.给消息加上诸葛设备ID
    //     $zg_did : "d:" + appId + ":" + msg.data.get("usr")
    ZgDeviceIdService.handleDeviceids(msgs,a)
    IdLogger.info(s"timeC handleDeviceids:${a}:" + watch.getTime())

    //todo 5.添加 $zg_sid (回话开始时间)
    //  $sid -> $zg_sid   给事件(dt == (evt,ss,se,mkt,abp))属性添加uuid
    SessionIdUUIdService.addSessionIds(msgs)
    IdLogger.info(s"timeC addSessionIds:${a}:" + watch.getTime())

    //todo 6.给数据添加userid
    //  $zg_uid: "u:" + appId + ":" + $cuid($cuid 用户id )
    ZgUserIdService.handleUserids(msgs)
    IdLogger.info(s"timeC handleUserids:${a}:" + watch.getTime())

    //todo 7.给消息加上诸葛ID
    // $zg_zgid
    ZgIdService.handleZgIds(msgs)
    IdLogger.info(s"timeC handleZgIds:${a}:" + watch.getTime())

    if ("true".equals(openToufang)) {
      //TODO 投放四期：app端广告信息存ssdb ip+ua
      AdService.saveAppAdData(msgs)
      //todo 投放五期：新增 事件属性（lid）、用户属性(首次、末次)
      LidAndUserFirstEndService.handleLidAndUserFirstFollow(msgs)
      IdLogger.info(s"timeC handleEventPropIds:${a}:" + watch.getTime())
    }

    //todo 8.判断是否有设备信息
    DevicePropService.handleDevicePropIds(msgs)
    IdLogger.info(s"timeC handleDevicePropIds:${a}:" + watch.getTime())


    //TODO 9.添加虚拟属性
    VirtualPropService.handleVirtualProps(msgs)
    IdLogger.info(s"timeC handleVirtualProps:${a}:" + watch.getTime())

    //TODO 10.添加虚拟事件
    VirtualEventService.handleVirtualEvent(msgs)
    IdLogger.info(s"timeC handleVirtualEvent:${a}:" + watch.getTime())

    //todo 11.获取用户属性
    UserPropService.handleUserPropIds(msgs)
    IdLogger.info(s"timeC handleUserPropIds:${a}:" + watch.getTime())

    //todo 12.获取自定义事件属性
    EventService.handleEventPropIds(msgs)
    IdLogger.info(s"timeC handleEventPropIds:${a}:" + watch.getTime())

    if ("true".equals(openToufang)) {
      //todo 13、投放五期：查询回传表判断是否符合回传行为 (匹配深度回传事件并发kafka)
      ConvertEventService.convertMessage(msgs)
      // TODO 投放四期 根据事件id为事件属性添加utm数据：由原dw模块迁移过来
      AdService.addUtm(msgs)
    }
    if (!test) {
      //发kafka由dw模块落库 （事件、用户、设备）
      DistributeService.distribute(msgs)
    }

    IdLogger.info(s"timeC distribute:${a}:" + watch.getTime())

    watch.stop()
  }

  def main(args: Array[String]): Unit = {

    val msgs = new ListBuffer[ZGMessage]()
    val ir = new BufferedReader(new InputStreamReader(this.getClass.getClassLoader.getResourceAsStream("sample.txt")))
    var line: String = null
    val iterator = ir.lines().iterator()
    while (iterator.hasNext) {
      msgs += new ZGMessage("1", 1, 1, "", iterator.next())
      MainService.process(msgs)
    }
    ir.close()
    JsonService.saveJson(msgs)
    msgs.foreach(s => println(s))

  }
}
