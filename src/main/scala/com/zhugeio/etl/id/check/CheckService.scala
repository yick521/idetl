package com.zhugeio.etl.id.check

import java.util.function.Consumer

import com.zhugeio.etl.commons.ErrorMessageEnum
import com.zhugeio.etl.id.ZGMessage
import com.zhugeio.etl.id.log.IdLogger

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer;

/**
  * Created by ziwudeng on 11/21/16.
  */
object CheckService {

  def checkMsgs(msgs: ListBuffer[ZGMessage]): Unit = {

    msgs.asJava.parallelStream().forEach(
      new Consumer[ZGMessage] {
        override def accept(t: ZGMessage): Unit = {
          if (t.result != -1) {
            checkOne(t)
          }
        }
      }
    );

  }


  def checkOne(msg: ZGMessage): Unit = {
    val basic = Check.checkBasic(msg.rawData)
    if (basic.isLeft) {
      msg.result = -1;
      msg.error = basic.left.get.mkString("||")
      msg.errorCode += ErrorMessageEnum.BASIC_SCHEMA_FORMAT_NOT_MATCH.getErrorCode
      msg.errorDescribe = ErrorMessageEnum.BASIC_SCHEMA_FORMAT_NOT_MATCH.getErrorMessage
      msg.errData = msg.data
      IdLogger.errorFormat(msg.errorDescribe, msg.rawData)
      return
    }
  }

}
