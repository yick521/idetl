package com.zhugeio.etl.id.log

import org.apache.commons.lang.exception.ExceptionUtils
import org.apache.log4j._

/**
  * Created by ziwudeng on 9/12/16.
  */
object IdLogger {

  val formatError = Logger.getLogger("com.zhugeio.etl.id.error.format");
  val dataError = Logger.getLogger("com.zhugeio.etl.id.error.data");

  val otherError = Logger.getLogger("com.zhugeio.etl.id.error.other");
  val msgLogger = Logger.getLogger("com.zhugeio.etl.id.msg");


  def errorFormat(errorMsg: String, msg: String): Unit = {
    formatError.error("\t" + errorMsg + "\t" + msg)
  }

  def errorData(errorMsg: String, msg: String): Unit = {
    dataError.error("\t" + errorMsg + "\t" + msg)
  }


  def error(msg: String, e: Exception): Unit = {
    otherError.error(msg, e)
  }

  def info(msg: String): Unit = {
    msgLogger.info(msg)
  }

}
