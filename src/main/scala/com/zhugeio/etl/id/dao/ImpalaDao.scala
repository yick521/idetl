package com.zhugeio.etl.id.dao

import com.zhugeio.etl.id.db.Datasource
import org.apache.log4j.Logger
import org.springframework.jdbc.core.support.JdbcDaoSupport

import java.sql.{ResultSet, Statement}
import java.util.Date
import scala.collection.mutable


/**
  * Created by hanqi on 17-6-20.
  */
object ImpalaDao extends JdbcDaoSupport {
  val logger1 = Logger.getLogger(this.getClass)
  this.setDataSource(Datasource.impalaDS)

  /**
    * 插入
    */
  def insert(bakTableName: String, tableName: String): Unit = {
    val sql = "insert into " + bakTableName + " select * from " + tableName
    executeSQL(sql)
  }

  def deleteTable(table: String): Unit = {
    val sql = "delete from " + table
    executeSQL(sql)
  }

  def querySQLForOne(sql: String,columnName:String):String = {
    val list = this.getJdbcTemplate.queryForList(sql)
    logger1.info(sql)
    if (list.size() > 0) {
      return String.valueOf(list.get(0).get(columnName))
    }
    return  null
  }

  def executeSQL(sql: String) = {
    this.getJdbcTemplate.execute(sql)
    logger1.info(sql)
  }

  def queryList(sql: String):  java.util.List[java.util.Map[String, AnyRef]] = {
    val list = this.getJdbcTemplate.queryForList(sql);
    list
  }

  def getDiviceIds(stmt :Statement,appId: Int,begin:Long): mutable.HashSet[String] = {
    val deviceIds = mutable.HashSet[String]()
    //    val sql = s"select device_id,device_md5  from b_device_${appId} OFFSET 10 LIMIT 20"
    val sql = s"SELECT device_id,device_md5 FROM b_device_${appId} t  WHERE  last_update_date > ${begin}"

    println(sql)
    try {
      val rs: ResultSet = stmt.executeQuery(sql)

      // Extract data from result set
      println(appId+"Printing results..."+new Date())
      while (rs.next()) {
        // Retrieve by column name
        val id = rs.getString("device_id").toInt
        val deviceId = rs.getString("device_md5")

        // Display values
        //        println(s"deviceId: $deviceId -> $id")
        //        val key = "d:" + appId + ":" + deviceId + (if(id % 2 == 0) "1" else "")
        //        val key = "d:" + appId + ":" + deviceId
        deviceIds += deviceId
      }
      rs.close()

    } catch {
      case e: Exception => e.printStackTrace()
    }
    deviceIds
  }

  def queryDiviceIds(stmt :Statement,appId: Int,dmd5list:mutable.HashSet[String]): mutable.HashSet[String] = {
    val deviceIds = mutable.HashSet[String]()
    val param = dmd5list.toList.map(dmd5 =>{"\""+dmd5+"\""}).mkString(",")
    //    val sql = s"select device_id,device_md5  from b_device_${appId} OFFSET 10 LIMIT 20"
    val sql = s"SELECT device_id FROM b_device_${appId} t   WHERE device_md5 in (${param}) "
    println(sql)

    try {
      val rs: ResultSet = stmt.executeQuery(sql)

      // Extract data from result set
      println(appId+"Printing results..."+new Date())
      while (rs.next()) {
        // Retrieve by column name
        val id = rs.getString("device_id").toInt

        deviceIds += id.toString
      }
      rs.close()

    } catch {
      case e: Exception => e.printStackTrace()
    }
    deviceIds
  }

  def main(args: Array[String]): Unit = {
    val sql = "select device_id,device_md5  from b_device_86255 limit 10"
    val list =  this.getJdbcTemplate.queryForList(sql);
    println("list:",list)


  }

}
