package com.zhugeio.etl.id.db

import cn.hutool.core.util.{CharsetUtil, HexUtil}
import cn.hutool.crypto.SmUtil

import java.util.Properties
import javax.sql._
import com.mchange.v2.c3p0.ComboPooledDataSource
import com.zhugeio.etl.id.Config
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.UserGroupInformation

import java.io.IOException
import java.security.PrivilegedAction


/**
  * Created by ziwudeng on 9/12/16.
  */


object Datasource {

  val front = get(Config.FRONT_DB_FILE, "front_db");
  var impalaDS = getImpalaDataSource(Config.IMPALA_DB_FILE, "impala_db");
  //val frontWeb = get(Config.FRONT_DB_FILE, "front_db_web");
  //val frontAdPg = get(Config.FRONT_DB_FILE, "dw_2-rs0");

  def get(propertyFile: String, dbPrefix: String): DataSource = {
    val properties = new Properties()
    properties.load(this.getClass.getClassLoader.getResourceAsStream(propertyFile))
    val ds = new ComboPooledDataSource
    ds.setDriverClass(properties.getProperty(dbPrefix + ".driverClass"));
    ds.setJdbcUrl(properties.getProperty(dbPrefix + ".url"));
    ds.setUser(properties.getProperty(dbPrefix + ".username"));
    var password = properties.getProperty(dbPrefix + ".password")
    val etype = Config.getProp(Config.ENCRYPTION_TYPE, "0").toInt
    if (etype == 2) {
      val sm4Key = Config.readFile(Config.getProp(Config.SM4_PRIKEY_PATH))
      val sm4 = SmUtil.sm4(HexUtil.decodeHex(sm4Key))
      password = sm4.decryptStr(password, CharsetUtil.CHARSET_UTF_8)
    }
    ds.setPassword(password)
    ds.setMinPoolSize(java.lang.Integer.parseInt(properties.getProperty(dbPrefix + ".minPoolSize")));
    ds.setMaxPoolSize(java.lang.Integer.parseInt(properties.getProperty(dbPrefix + ".maxPoolSize")));
    ds.setPreferredTestQuery(properties.getProperty(dbPrefix + ".testQuery"));
    ds.setMaxIdleTime(25000);
    println("初始化mysql连接: " + propertyFile + ",  " + properties.getProperty(dbPrefix + ".driverClass") + ",  " + properties.getProperty(dbPrefix + ".url") + ",  " + properties.getProperty(dbPrefix + ".testQuery"))
    ds.setTestConnectionOnCheckout(true);
    ds.getConnection.close()
    println(s"init db success:${propertyFile}:${dbPrefix}")
    ds
  }


  def getImpalaDataSource(propertyFile: String, dbPrefix: String): DataSource = {
    val properties = new Properties()
    properties.load(this.getClass.getClassLoader.getResourceAsStream(propertyFile))

    val openKerberos: String = properties.getProperty(dbPrefix + ".kerberos.open","false")
    if (openKerberos.equals("true")) {
      //impala 的kerberos登录认证
      val configuration = new Configuration
      val krb5FilePath = properties.getProperty(dbPrefix + ".krb5FilePath")
      val principal = properties.getProperty(dbPrefix + ".principal")
      val keytabPath = properties.getProperty(dbPrefix + ".keytabPath")
      // 通过系统设置参数设置krb5.conf
      System.setProperty("java.security.krb5.conf", krb5FilePath)
      // 指定kerberos 权限认证
      configuration.set("hadoop.security.authentication", "Kerberos")
      // 用 UserGroupInformation 类做kerberos认证
      UserGroupInformation.setConfiguration(configuration)
      try {
        // 用于刷新票据，当票据过期的时候自动刷新
        UserGroupInformation.getLoginUser.checkTGTAndReloginFromKeytab()
        // 通过 keytab 登录  参数1：认证主体   参数2：认证文件
        UserGroupInformation.loginUserFromKeytab(principal, keytabPath)
        //获取认证用户
        val loginUser = UserGroupInformation.getLoginUser()
        println("loginUser:" + loginUser);
        loginUser.doAs(new PrivilegedAction[DataSource] {
          override def run(): DataSource = {
            val ds = new ComboPooledDataSource
            ds.setDriverClass(properties.getProperty(dbPrefix + ".driverClass"))
            ds.setJdbcUrl(properties.getProperty(dbPrefix + ".kerberos.url"))
            ds.setPreferredTestQuery(properties.getProperty(dbPrefix + ".testQuery"))
            ds.setMaxIdleTime(25000)
            ds.setTestConnectionOnCheckout(true)
            ds.getConnection.close()
            println(s"init db success:${propertyFile}:${dbPrefix}")
            ds
          }
        })
      } catch {
        case e: IOException =>
          e.printStackTrace()
          throw new Exception("kerberos获取impala连接失败")
      }
    } else {
      val ds = new ComboPooledDataSource
      ds.setDriverClass(properties.getProperty(dbPrefix + ".driverClass"))
      ds.setJdbcUrl(properties.getProperty(dbPrefix + ".url"))
      ds.setPreferredTestQuery(properties.getProperty(dbPrefix + ".testQuery"))
      ds.setUser(properties.getProperty(dbPrefix + ".username"));
      ds.setPassword(properties.getProperty(dbPrefix + ".password"))
      ds.setMaxIdleTime(25000)
      ds.setTestConnectionOnCheckout(true)
      ds.getConnection.close()
      println(s"init db success:${propertyFile}:${dbPrefix}")
      ds
    }
  }


}