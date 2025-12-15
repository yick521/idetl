package com.zhugeio.etl.id

import com.zhugeio.etl.id.adtoufang.common.ToufangMatchFeild
import org.apache.log4j.PropertyConfigurator
import org.junit.Test

/**
  * Created by ziwudeng on 11/22/16.
  */
object TestInit {

  def init(): Unit = {
    //System.setProperty("spark.yarn.app.container.log.dir", "/Users/ziwudeng/IdeaProjects/etl-id")
    //PropertyConfigurator.configure(this.getClass.getResourceAsStream("/log4j_id.properties"))
   // println(ToufangMatchFeild.originExcludeSet)
  }
}
