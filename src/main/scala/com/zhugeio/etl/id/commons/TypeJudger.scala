package com.zhugeio.etl.id.commons

/**
  * Created by ziwudeng on 9/14/16.
  */
object TypeJudger {

  def getObjectType(obj:java.lang.Object):String = {
    if(obj == null){
      return "null";
    }else if(obj.isInstanceOf[java.lang.Number]){
      return "number"
    }else if(obj.isInstanceOf[java.lang.String]){
      return "string"
    }else {
      return "object"
    }
  }


  def getIntType(typeString:String): Integer ={
      if(typeString == "number"){
        return 2
      }else{
        return 1
      }
  }
}
