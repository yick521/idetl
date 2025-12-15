package com.zhugeio.etl.id.check
import java.io.{BufferedReader, InputStreamReader}
import java.nio.charset.StandardCharsets

import com.zhugeio.etl.id.log.IdLogger
import org.json._
import org.everit.json.schema._
import org.everit.json.schema.loader.SchemaLoader

import scala.collection.JavaConversions._

/**
  * Created by ziwudeng on 11/21/16.
  */
object Check {

  val basicSchema = getSchema("/basicSchema.json")

  def getSchema(schemaPath: String): Schema = {

    val reader = new BufferedReader(new InputStreamReader(this.getClass.getResourceAsStream(schemaPath),StandardCharsets.UTF_8))
    val sb = new StringBuilder();
    var line:String = reader.readLine()
    while(line != null){
      sb.append(line+"\n")
      line = reader.readLine()
    }
    reader.close()
    val rawSchema = new JSONObject(new JSONTokener(sb.toString()));
    SchemaLoader.load(rawSchema);

  }

  def checkBasic(jsonBasic: String): Either[scala.collection.Seq[String], Boolean] = {

      try {
        basicSchema.validate(new JSONObject(new JSONTokener(jsonBasic)));
        return Right(true)
      } catch {
        case e: ValidationException => {
          if(e.getViolationCount==1){
            return Left(List(e.getMessage))
          }else{
            return Left(e.getCausingExceptions.toList.map(m=>m.getMessage))
          }
        }
        case e1: Exception => {
          return Left(List(e1.getMessage))
        }
      }
    }

}
