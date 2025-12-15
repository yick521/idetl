package com.zhugeio.etl.id.check

/**
  * Created by ziwudeng on 11/21/16.
  */

import com.github.fge.jackson.JsonLoader
import com.github.fge.jsonschema.core.report.ListProcessingReport
import com.github.fge.jsonschema.main.{JsonSchema, JsonSchemaFactory}

object CheckFstab {

  val basicSchema = getSchema("/basicSchema.json")

  def getSchema(schemaPath: String): JsonSchema = {

    val fstabSchema = JsonLoader.fromResource(schemaPath)

    val factory = JsonSchemaFactory.byDefault();

    val schema = factory.getJsonSchema(fstabSchema);

    schema
  }

  def checkBasic(jsonBasic: String): Either[scala.collection.Seq[String], Boolean] = {

    try {
      val result = basicSchema.validate(JsonLoader.fromString(jsonBasic)).asInstanceOf[ListProcessingReport]
      if (result.isSuccess) {
        return Right(true)
      } else {
        val iterator = result.iterator();
        var list = new scala.collection.mutable.ListBuffer[String]();
        while (iterator.hasNext) {
          list += iterator.next().getMessage
        }
        return Left(list)
      }
    } catch {
      case e: Exception => {
        return Left(List(e.getMessage))
      }
    }
  }

}
