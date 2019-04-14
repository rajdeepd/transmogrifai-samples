package com.salesforce.hw.primaindians

import scala.io.Source
import com.google.gson.Gson
import com.google.gson.JsonObject
import com.google.gson.JsonParser

import java.io._

object ModelEval {
  val filename = "/tmp/pi-eval/schema/part-00000"
  val outputFile = "./output/primaindians/modeleval/output.csv"

  val fileContents = Source.fromFile(filename).getLines.mkString

  def main(args: Array[String]): Unit = {

      val pw = new PrintWriter(new File(outputFile ))
      pw.write("modelName, modelType, regularizationParam, maxDepth, metricValue\n")
      val jsonStringAsObject= new JsonParser().parse(fileContents).getAsJsonObject
      val fields = jsonStringAsObject.get("fields").getAsJsonArray.get(2)
      val validationResults = fields.getAsJsonObject.get("metadata").getAsJsonObject.get("summary").
        getAsJsonObject.get("ValidationResults").getAsJsonArray
      var i = 0
      for( i <- 1 to validationResults.size() -1){
        val model = validationResults.get(i).getAsJsonObject

        val modelName = validationResults.get(i).getAsJsonObject.get("ModelName").
          toString.replaceAll("\"", "")
        val modelType = validationResults.get(i).getAsJsonObject.get("ModelType")
        val modelParams = validationResults.get(i).getAsJsonObject.get("ModelParameters").getAsJsonObject
        val regParam = modelParams.get("regParam")
        val maxDepth = modelParams.get("maxDepth")

        val metricValuesRaw = validationResults.get(i).getAsJsonObject.get("MetricValues").
          getAsJsonArray().get(1)
        val metricValues_ = metricValuesRaw.toString.substring(1, metricValuesRaw.toString.length -1).
          replace("\\", "")
        val metricValue = new JsonParser().parse(metricValues_).getAsJsonObject.get("value")
        val output = modelName + ", " +  modelType + ", " + regParam + ", " + maxDepth + ", " + metricValue
        println(output)
        pw.write(output + "\n")
      }
    pw.close
  }
}
