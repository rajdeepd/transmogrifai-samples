package com.salesforce.hw.regression

import java.io._

import com.google.gson.JsonParser

import scala.io.Source

object SimpleRegressionModelEval {
  val filename = "/tmp/sr-eval/schema/part-00000"
  val outputFile = "./output/simple_regression/modeleval/output.csv"

  val fileContents = Source.fromFile(filename).getLines.mkString

  def main(args: Array[String]): Unit = {

      val pw = new PrintWriter(new File(outputFile ))
      pw.write("modelName,modelType,maxBins,maxDepth,numTrees,metricValue\n")
      val jsonStringAsObject= new JsonParser().parse(fileContents).getAsJsonObject
      val fields = jsonStringAsObject.get("fields").getAsJsonArray.get(2)
      val validationResults = fields.getAsJsonObject.get("metadata").getAsJsonObject.get("summary").
        getAsJsonObject.get("ValidationResults").getAsJsonArray
      var i = 0
      for( i <- 1 to validationResults.size() -1){
        val model = validationResults.get(i).getAsJsonObject

        val modelName = validationResults.get(i).getAsJsonObject.get("ModelName").
          toString.replaceAll("\"", "")
        val modelType = validationResults.get(i).getAsJsonObject.get("ModelType").toString
          .substring(1,
            validationResults.get(i).getAsJsonObject.get("ModelType").toString.length -1).
          replace("\\", "")
        val modelParams = validationResults.get(i).getAsJsonObject.get("ModelParameters").getAsJsonObject
        val maxBins = modelParams.get("maxBins")
        val maxDepth = modelParams.get("maxDepth")
        val numTrees = modelParams.get("numTrees")
        //maxBins
        //numTrees

        val metricValuesRaw = validationResults.get(i).getAsJsonObject.get("MetricValues").
          getAsJsonArray().get(1)
        val metricValues_ = metricValuesRaw.toString.substring(1, metricValuesRaw.toString.length -1).
          replace("\\", "")
        val metricValue = new JsonParser().parse(metricValues_).getAsJsonObject.get("value")
        val output = modelName + "," +  modelType + "," + maxBins + "," + maxDepth + "," + numTrees + "," + metricValue
        println(output)
        pw.write(output + "\n")
      }
    pw.close
  }
}
