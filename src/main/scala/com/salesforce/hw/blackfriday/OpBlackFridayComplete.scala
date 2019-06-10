package com.salesforce.hw.blackfriday

import com.salesforce.op.OpWorkflow
import com.salesforce.op.evaluators.Evaluators
import com.salesforce.op.features.FeatureBuilder
import com.salesforce.op.readers.DataReaders
import com.salesforce.op.stages.impl.regression.RegressionModelSelector
import com.salesforce.op.stages.impl.regression.RegressionModelsToTry.{OpGBTRegressor, OpRandomForestRegressor}
import com.salesforce.op.stages.impl.tuning.{DataCutter, DataSplitter}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Encoders, SparkSession}
import com.salesforce.op.features.types._

object OpSimpleRegressionComplete {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("..")
    implicit val spark = SparkSession.builder.config(conf).getOrCreate()

    implicit val srEncoder = Encoders.product[BlackFriday]

    val userId = FeatureBuilder.Real[BlackFriday].extract(_.userId.toReal).asPredictor
    //val userId = FeatureBuilder.Real[BlackFriday].extract(
    //  v => Option(v.userId).toRealNN(throw new Exception("Outcome cannot be null"))).asPredictor
    val productId = FeatureBuilder.Text[BlackFriday].extract(_.productId.toText).asPredictor
    val gender = FeatureBuilder.Real[BlackFriday].extract(_.gender.toReal).asPredictor
    val age = FeatureBuilder.Text[BlackFriday].extract(_.age.toText).asPredictor
    val occupation = FeatureBuilder.Text[BlackFriday].extract(_.occupation.toText).asPredictor
    val cityCategory = FeatureBuilder.Text[BlackFriday].extract(_.cityCategory.toText).asPredictor
    val stayInCurrentCityYears = FeatureBuilder.Text[BlackFriday].extract(_.stayInCurrentCityYears.toText).asPredictor
    val maritalStatus = FeatureBuilder.Integral[BlackFriday].extract(_.maritalStatus.toIntegral).asPredictor
    val productCategoryOne = FeatureBuilder.Integral[BlackFriday].extract(_.productCategoryOne.toIntegral).asPredictor
    val productCategoryTwo = FeatureBuilder.Integral[BlackFriday].extract(_.productCategoryTwo.toIntegral).asPredictor
    val productCategoryThree = FeatureBuilder.Integral[BlackFriday].extract(_.productCategoryThree.toIntegral).asPredictor
    val purchase = FeatureBuilder.RealNN[BlackFriday].extract(_.purchase.toRealNN).asResponse

    val trainFilePath = "./src/main/resources/BlackFridayDataset/export_dataframe_1.csv"

    val trainDataReader = DataReaders.Simple.csvCase[BlackFriday](
      path = Option(trainFilePath)
    )

    val features = Seq(userId, productId,gender, age, occupation, cityCategory, stayInCurrentCityYears, maritalStatus,productCategoryOne,
      productCategoryTwo, productCategoryThree).transmogrify()

    val randomSeed = 42L
    val splitter = DataSplitter(seed = randomSeed)

    val cutter = DataCutter(reserveTestFraction = 0.2, seed = randomSeed)

    val prediction = RegressionModelSelector
      .withCrossValidation(
        dataSplitter = Some(splitter), seed = randomSeed,
        modelTypesToUse = Seq(OpGBTRegressor, OpRandomForestRegressor)
      ).setInput(purchase, features).getOutput()

    val evaluator = Evaluators.Regression().setLabelCol(purchase).
      setPredictionCol(prediction)

    val workflow = new OpWorkflow().setResultFeatures(prediction, purchase).setReader(trainDataReader)
    val workflowModel = workflow.train()


    val dfScoreAndEvaluate = workflowModel.scoreAndEvaluate(evaluator)
    println(dfScoreAndEvaluate._1)
//    val dfScore = dfScoreAndEvaluate._1.withColumnRenamed("population-profit_3-stagesApplied_Prediction_00000000000f",
//      "predicted_profit")
//    val dfEvaluate = dfScoreAndEvaluate._2
//    println("Evaluate:\n" + dfEvaluate.toString())
//
//    dfScore.show(false)
//
//    val dfScoreMod = dfScore.rdd.map(x => x(2).toString.split("->")(1).dropRight(1).dropRight(1))
//    dfScoreMod.foreach(println)
//    dfScoreMod.saveAsTextFile("./output/simple_regression/predictions")
  }
}
