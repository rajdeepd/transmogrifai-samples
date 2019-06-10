package com.salesforce.hw.regression.housing_prices.archive

import com.salesforce.hw.regression.housing_prices.archive
import com.salesforce.op.OpWorkflow
import com.salesforce.op.evaluators.Evaluators
import com.salesforce.op.features.FeatureBuilder
import com.salesforce.op.features.types._
import com.salesforce.op.readers.DataReaders
import com.salesforce.op.stages.impl.regression.RegressionModelSelector
import com.salesforce.op.stages.impl.regression.RegressionModelsToTry.{OpGBTRegressor, OpRandomForestRegressor}
import com.salesforce.op.stages.impl.tuning.{DataCutter, DataSplitter}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Encoders, SparkSession}

object OpHousingPricesComplete {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("..")
    implicit val spark = SparkSession.builder.config(conf).getOrCreate()

    implicit val srEncoder = Encoders.product[archive.HousingPrices]

    val lotFrontage = FeatureBuilder.Real[archive.HousingPrices].extract(_.lotFrontage.toReal).asPredictor
    val area = FeatureBuilder.Integral[archive.HousingPrices].extract(_.area.toIntegral).asPredictor
    val lotShape = FeatureBuilder.Text[archive.HousingPrices].extract(_.lotShape.toText).asPredictor
    val salePrice = FeatureBuilder.RealNN[archive.HousingPrices].extract(_.salePrice.toRealNN).asResponse


    val features1 = Seq(lotFrontage,area, lotShape).transmogrify()

    val trainFilePath = "./src/main/resources/HousingPricesDataset/train_lf_la_ls.csv"

    val trainDataReader = DataReaders.Simple.csvCase[archive.HousingPrices](
      path = Option(trainFilePath)
    )

    val features = Seq(lotFrontage,area,lotShape).transmogrify()
    val randomSeed = 42L
    val splitter = DataSplitter(seed = randomSeed)

    val cutter = DataCutter(reserveTestFraction = 0.2, seed = randomSeed)

    val prediction1 = RegressionModelSelector
      .withCrossValidation(
        dataSplitter = Some(splitter), seed = randomSeed,
        modelTypesToUse = Seq(OpGBTRegressor, OpRandomForestRegressor)
      ).setInput(salePrice,features1).getOutput()

    val evaluator = Evaluators.Regression().setLabelCol(salePrice).
      setPredictionCol(prediction1)

    val workflow = new OpWorkflow().setResultFeatures(prediction1, salePrice).setReader(trainDataReader)
    val workflowModel = workflow.train()


    val dfScoreAndEvaluate = workflowModel.scoreAndEvaluate(evaluator)
    dfScoreAndEvaluate._1.show()
    val dfScore = dfScoreAndEvaluate._1.withColumnRenamed(
      "area-lotFrontage-lotShape-salePrice_5-stagesApplied_Prediction_000000000017",
      "predicted_price")
    val dfEvaluate = dfScoreAndEvaluate._2
    println("Evaluate:\n" + dfEvaluate.toString())

    dfScore.show(false)

    val dfScoreMod = dfScore.rdd.map(x => x(2).toString.split("->")(1).dropRight(1).dropRight(1))
    dfScoreMod.foreach(println)
    dfScoreMod.saveAsTextFile("./output/housing_prices/predictions")
  }
}
