package com.salesforce.hw.regression.housing_prices

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

    implicit val srEncoder = Encoders.product[HousingPrices]
    val saleTypeEncoder = Map("COD" -> 1, "CWD" -> 2, "Con" -> 3, "ConLD" -> 4,
      "ConLI" -> 5, "ConLw" -> 6,"New" -> 7, "Oth" -> 8,"WD" -> 9  )

    val lotFrontage = FeatureBuilder.Real[HousingPrices].extract(_.lotFrontage.toReal).asPredictor
    val area = FeatureBuilder.Integral[HousingPrices].extract(_.area.toIntegral).asPredictor
    val lotShape = FeatureBuilder.Integral[HousingPrices].extract(x =>
    {
      var y = 0
      if(x.lotShape.equals("IR1")){
        y = 1
      }else{
        y = 0
      }
      y.toIntegral
    }).asPredictor
    val yrSold = FeatureBuilder.Integral[HousingPrices].extract(x =>
      {
        var y =  2019 - x.yrSold
        y.toIntegral
      }
    ).asPredictor

    val saleType = FeatureBuilder.Integral[HousingPrices].extract(x =>
      {
        val y = x.saleType
        val z = saleTypeEncoder.get(y)
        z.toIntegral
      }
    ).asPredictor

    /*
     * SalesCondition values Abnorml, AdjLand, Alloca, Family, Normal, Partial
     */
    val saleConditionEncoder = Map("Abnorml" -> 1, "AdjLand" -> 2, "Alloca" -> 3, "Family" -> 4,
      "Normal" -> 5, "Partial" -> 6 )

    val saleCondition = FeatureBuilder.Integral[HousingPrices].extract(x =>
    {
      val y = x.saleCondition
      val z = saleConditionEncoder.get(y)
      z.toIntegral
    }
    ).asPredictor

    val salePrice = FeatureBuilder.RealNN[HousingPrices].extract(_.salePrice.toRealNN).asResponse

    val trainFilePath = "./src/main/resources/HousingPricesDataset/train_lf_la_ls_ys_st_sc.csv"

    val trainDataReader = DataReaders.Simple.csvCase[HousingPrices](
      path = Option(trainFilePath)
    )

    val features = Seq(lotFrontage,area,lotShape, yrSold, saleType, saleCondition).transmogrify()
    val randomSeed = 42L
    val splitter = DataSplitter(seed = randomSeed)

    val cutter = DataCutter(reserveTestFraction = 0.2, seed = randomSeed)

    val prediction1 = RegressionModelSelector
      .withCrossValidation(
        dataSplitter = Some(splitter), seed = randomSeed,
        modelTypesToUse = Seq(OpGBTRegressor, OpRandomForestRegressor)
      ).setInput(salePrice,features).getOutput()

    val evaluator = Evaluators.Regression().setLabelCol(salePrice).
      setPredictionCol(prediction1)

    val workflow = new OpWorkflow().setResultFeatures(prediction1, salePrice).setReader(trainDataReader)
    val workflowModel = workflow.train()


    val dfScoreAndEvaluate = workflowModel.scoreAndEvaluate(evaluator)
    dfScoreAndEvaluate._1.show()
    val dfScore = dfScoreAndEvaluate._1.withColumnRenamed(
      "area-lotFrontage-lotShape-saleCondition-salePrice-saleType-yrSold_4-stagesApplied_Prediction_000000000015",
      "predicted_price")
    val dfEvaluate = dfScoreAndEvaluate._2
    println("Evaluate:\n" + dfEvaluate.toString())

    dfScore.show(false)

    val dfScoreMod = dfScore.rdd.map(x => x(2).toString.split("->")(1).dropRight(1).dropRight(1))
    dfScoreMod.foreach(println)
    dfScoreMod.saveAsTextFile("./output/housing_prices/predictions_2")
  }
}
