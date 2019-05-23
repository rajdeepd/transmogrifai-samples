package com.salesforce.hw.regression

import com.salesforce.op.OpWorkflow
import com.salesforce.op.evaluators.Evaluators
import com.salesforce.op.readers.DataReaders
import com.salesforce.op.stages.impl.regression.RegressionModelSelector
import com.salesforce.op.stages.impl.regression.RegressionModelsToTry.{OpGBTRegressor, OpRandomForestRegressor}
import com.salesforce.op.stages.impl.tuning.{DataCutter, DataSplitter}
import org.apache.spark.sql.{Encoders, SparkSession}
import com.salesforce.hw.regression.SimpleRegressionFeatures
import com.salesforce.op.features.FeatureBuilder
import org.apache.spark.SparkConf
import com.salesforce.op.features.types._

object OpSimpleRegressionComplete {
  def main(args: Array[String]): Unit = {
    print("hello")
    val conf = new SparkConf().setMaster("local[*]").setAppName("..")
    implicit val spark = SparkSession.builder.config(conf).getOrCreate()

    implicit val srEncoder = Encoders.product[SimpleRegression]
    val population = FeatureBuilder.RealNN[SimpleRegression].extract(_.population.toRealNN).asPredictor
    val profit = FeatureBuilder.RealNN[SimpleRegression].extract(_.profit.toRealNN).asResponse

    val trainFilePath = "./src/main/resources/SimpleRegressionDataset/simple_regression.csv"

    val trainDataReader = DataReaders.Simple.csvCase[SimpleRegression](
      path = Option(trainFilePath)
    )

    val features = Seq(population).transmogrify()
    val randomSeed = 42L
    val splitter = DataSplitter(seed = randomSeed)

    val cutter = DataCutter(reserveTestFraction = 0.2, seed = randomSeed)

    val prediction = RegressionModelSelector
      .withCrossValidation(
        dataSplitter = Some(splitter), seed = randomSeed,
        modelTypesToUse = Seq(OpGBTRegressor, OpRandomForestRegressor)
      ).setInput(profit, features).getOutput()

    val evaluator = Evaluators.Regression().setLabelCol(profit).
      setPredictionCol(prediction)

    val workflow = new OpWorkflow().setResultFeatures(prediction, profit).setReader(trainDataReader)
    val workflowModel = workflow.train()
    print(workflowModel)
    val dataFrame = workflowModel.score()
    val df2 = dataFrame.withColumnRenamed("population-profit_3-stagesApplied_Prediction_00000000000f","predicted_profit")
    df2.show(false)
    val dataFrameMod = dataFrame.rdd.map(x => x(2).toString.split("->")(1).dropRight(1).dropRight(1))
    dataFrameMod.foreach(println)
    dataFrameMod.saveAsTextFile("./output/simple_regression/predictions")
  }
}
