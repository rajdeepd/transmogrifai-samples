/*
 * Copyright (c) 2017, Salesforce.com, Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * * Redistributions of source code must retain the above copyright notice, this
 *   list of conditions and the following disclaimer.
 *
 * * Redistributions in binary form must reproduce the above copyright notice,
 *   this list of conditions and the following disclaimer in the documentation
 *   and/or other materials provided with the distribution.
 *
 * * Neither the name of the copyright holder nor the names of its
 *   contributors may be used to endorse or promote products derived from
 *   this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
 * CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
 * OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package com.salesforce.hw.blackfriday

import java.util.concurrent.TimeUnit

import com.salesforce.op._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.concurrent.duration.Duration

/**
 * TransmogrifAI MultiClass Classification example on the Iris Dataset
 */
object OpBlackFridayTrain extends OpAppWithRunner with BlackFridayFeatures {

  val conf = new SparkConf().setMaster("local[*]").setAppName("BlackFridayPrediction")
  implicit val spark = SparkSession.builder.config(conf).getOrCreate()

  val opBackFridayBase = new OpBlackFridayBase()

  def runner(opParams: OpParams): OpWorkflowRunner =
    new OpWorkflowRunner(
      workflow = opBackFridayBase.workflow,
      trainingReader = opBackFridayBase.irisReader,
      scoringReader = opBackFridayBase.irisReader,
      evaluationReader = Option(opBackFridayBase.irisReader),
      evaluator = Option(opBackFridayBase.evaluator),
      featureToComputeUpTo = Option(opBackFridayBase.features)
    )
  /*
  --run-type=train \
  --model-location=/tmp/iris-model \
  --read-location Iris=`pwd`/src/main/resources/IrisDataset/iris.data"
   */
  override def main(args: Array[String]): Unit = {
    val myArgs = Array("--run-type=train", "--model-location=/tmp/bf-model",
      "--read-location", "BlackFriday=./src/main/resources/BlackFridayDataset/export_dataframe_1.csv")
    val (runType, opParams) = parseArgs(myArgs)
    val batchDuration = Duration(opParams.batchDurationSecs.getOrElse(1), TimeUnit.SECONDS)
    val (spark, streaming) = sparkSession -> sparkStreamingContext(batchDuration)
    run(runType, opParams)(spark, streaming)
  }

}