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

package com.salesforce.hw.pimaindians

import java.io.File
import java.util.concurrent.TimeUnit

import com.salesforce.op._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.concurrent.duration.Duration

/**
 * TransmogrifAI MultiClass Classification example on the PimaIndian dataset
 */
object OpPimaIndiansEvaluate  extends OpAppWithRunner with PimaIndianFeatures {

  val conf = new SparkConf().setMaster("local[*]").setAppName("PimaPrediction")
  implicit val spark = SparkSession.builder.config(conf).getOrCreate()

  val opPIBase = new OpPimaIndiansBase()

  def runner(opParams: OpParams): OpWorkflowRunner =
    new OpWorkflowRunner(
      workflow = opPIBase.workflow,
      trainingReader = opPIBase.piReader,
      scoringReader = opPIBase.piReader,
      evaluationReader = Option(opPIBase.piReader),
      evaluator = Option(opPIBase.evaluator),
      featureToComputeUpTo = Option(opPIBase.features)
    )
  /*
  --run-type=evaluate \
  --model-location=/tmp/pi-model \
  --metrics-location=/tmp/pi-metrics \
  --read-location Iris=`pwd`/src/main/resources/PimaIndiansDataset/pimaindiansdiabetes.data \
  --write-location=/tmp/pi-eval"
   */
  override def main(args: Array[String]): Unit = {
    val metricsLocation = "/tmp/pi-metrics"
    val evalLocation = "/tmp/pi-eval"

    val myArgs = Array("--run-type=evaluate", "--model-location=/tmp/pi-model",
      "--metrics-location=" + metricsLocation,
      "--read-location",
      "PrimaIndians=./src/main/resources/PimaIndiansDataset/pimaindiansdiabetes.data",
      "--write-location=" + evalLocation
    )
    var fileExits = false
    var reason = ""
    val file = new File(metricsLocation)
    val evalFile = new File(evalLocation)
    if(file.exists()) {
      fileExits= true
      reason = metricsLocation + " already exits"
      println(reason)
    }
    if (evalFile.exists()) {
      fileExits= true
      reason = evalLocation + " already exits"
      println(reason)
    }
    if(!fileExits){
      val (runType, opParams) = parseArgs(myArgs)
      val batchDuration = Duration(opParams.batchDurationSecs.getOrElse(1), TimeUnit.SECONDS)
      val (spark, streaming) = sparkSession -> sparkStreamingContext(batchDuration)
      run(runType, opParams)(spark, streaming)
    }
  }
}
