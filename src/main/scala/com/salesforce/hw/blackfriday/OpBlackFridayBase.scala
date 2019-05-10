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

import com.salesforce.hw.iris.IrisFeatures
import com.salesforce.op._
import com.salesforce.op.evaluators.Evaluators
import com.salesforce.op.readers.DataReaders
import com.salesforce.op.stages.impl.classification.MultiClassificationModelSelector
import com.salesforce.op.stages.impl.tuning.DataCutter
import org.apache.spark.sql.Encoders

/**
 * TransmogrifAI MultiClass Classification example on the Iris Dataset
 */
class OpBlackFridayBase extends BlackFridayFeatures {

  implicit val irisEncoder = Encoders.product[BlackFriday]

  val irisReader = DataReaders.Simple.csvCase[BlackFriday]()
  //val labels = purchase.asRaw()

  val features = Seq(userId, productId,gender, age, occupation, cityCategory, stayInCurrentCityYears, maritalStatus,productCategoryOne,
    productCategoryTwo, productCategoryThree).transmogrify()

  val randomSeed = 42L

  val cutter = DataCutter(reserveTestFraction = 0.2, seed = randomSeed)

  val prediction = MultiClassificationModelSelector
    .withCrossValidation(splitter = Option(cutter), seed = randomSeed)
    .setInput(purchase, features).getOutput()

  //val evaluator = Evaluators.MultiClassification.f1().setLabelCol(labels).setPredictionCol(prediction)
  val evaluator = Evaluators.Regression().setLabelCol(purchase).setPredictionCol(prediction)

  val workflow = new OpWorkflow().setResultFeatures(prediction, purchase)


}
