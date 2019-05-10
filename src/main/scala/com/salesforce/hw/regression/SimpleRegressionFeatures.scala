/*
 * Copyright (c) 2019, Rajdeep Dua.
 * All rights reserved.
 *
 */

package com.salesforce.hw.regression

import com.salesforce.op.features.FeatureBuilder
import com.salesforce.op.features.types._

trait SimpleRegressionFeatures extends Serializable {
  /*productId: Double,
  gender: String,
  age: String,
  occupation: String,
  cityCategory: Char,
  stayInCurrentCityYears: String,
  maritalStatus: Int,
  productCategoryOne: Int,
  productCategoryTwo: Int,
  productCategoryThree: Int,
  purchase: Int*/

  val population = FeatureBuilder.RealNN[SimpleRegression].extract(_.population.toRealNN).asPredictor

  val profit = FeatureBuilder.RealNN[SimpleRegression].extract(_.profit.toRealNN).asResponse
}
