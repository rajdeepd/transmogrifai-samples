/*
 * Copyright (c) 2019, Rajdeep Dua.
 * All rights reserved.
 *
 */

package com.salesforce.hw.regression

import com.salesforce.op.features.FeatureBuilder
import com.salesforce.op.features.types._

trait SimpleRegressionFeatures extends Serializable {
  val population = FeatureBuilder.RealNN[SimpleRegression].extract(_.population.toRealNN).asPredictor
  val profit = FeatureBuilder.RealNN[SimpleRegression].extract(_.profit.toRealNN).asResponse
}
