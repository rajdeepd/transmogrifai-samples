/*
 * Copyright (c) 2019, Rajdeep Dua.
 * All rights reserved.
 *
 */

package com.salesforce.hw.blackfriday

import com.salesforce.op.features.FeatureBuilder
import com.salesforce.op.features.types._
trait BlackFridayFeatures extends Serializable {
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

  val userId = FeatureBuilder.RealNN[BlackFriday].extract(_.userId.toRealNN).asPredictor
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
}
