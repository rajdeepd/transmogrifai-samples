/*
 * Copyright (c) 2019, Rajdeep Dua.
 * All rights reserved.
 *
 */

package com.salesforce.hw.blackfriday

case class BlackFriday (
  //User_ID	Product_ID	Gender	Age	Occupation	City_Category	Stay_In_Current_City_Years	Marital_Status
  //Product_Category_1	Product_Category_2	Product_Category_3	Purchase
  userId: Double,
  productId: String,
  gender: Double,
  age: String,
  occupation: String,
  cityCategory: String,
  stayInCurrentCityYears: String,
  maritalStatus: Int,
  productCategoryOne: Int,
  productCategoryTwo: Int,
  productCategoryThree: Int,
  purchase: Double)

