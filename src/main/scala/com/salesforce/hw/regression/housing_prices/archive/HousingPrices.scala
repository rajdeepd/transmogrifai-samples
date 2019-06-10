/*
 * Copyright (c) 2019, Rajdeep Dua.
 * All rights reserved.
 *
 */

package com.salesforce.hw.regression.housing_prices.archive


//LotFrontage	LotArea	LotShape	SalePrice
//65.0	         8450	     Reg	   208500
case class HousingPrices(
  lotFrontage: Double,
  area: Integer,
  lotShape: String,
  salePrice: Double)

