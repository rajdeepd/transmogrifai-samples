/*
 * Copyright (c) 2019, Rajdeep Dua.
 * All rights reserved.
 *
 */

package com.salesforce.hw.regression.housing_prices

//LotFrontage	LotArea	LotShape	YrSold	SaleType	SaleCondition	SalePrice
//65.0	         8450	  Reg	     2008	    WD	       Normal	     208500
case class HousingPrices(
  lotFrontage: Double,
  area: Integer,
  lotShape: String,
  yrSold : Integer,
  saleType: String,
  saleCondition: String,
  salePrice: Double)

