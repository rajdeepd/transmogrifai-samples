package com.salesforce.hw.primaindians

import net.liftweb.json.DefaultFormats
import net.liftweb.json._
import scala.io.Source
/*
 "Recall" : 0.7747395833333334,
  "F1" : 0.7744798638274697,
  "Error" : 0.22526041666666663,
 */
object ModelMetricsDisplay {
  implicit val formats = DefaultFormats
  val filename = "/tmp/pi-metrics/part-00000"

  val fileContents = Source.fromFile(filename).getLines.mkString
  def main(args: Array[String]): Unit = {
    val json = parse(fileContents)
    val precisionElements = (json \\ "Precision").children
    var precision = ""
    for ( element <- precisionElements ) {
      precision = precisionElements(0).values.toString
    }

    val recallElements = (json \\ "Recall").children
    var recall = ""
    for ( element <- recallElements ) {
      recall = recallElements(0).values.toString
    }
    var f1Score = ""
    val f1Elements = (json \\ "F1").children
    for ( element <- f1Elements ) {
      f1Score = f1Elements(0).values.toString
    }

    println("Precision:" + precision)
    println("Recall: " + recall)
    println("F1 Score: " + f1Score)

  }
}
/**
  * A case class to match the json properties.
  */
case class EmailAccount(
                         accountName: String,
                         url: String,
                         username: String,
                         password: String,
                         minutesBetweenChecks: Int,
                         usersOfInterest: List[String]
)
