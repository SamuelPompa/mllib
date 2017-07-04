package org.pompa

import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

/**
  * Created by utad on 4/07/17.
  */
object FirstStep {

  val LOGGER = LoggerFactory.getLogger(FirstStep.getClass)

  def main(args: Array[String]) = {

    val ss = SparkSession.builder()
      .master("local[*]")
      .appName("FirstStep").getOrCreate()
    val rdd = ss.sparkContext.textFile("src/main/resources/foods_prueba.txt")

    rdd.take(4).foreach(x => println(x))

    val count = rdd.count

    LOGGER.debug(s"Numero de filas en fichero: $count")
    LOGGER.error(s"Numero de filas en fichero: $count")



  }

}
