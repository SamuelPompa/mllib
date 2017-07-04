package org.pompa

import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

/**
  * Created by Samuel on 4/07/17.
  */
object FirstStep3 {

  val LOGGER = LoggerFactory.getLogger(FirstStep.getClass)

  def main(args: Array[String]) = {

    val ss = SparkSession.builder()
      .master("local[*]")
      .appName("FirstStep").getOrCreate()



    val rdd = ss.sparkContext.textFile("src/main/resources/foods_prueba.txt")
    LOGGER.debug(s"NumPartitions ${rdd.getNumPartitions}")
    val hugeRdd2 = ss.sparkContext.textFile("/home/utad/Escritorio/Datos/foods.txt")

    rdd.take(10).foreach(x => println(x))

    LOGGER.debug(s"Numero de filas antes de memoria en fichero: ${hugeRdd2.count}")
    hugeRdd2.cache
    LOGGER.debug(s"Numero de filas despues de memoria en fichero: ${hugeRdd2.count}")
    LOGGER.debug(s"Numero de filas despues de memoria en fichero: ${hugeRdd2.count}")

    val count = rdd.count

    LOGGER.debug(s"Numero de filas en fichero: $count")
    LOGGER.error(s"Numero de filas en fichero: $count")

    LOGGER.debug(s"Numero de filas en fichero: ${hugeRdd2.count}")
    while(true){}

  }

}
