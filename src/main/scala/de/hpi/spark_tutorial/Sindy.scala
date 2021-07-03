package de.hpi.spark_tutorial

import org.apache.spark.sql.SparkSession

object Sindy {

  def discoverINDs(inputs: List[String], spark: SparkSession): Unit = {

    // TODO
    /**
     * # TASK
     * Input: a dataset with different tables
     * - look for foreign-key relationships between different tables -> and FIND all
     * - and also within the same table (could ne used for self-joins)
     * Do that by:
     * - checking for unary inclusion dependencies (order of values can be arbitrary, see columns as Sets)
     *      (= all values in col X of table1 are contained in col Y of table2)
     * - complexity is O(n²-n) -> so we want to distribute it
     */

    // TODO print all dependencies
    //  for correct solution see slide 45
    // println(resultline) or whatever
  }
}
