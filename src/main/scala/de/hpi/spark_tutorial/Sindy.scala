package de.hpi.spark_tutorial

import org.apache.spark.sql.SparkSession

object Sindy {

  def discoverINDs(inputs: List[String], spark: SparkSession): Unit = {
    import spark.implicits._

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

    val dfs = inputs.map(table => spark.read
      .option("sep", ";")
      .option("header", "true")
      .csv(table)
    )
    //dfs.foreach(table => print(table.show()))

    // TODO 1st step: -> "cells": zip every value of every cell with column name
    //  example: [("Thriller", (a)), ("Thriller", (t)), ("Thriller", (p))]
    val cells = dfs.map(df => {
      val columns = df.columns
      df.flatMap(row => row.toSeq.map(p => String.valueOf(p)).zip(columns))
    })

    cells.take(2).foreach(cell => print(cell.show()))

    // TODO 2nd step: -> "cache-based preaggr.": pre-aggregate all values that occur multiple times so that:
    //  (("Thriller", (a, t)) in worker 1, ("Thriller", (p)) in worker 2

    cells.map(cell => cell.groupByKey(cell => cell._1))

    // TODO 3rd step: -> "global partitioning & attribute sets": bring same values from different workers
    //  to the same workers and create sets of columns with potential INDs; example: everything with value "Thriller"
    //  is in the same worker, worker creates: {a,t,p} {c}

    // TODO 4th step: -> "inclusion lists": from every set, create a list of possible inclusion dependencies:
    //  [(a, {t,p}), (t, {a,p}), (p,{t,a})]. [c]
    //  note: also create lists with length 1, if a value only occurs in one specific column

    // TODO 5th step: -> "partition": match inclusion lists to workers so that a set of tables is only checked by
    //  a particular worker
    //  example: if a worker has (a, {t,p}), he also has every other inclusion list that mentions either of a,t, or p

    // TODO 6th step: -> "aggregate": if there are inclusion lists, where a column has no dependency (/ is "alone"),
    //  they are not dependent.
    //  from all other inclusion potentials, where a column potentially has an IND && never appears "alone" like this,
    //  create INDs

    // TODO 7th step: -> print all created INDs
    //  for correct solution see slide 45
    // println(resultline) or whatever
  }
}
