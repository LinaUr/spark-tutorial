package de.hpi.spark_tutorial
import org.apache.spark.sql.{Dataset, SparkSession}

object Sindy {

  def discoverINDs(inputs: List[String], spark: SparkSession): Unit = {
    import spark.implicits._

    // stratosphere plan: data source multi-file

    val dfs = inputs.map(table => spark.read
      .option("sep", ";")
      .option("header", "true")
      .csv(table)
    )

    // TODO 1st step: -> "cells": zip every value of every cell with column name
    //  example: [("Thriller", (a)), ("Thriller", (t)), ("Thriller", (p))]


    // TODO 2nd step: -> "cache-based preaggr.": pre-aggregate all values that occur multiple times so that:
    //  (("Thriller", (a, t)) in worker 1, ("Thriller", (p)) in worker 2

    // stratosphere plan: flat-map split records; Step 1 + 2 can be combined

    val cells = dfs.map(df => {
      val columns = df.columns.map(name => List(name))
      val cells = df.flatMap(row => row.toSeq.map(p => String.valueOf(p)).zip(columns))
      cells.rdd.reduceByKey((a, b) => (a ++ b).distinct)
    })
    // cells.foreach(cell => print(cell.toDF().show()))

    // TODO 3rd step: -> "global partitioning & attribute sets": bring same values from different workers
    //  to the same workers and create sets of columns with potential INDs; example: everything with value "Thriller"
    //  is in the same worker, worker creates: {a,t,p} {c}

    // stratosphere plan: reduce by value union attributes

    val reducedUnionAttributes = cells.reduce((a, b) => a.union(b))
    val attributeSets = reducedUnionAttributes.reduceByKey((a, b) => (a ++ b).distinct).map(_._2)

    // TODO 4th step: -> "inclusion lists": from every set, create a list of possible inclusion dependencies:
    //  [(a, {t,p}), (t, {a,p}), (p,{t,a})]. [c]
    //  note: also create lists with length 1, if a value only occurs in one specific column

    // stratosphere plan: flatmap create inclusion lists

    val inclusionLists = attributeSets
      .flatMap(attributeSet => {
        attributeSet.map(column => (column , attributeSet.toSet - column))
      })

    // TODO 5th step: -> "partition": match inclusion lists to workers so that a set of tables is only checked by
    //  a particular worker
    //  example: if a worker has (a, {t,p}), he also has every other inclusion list that mentions either of a,t, or p
    // probably not necessary? not mentioned in "stratosphere plan"

    // TODO 6th step: -> "aggregate": if there are inclusion lists, where a column has no dependency (/ is "alone"),
    //  they are not dependent.
    //  from all other inclusion potentials, where a column potentially has an IND && never appears "alone" like this,
    //  create INDs

    val results = inclusionLists.reduceByKey(_ intersect _).filter(_._2.nonEmpty)

    // TODO 7th step: -> print all created INDs
    //  for correct solution see slide 45
    results.collect().sortBy(_._1).foreach(n => println(s"${n._1} < ${n._2.mkString(", ")}"))
  }
}
