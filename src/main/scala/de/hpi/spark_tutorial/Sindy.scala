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

    // 1st step: -> "cells": zip every value of every cell with column name -> [("Thriller", (a)), ("Thriller", (t)), ("Thriller", (p))]
    // 2nd step: -> "cache-based preaggr.": pre-aggregate all values that occur multiple times -> (("Thriller", (a, t)) in worker 1, ("Thriller", (p)) in worker 2
    // stratosphere plan: flat-map split records; Step 1 + 2 can be combined
    val cells = dfs.map(df => {
      val columns = df.columns.map(name => List(name))
      val cells = df.flatMap(row => row.toSeq.map(p => String.valueOf(p)).zip(columns))
      cells.rdd.reduceByKey((a, b) => (a ++ b).distinct)
    })

    // stratosphere plan: reduce by value union attributes -> {a,t,p} {c}
    val reducedUnionAttributes = cells.reduce((a, b) => a.union(b))
    val attributeSets = reducedUnionAttributes.reduceByKey((a, b) => (a ++ b).distinct).map(_._2)

    // stratosphere plan: flatmap create inclusion lists
    val inclusionLists = attributeSets
      .flatMap(attributeSet => {
        attributeSet.map(column => (column , attributeSet.toSet - column))
      })

    val results = inclusionLists.reduceByKey(_ intersect _).filter(_._2.nonEmpty)

    results.collect().sortBy(_._1).foreach(n => println(s"${n._1} < ${n._2.mkString(", ")}")) // print all INDs
  }
}
