/*
 * Copyright 2019 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

// Example: BigQuery Tornadoes Example
// Usage:

// `sbt "runMain com.spotify.scio.examples.cookbook.BigQueryTornadoes
// --project=[PROJECT] --runner=DataflowRunner --region=[REGION NAME]
// --input=apache-beam-testing:samples.weather_stations
// --output=[DATASET].bigquery_tornadoes"`
package com.spotify.scio.examples.cookbook

import com.google.api.services.bigquery.model.{TableFieldSchema, TableSchema}
import com.spotify.scio._
import com.spotify.scio.bigquery._
import com.spotify.scio.examples.common.ExampleData

import scala.jdk.CollectionConverters._

object BigQueryTornadoes {
  def main(cmdlineArgs: Array[String]): Unit = {
    // Create `ScioContext` and `Args`
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    // Schema for result BigQuery table
    val schema = new TableSchema().setFields(
      List(
        new TableFieldSchema().setName("month").setType("INTEGER"),
        new TableFieldSchema().setName("tornado_count").setType("INTEGER")
      ).asJava
    )

    // Open a BigQuery table as a `SCollection[TableRow]`
    val table = Table.Spec(args.getOrElse("input", ExampleData.WEATHER_SAMPLES_TABLE))
    sc.bigQueryTable(table)
      // Extract months with tornadoes
      .flatMap(r => if (r.getBoolean("tornado")) Some(r.getLong("month")) else None)
      // Count occurrences of each unique month to get `(Long, Long)`
      .countByValue
      // Map `(Long, Long)` tuples into result `TableRow`s
      .map(kv => TableRow("month" -> kv._1, "tornado_count" -> kv._2))
      // Save result as a BigQuery table
      .saveAsBigQueryTable(Table.Spec(args("output")), schema, WRITE_TRUNCATE, CREATE_IF_NEEDED)

    // Execute the pipeline
    sc.run()
    ()
  }
}
