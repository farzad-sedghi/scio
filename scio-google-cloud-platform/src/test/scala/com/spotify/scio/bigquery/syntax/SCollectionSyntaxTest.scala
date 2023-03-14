/*
 * Copyright 2020 Spotify AB.
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

package com.spotify.scio.bigquery.syntax

import com.spotify.scio.ScioContext
import com.spotify.scio.bigquery._
import com.spotify.scio.testing.{EqualNamePTransformMatcher, PipelineSpec, TransformFinder}
import org.apache.avro.generic.GenericRecord
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write
import org.apache.beam.sdk.transforms.display.DisplayData

import scala.jdk.CollectionConverters._

object SCollectionSyntaxTest {

  // BQ Write transform display id data for tableDescription
  private val TableDescriptionId = DisplayData.Identifier.of(
    DisplayData.Path.root(),
    classOf[Write[_]],
    "tableDescription"
  )

  @BigQueryType.toTable
  case class BQRecord(i: Int, s: String, r: List[String])
}

class SCollectionSyntaxTest extends PipelineSpec {

  import SCollectionSyntaxTest._

  def testTableDescription(sc: ScioContext, bqWriteName: String): Unit = {
    val finder = new TransformFinder(new EqualNamePTransformMatcher(bqWriteName))
    sc.pipeline.traverseTopologically(finder)
    val transforms = finder.result()
    transforms should have size 1
    val displayData = DisplayData.from(transforms.head).asMap().asScala
    displayData.get(TableDescriptionId).map(_.getValue) shouldBe Some("table-description")
  }

  "SCollectionTableRowOps" should "provide PTransform override on saveAsBigQueryTable" in {
    val name = "saveAsBigQueryTable"
    val sc = ScioContext()
    sc
      .empty[TableRow]
      .withName(name)
      .saveAsBigQueryTable(
        Table.Spec("project:dataset.table"),
        createDisposition = Write.CreateDisposition.CREATE_NEVER,
        configOverride = _.withTableDescription("table-description")
      )

    testTableDescription(sc, name)
  }

  "SCollectionGenericRecordOps" should "provide PTransform override on saveAsBigQueryTable" in {
    val name = "saveAsBigQueryTable"
    val sc = ScioContext()
    sc
      .empty[GenericRecord]
      .withName("saveAsBigQueryTable")
      .saveAsBigQueryTable(
        Table.Spec("project:dataset.table"),
        createDisposition = Write.CreateDisposition.CREATE_NEVER,
        configOverride = _.withTableDescription("table-description")
      )

    testTableDescription(sc, name)
  }

  "SCollectionTypedOps" should "provide PTransform override on saveAsTypedBigQueryTable" in {
    val name = "saveAsTypedBigQueryTable"
    val sc = ScioContext()
    sc
      .empty[BQRecord]
      .withName(name)
      .saveAsTypedBigQueryTable(
        Table.Spec("project:dataset.table"),
        createDisposition = Write.CreateDisposition.CREATE_NEVER,
        configOverride = _.withTableDescription("table-description")
      )

    // TODO why is name not respected here ?
    testTableDescription(sc, s"${name}$$Write")
  }
}
