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

// Example: Sort Merge Bucket write and join
// Usage:

// `sbt runMain "com.spotify.scio.examples.extra.SortMergeBucketWriteExample
// --outputL=[OUTPUT]--outputR=[OUTPUT]"`
// `sbt runMain "com.spotify.scio.examples.extra.SortMergeBucketJoinExample
// --inputL=[INPUT]--inputR=[INPUT] --output=[OUTPUT]"`
// `sbt runMain "com.spotify.scio.examples.extra.SortMergeBucketTransformExample
// --inputL=[INPUT]--inputR=[INPUT] --output=[OUTPUT]"`
package com.spotify.scio.examples.extra

import com.spotify.scio.{Args, ContextAndArgs, ScioContext}
import com.spotify.scio.avro._
import com.spotify.scio.coders.Coder
import com.spotify.scio.values.SCollection
import org.apache.avro.Schema
import org.apache.avro.file.CodecFactory
import org.apache.avro.generic.{GenericRecord, GenericRecordBuilder}
import org.apache.beam.sdk.extensions.smb.BucketMetadata.HashType
import org.apache.beam.sdk.extensions.smb.{AvroSortedBucketIO, TargetParallelism}
import org.apache.beam.sdk.values.TupleTag

import scala.util.Random

object SortMergeBucketExample {
  lazy val UserDataSchema: Schema = new Schema.Parser().parse(
    """
      |{
      |    "name": "UserData",
      |    "namespace": "com.spotify.examples.extra",
      |    "type": "record",
      |    "fields": [
      |        {
      |            "name": "userId",
      |            "type": ["null", {"type": "string", "avro.java.string": "String"}]
      |        },
      |        {
      |          "name": "age", "type": "int"
      |        }
      |    ]}
      |""".stripMargin
  )

  def user(id: String, age: Int): GenericRecord = new GenericRecordBuilder(UserDataSchema)
    .set("userId", id)
    .set("age", age)
    .build()
}

object SortMergeBucketWriteExample {
  import com.spotify.scio.smb._

  implicit val coder: Coder[GenericRecord] =
    avroGenericRecordCoder(SortMergeBucketExample.UserDataSchema)

  def pipeline(cmdLineArgs: Array[String]): ScioContext = {
    val (sc, args) = ContextAndArgs(cmdLineArgs)

    sc.parallelize(0 until 500)
      .map(i => SortMergeBucketExample.user(i.toString, i % 100))
      .saveAsSortedBucket(
        AvroSortedBucketIO
          .write(classOf[String], "userId", SortMergeBucketExample.UserDataSchema)
          .to(args("users"))
          .withTempDirectory(sc.options.getTempLocation)
          .withCodec(CodecFactory.snappyCodec())
          .withHashType(HashType.MURMUR3_32)
          .withFilenamePrefix("example-prefix")
          .withNumBuckets(2)
          .withNumShards(1)
      )

    // #SortMergeBucketExample_sink
    sc.parallelize(250 until 750)
      .map { i =>
        Account
          .newBuilder()
          .setId(i)
          .setName(i.toString)
          .setType(s"type${i % 5}")
          .setAmount(Random.nextDouble() * 1000)
          .build()
      }
      .saveAsSortedBucket(
        AvroSortedBucketIO
          .write[String, Account](classOf[String], "name", classOf[Account])
          .to(args("accounts"))
          .withSorterMemoryMb(128)
          .withTempDirectory(sc.options.getTempLocation)
          .withCodec(CodecFactory.snappyCodec())
          .withHashType(HashType.MURMUR3_32)
          .withFilenamePrefix("part") // Default is "bucket"
          .withNumBuckets(1)
          .withNumShards(1)
      )
    // #SortMergeBucketExample_sink
    sc
  }

  def secondaryKeyExample(
    args: Args,
    in: SCollection[Account]
  ): Unit = {
    in
      // #SortMergeBucketExample_sink_secondary
      .saveAsSortedBucket(
        AvroSortedBucketIO
          .write[String, String, Account](
            // primary key class and field
            classOf[String],
            "name",
            // secondary key class and field
            classOf[String],
            "type",
            classOf[Account]
          )
          .to(args("accounts"))
      )
    // #SortMergeBucketExample_sink_secondary
  }

  def main(cmdLineArgs: Array[String]): Unit = {
    val sc = pipeline(cmdLineArgs)
    sc.run().waitUntilDone()
  }
}

object SortMergeBucketJoinExample {
  import com.spotify.scio.smb._

  implicit val coder: Coder[GenericRecord] =
    avroGenericRecordCoder(SortMergeBucketExample.UserDataSchema)

  case class UserAccountData(userId: String, age: Int, balance: Double) {
    override def toString: String = s"$userId\t$age\t$balance"
  }

  def pipeline(cmdLineArgs: Array[String]): ScioContext = {
    val (sc, args) = ContextAndArgs(cmdLineArgs)

    val mapFn: ((String, (GenericRecord, Account))) => UserAccountData = {
      case (userId, (userData, account)) =>
        UserAccountData(userId, userData.get("age").toString.toInt, account.getAmount)
    }

    // #SortMergeBucketExample_join
    sc.sortMergeJoin(
      classOf[String],
      AvroSortedBucketIO
        .read(new TupleTag[GenericRecord]("lhs"), SortMergeBucketExample.UserDataSchema)
        // 1. Only 1 user per user ID
        // 2. Out of key intersection 250-499, only 100 (300-349, 400-499) with age < 50
        .withPredicate((xs, x) => xs.size() == 0 && x.get("age").asInstanceOf[Int] < 50)
        .from(args("users")),
      AvroSortedBucketIO
        .read(new TupleTag[Account]("rhs"), classOf[Account])
        .from(args("accounts")),
      TargetParallelism.max()
    ).map(mapFn) // Apply mapping function
      .saveAsTextFile(args("output"))
    // #SortMergeBucketExample_join

    sc
  }

  def main(cmdLineArgs: Array[String]): Unit = {
    val sc = pipeline(cmdLineArgs)
    sc.run().waitUntilDone()
    ()
  }
}

object SortMergeBucketTransformExample {
  import com.spotify.scio.smb._

  def pipeline(cmdLineArgs: Array[String]): ScioContext = {
    val (sc, args) = ContextAndArgs(cmdLineArgs)

    // #SortMergeBucketExample_transform
    val (readLhs, readRhs) = (
      AvroSortedBucketIO
        .read(new TupleTag[GenericRecord]("lhs"), SortMergeBucketExample.UserDataSchema)
        .from(args("users")),
      AvroSortedBucketIO
        .read(new TupleTag[Account]("rhs"), classOf[Account])
        .from(args("accounts"))
    )

    sc.sortMergeTransform(
      classOf[String],
      readLhs,
      readRhs,
      TargetParallelism.auto()
    ).to(
      AvroSortedBucketIO
        .transformOutput(classOf[String], "name", classOf[Account])
        .to(args("output"))
    ).via { case (key, (users, accounts), outputCollector) =>
      users.foreach { _ =>
        outputCollector.accept(
          Account
            .newBuilder()
            .setId(key.toInt)
            .setName(key)
            .setType("combinedAmount")
            .setAmount(accounts.foldLeft(0.0)(_ + _.getAmount))
            .build()
        )
      }
    }
    // #SortMergeBucketExample_transform
    sc
  }

  def secondaryReadExample(cmdLineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdLineArgs)

    // #SortMergeBucketExample_secondary_read
    sc.sortMergeGroupByKey(
      classOf[String], // primary key class
      classOf[String], // secondary key class
      AvroSortedBucketIO
        .read(new TupleTag[Account]("account"), classOf[Account])
        .from(args("accounts"))
    ).map { case ((primaryKey, secondaryKey), elements) =>
    // ...
    }
    // #SortMergeBucketExample_secondary_read
  }

  def main(cmdLineArgs: Array[String]): Unit = {
    val sc = pipeline(cmdLineArgs)
    sc.run().waitUntilDone()
    ()
  }
}
