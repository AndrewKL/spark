/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.datasources.v2

import collection.JavaConverters._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.{InternalRow}
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{Ascending, AttributeMap, Descending, NullsFirst, NullsLast, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical
import org.apache.spark.sql.catalyst.plans.physical.SinglePartition
import org.apache.spark.sql.catalyst.util.truncatedString
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReaderFactory, Scan, SupportsReadOrdering, SupportsReportPartitioning}
import org.apache.spark.sql.connector.read.ordering.{NullOrdering => V2NullOrdering, SortDirection => V2SortDirection}
import org.apache.spark.sql.execution.LeafExecNode
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.Utils

trait DataSourceV2ScanExecBase extends LeafExecNode {

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"))

  def scan: Scan

  def partitions: Seq[InputPartition]

  def readerFactory: PartitionReaderFactory

  override def simpleString(maxFields: Int): String = {
    val result =
      s"$nodeName${truncatedString(output, "[", ", ", "]", maxFields)} ${scan.description()}"
    Utils.redact(sqlContext.sessionState.conf.stringRedactionPattern, result)
  }

  override def outputPartitioning: physical.Partitioning = scan match {
    case _ if partitions.length == 1 =>
      SinglePartition

    case s: SupportsReportPartitioning =>
      new DataSourcePartitioning(
        s.outputPartitioning(), AttributeMap(output.map(a => a -> a.name)))

    case _ => super.outputPartitioning
  }

  override def outputOrdering: Seq[SortOrder] = scan match {
    case s: SupportsReadOrdering =>
      s.outputOrdering().asScala.map(v2SortOrder => {
        val expr = output.resolve(
          UnresolvedAttribute.parseAttributeName(v2SortOrder.column),
          conf.resolver)

        assert(expr.isDefined,
          s"Attribute ${v2SortOrder.column} is not found in the data source output")

        new SortOrder(
          expr.get,
          sortDirectionMap(v2SortOrder.sortDirection),
          nullOrdering(v2SortOrder.nullOrdering),
          Set.empty
        )
      })
    case _ => Nil
  }

  val sortDirectionMap = Map(
    V2SortDirection.Asc -> Ascending,
    V2SortDirection.Desc -> Descending
  )

  val nullOrdering = Map(
    V2NullOrdering.NullsFirst -> NullsFirst,
    V2NullOrdering.NullsLast -> NullsLast
  )

  override def supportsColumnar: Boolean = {
    require(partitions.forall(readerFactory.supportColumnarReads) ||
      !partitions.exists(readerFactory.supportColumnarReads),
      "Cannot mix row-based and columnar input partitions.")

    partitions.exists(readerFactory.supportColumnarReads)
  }

  def inputRDD: RDD[InternalRow]

  def inputRDDs(): Seq[RDD[InternalRow]] = Seq(inputRDD)

  override def doExecute(): RDD[InternalRow] = {
    val numOutputRows = longMetric("numOutputRows")
    inputRDD.map { r =>
      numOutputRows += 1
      r
    }
  }

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val numOutputRows = longMetric("numOutputRows")
    inputRDD.asInstanceOf[RDD[ColumnarBatch]].map { b =>
      numOutputRows += b.numRows()
      b
    }
  }
}
