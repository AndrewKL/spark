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

package org.apache.spark.sql.streaming

import java.util.{ConcurrentModificationException, UUID}
import java.util.concurrent.TimeUnit
import javax.annotation.concurrent.GuardedBy

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.hadoop.fs.Path

import org.apache.spark.SparkException
import org.apache.spark.annotation.Evolving
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{AnalysisException, DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.analysis.UnsupportedOperationChecker
import org.apache.spark.sql.connector.catalog.{SupportsWrite, Table}
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.execution.streaming.continuous.ContinuousExecution
import org.apache.spark.sql.execution.streaming.state.StateStoreCoordinatorRef
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.StaticSQLConf.STREAMING_QUERY_LISTENERS
import org.apache.spark.util.{Clock, SystemClock, ThreadUtils, Utils}

/**
 * A class to manage all the [[StreamingQuery]] active in a `SparkSession`.
 *
 * @since 2.0.0
 */
@Evolving
class StreamingQueryManager private[sql] (sparkSession: SparkSession) extends Logging {

  private[sql] val stateStoreCoordinator =
    StateStoreCoordinatorRef.forDriver(sparkSession.sparkContext.env)
  private val listenerBus = new StreamingQueryListenerBus(sparkSession.sparkContext.listenerBus)

  @GuardedBy("activeQueriesSharedLock")
  private val activeQueries = new mutable.HashMap[UUID, StreamingQuery]
  // A global lock to keep track of active streaming queries across Spark sessions
  private val activeQueriesSharedLock = sparkSession.sharedState.activeQueriesLock
  private val awaitTerminationLock = new Object

  @GuardedBy("awaitTerminationLock")
  private var lastTerminatedQuery: StreamingQuery = null

  try {
    sparkSession.sparkContext.conf.get(STREAMING_QUERY_LISTENERS).foreach { classNames =>
      Utils.loadExtensions(classOf[StreamingQueryListener], classNames,
        sparkSession.sparkContext.conf).foreach(listener => {
        addListener(listener)
        logInfo(s"Registered listener ${listener.getClass.getName}")
      })
    }
  } catch {
    case e: Exception =>
      throw new SparkException("Exception when registering StreamingQueryListener", e)
  }

  /**
   * Returns a list of active queries associated with this SQLContext
   *
   * @since 2.0.0
   */
  def active: Array[StreamingQuery] = activeQueriesSharedLock.synchronized {
    activeQueries.values.toArray
  }

  /**
   * Returns the query if there is an active query with the given id, or null.
   *
   * @since 2.1.0
   */
  def get(id: UUID): StreamingQuery = activeQueriesSharedLock.synchronized {
    activeQueries.get(id).orNull
  }

  /**
   * Returns the query if there is an active query with the given id, or null.
   *
   * @since 2.1.0
   */
  def get(id: String): StreamingQuery = get(UUID.fromString(id))

  /**
   * Wait until any of the queries on the associated SQLContext has terminated since the
   * creation of the context, or since `resetTerminated()` was called. If any query was terminated
   * with an exception, then the exception will be thrown.
   *
   * If a query has terminated, then subsequent calls to `awaitAnyTermination()` will either
   * return immediately (if the query was terminated by `query.stop()`),
   * or throw the exception immediately (if the query was terminated with exception). Use
   * `resetTerminated()` to clear past terminations and wait for new terminations.
   *
   * In the case where multiple queries have terminated since `resetTermination()` was called,
   * if any query has terminated with exception, then `awaitAnyTermination()` will
   * throw any of the exception. For correctly documenting exceptions across multiple queries,
   * users need to stop all of them after any of them terminates with exception, and then check the
   * `query.exception()` for each query.
   *
   * @throws StreamingQueryException if any query has terminated with an exception
   *
   * @since 2.0.0
   */
  @throws[StreamingQueryException]
  def awaitAnyTermination(): Unit = {
    awaitTerminationLock.synchronized {
      while (lastTerminatedQuery == null) {
        awaitTerminationLock.wait(10)
      }
      if (lastTerminatedQuery != null && lastTerminatedQuery.exception.nonEmpty) {
        throw lastTerminatedQuery.exception.get
      }
    }
  }

  /**
   * Wait until any of the queries on the associated SQLContext has terminated since the
   * creation of the context, or since `resetTerminated()` was called. Returns whether any query
   * has terminated or not (multiple may have terminated). If any query has terminated with an
   * exception, then the exception will be thrown.
   *
   * If a query has terminated, then subsequent calls to `awaitAnyTermination()` will either
   * return `true` immediately (if the query was terminated by `query.stop()`),
   * or throw the exception immediately (if the query was terminated with exception). Use
   * `resetTerminated()` to clear past terminations and wait for new terminations.
   *
   * In the case where multiple queries have terminated since `resetTermination()` was called,
   * if any query has terminated with exception, then `awaitAnyTermination()` will
   * throw any of the exception. For correctly documenting exceptions across multiple queries,
   * users need to stop all of them after any of them terminates with exception, and then check the
   * `query.exception()` for each query.
   *
   * @throws StreamingQueryException if any query has terminated with an exception
   *
   * @since 2.0.0
   */
  @throws[StreamingQueryException]
  def awaitAnyTermination(timeoutMs: Long): Boolean = {

    val startTime = System.nanoTime()
    def isTimedout = {
      System.nanoTime() - startTime >= TimeUnit.MILLISECONDS.toNanos(timeoutMs)
    }

    awaitTerminationLock.synchronized {
      while (!isTimedout && lastTerminatedQuery == null) {
        awaitTerminationLock.wait(10)
      }
      if (lastTerminatedQuery != null && lastTerminatedQuery.exception.nonEmpty) {
        throw lastTerminatedQuery.exception.get
      }
      lastTerminatedQuery != null
    }
  }

  /**
   * Forget about past terminated queries so that `awaitAnyTermination()` can be used again to
   * wait for new terminations.
   *
   * @since 2.0.0
   */
  def resetTerminated(): Unit = {
    awaitTerminationLock.synchronized {
      lastTerminatedQuery = null
    }
  }

  /**
   * Register a [[StreamingQueryListener]] to receive up-calls for life cycle events of
   * [[StreamingQuery]].
   *
   * @since 2.0.0
   */
  def addListener(listener: StreamingQueryListener): Unit = {
    listenerBus.addListener(listener)
  }

  /**
   * Deregister a [[StreamingQueryListener]].
   *
   * @since 2.0.0
   */
  def removeListener(listener: StreamingQueryListener): Unit = {
    listenerBus.removeListener(listener)
  }

  /**
   * List all [[StreamingQueryListener]]s attached to this [[StreamingQueryManager]].
   *
   * @since 3.0.0
   */
  def listListeners(): Array[StreamingQueryListener] = {
    listenerBus.listeners.asScala.toArray
  }

  /** Post a listener event */
  private[sql] def postListenerEvent(event: StreamingQueryListener.Event): Unit = {
    listenerBus.post(event)
  }

  private def createQuery(
      userSpecifiedName: Option[String],
      userSpecifiedCheckpointLocation: Option[String],
      df: DataFrame,
      extraOptions: Map[String, String],
      sink: Table,
      outputMode: OutputMode,
      useTempCheckpointLocation: Boolean,
      recoverFromCheckpointLocation: Boolean,
      trigger: Trigger,
      triggerClock: Clock): StreamingQueryWrapper = {
    var deleteCheckpointOnStop = false
    val checkpointLocation = userSpecifiedCheckpointLocation.map { userSpecified =>
      new Path(userSpecified).toString
    }.orElse {
      df.sparkSession.sessionState.conf.checkpointLocation.map { location =>
        new Path(location, userSpecifiedName.getOrElse(UUID.randomUUID().toString)).toString
      }
    }.getOrElse {
      if (useTempCheckpointLocation) {
        deleteCheckpointOnStop = true
        val tempDir = Utils.createTempDir(namePrefix = s"temporary").getCanonicalPath
        logWarning("Temporary checkpoint location created which is deleted normally when" +
          s" the query didn't fail: $tempDir. If it's required to delete it under any" +
          s" circumstances, please set ${SQLConf.FORCE_DELETE_TEMP_CHECKPOINT_LOCATION.key} to" +
          s" true. Important to know deleting temp checkpoint folder is best effort.")
        tempDir
      } else {
        throw new AnalysisException(
          "checkpointLocation must be specified either " +
            """through option("checkpointLocation", ...) or """ +
            s"""SparkSession.conf.set("${SQLConf.CHECKPOINT_LOCATION.key}", ...)""")
      }
    }

    // If offsets have already been created, we trying to resume a query.
    if (!recoverFromCheckpointLocation) {
      val checkpointPath = new Path(checkpointLocation, "offsets")
      val fs = checkpointPath.getFileSystem(df.sparkSession.sessionState.newHadoopConf())
      if (fs.exists(checkpointPath)) {
        throw new AnalysisException(
          s"This query does not support recovering from checkpoint location. " +
            s"Delete $checkpointPath to start over.")
      }
    }

    val analyzedPlan = df.queryExecution.analyzed
    df.queryExecution.assertAnalyzed()

    val operationCheckEnabled = sparkSession.sessionState.conf.isUnsupportedOperationCheckEnabled

    if (sparkSession.sessionState.conf.adaptiveExecutionEnabled) {
      logWarning(s"${SQLConf.ADAPTIVE_EXECUTION_ENABLED.key} " +
          "is not supported in streaming DataFrames/Datasets and will be disabled.")
    }

    (sink, trigger) match {
      case (table: SupportsWrite, trigger: ContinuousTrigger) =>
        if (operationCheckEnabled) {
          UnsupportedOperationChecker.checkForContinuous(analyzedPlan, outputMode)
        }
        new StreamingQueryWrapper(new ContinuousExecution(
          sparkSession,
          userSpecifiedName.orNull,
          checkpointLocation,
          analyzedPlan,
          table,
          trigger,
          triggerClock,
          outputMode,
          extraOptions,
          deleteCheckpointOnStop))
      case _ =>
        if (operationCheckEnabled) {
          UnsupportedOperationChecker.checkForStreaming(analyzedPlan, outputMode)
        }
        new StreamingQueryWrapper(new MicroBatchExecution(
          sparkSession,
          userSpecifiedName.orNull,
          checkpointLocation,
          analyzedPlan,
          sink,
          trigger,
          triggerClock,
          outputMode,
          extraOptions,
          deleteCheckpointOnStop))
    }
  }

  /**
   * Start a [[StreamingQuery]].
   *
   * @param userSpecifiedName Query name optionally specified by the user.
   * @param userSpecifiedCheckpointLocation  Checkpoint location optionally specified by the user.
   * @param df Streaming DataFrame.
   * @param sink  Sink to write the streaming outputs.
   * @param outputMode  Output mode for the sink.
   * @param useTempCheckpointLocation  Whether to use a temporary checkpoint location when the user
   *                                   has not specified one. If false, then error will be thrown.
   * @param recoverFromCheckpointLocation  Whether to recover query from the checkpoint location.
   *                                       If false and the checkpoint location exists, then error
   *                                       will be thrown.
   * @param trigger [[Trigger]] for the query.
   * @param triggerClock [[Clock]] to use for the triggering.
   */
  private[sql] def startQuery(
      userSpecifiedName: Option[String],
      userSpecifiedCheckpointLocation: Option[String],
      df: DataFrame,
      extraOptions: Map[String, String],
      sink: Table,
      outputMode: OutputMode,
      useTempCheckpointLocation: Boolean = false,
      recoverFromCheckpointLocation: Boolean = true,
      trigger: Trigger = Trigger.ProcessingTime(0),
      triggerClock: Clock = new SystemClock()): StreamingQuery = {
    val query = createQuery(
      userSpecifiedName,
      userSpecifiedCheckpointLocation,
      df,
      extraOptions,
      sink,
      outputMode,
      useTempCheckpointLocation,
      recoverFromCheckpointLocation,
      trigger,
      triggerClock)

    // The following code block checks if a stream with the same name or id is running. Then it
    // returns an Option of an already active stream to stop outside of the lock
    // to avoid a deadlock.
    val activeRunOpt = activeQueriesSharedLock.synchronized {
      // Make sure no other query with same name is active
      userSpecifiedName.foreach { name =>
        if (activeQueries.values.exists(_.name == name)) {
          throw new IllegalArgumentException(s"Cannot start query with name $name as a query " +
            s"with that name is already active in this SparkSession")
        }
      }

      // Make sure no other query with same id is active across all sessions
      val activeOption = Option(sparkSession.sharedState.activeStreamingQueries.get(query.id))
        .orElse(activeQueries.get(query.id)) // shouldn't be needed but paranoia ...

      val shouldStopActiveRun =
        sparkSession.sessionState.conf.getConf(SQLConf.STREAMING_STOP_ACTIVE_RUN_ON_RESTART)
      if (activeOption.isDefined) {
        if (shouldStopActiveRun) {
          val oldQuery = activeOption.get
          logWarning(s"Stopping existing streaming query [id=${query.id}, " +
            s"runId=${oldQuery.runId}], as a new run is being started.")
          Some(oldQuery)
        } else {
          throw new IllegalStateException(
            s"Cannot start query with id ${query.id} as another query with same id is " +
              s"already active. Perhaps you are attempting to restart a query from checkpoint " +
              s"that is already active. You may stop the old query by setting the SQL " +
              "configuration: " +
              s"""spark.conf.set("${SQLConf.STREAMING_STOP_ACTIVE_RUN_ON_RESTART.key}", true) """ +
              "and retry.")
        }
      } else {
        // nothing to stop so, no-op
        None
      }
    }

    // stop() will clear the queryId from activeStreamingQueries as well as activeQueries
    activeRunOpt.foreach(_.stop())

    activeQueriesSharedLock.synchronized {
      // We still can have a race condition when two concurrent instances try to start the same
      // stream, while a third one was already active and stopped above. In this case, we throw a
      // ConcurrentModificationException.
      val oldActiveQuery = sparkSession.sharedState.activeStreamingQueries.put(
        query.id, query.streamingQuery) // we need to put the StreamExecution, not the wrapper
      if (oldActiveQuery != null) {
        throw new ConcurrentModificationException(
          "Another instance of this query was just started by a concurrent session.")
      }
      activeQueries.put(query.id, query)
    }

    try {
      // When starting a query, it will call `StreamingQueryListener.onQueryStarted` synchronously.
      // As it's provided by the user and can run arbitrary codes, we must not hold any lock here.
      // Otherwise, it's easy to cause dead-lock, or block too long if the user codes take a long
      // time to finish.
      query.streamingQuery.start()
    } catch {
      case e: Throwable =>
        unregisterTerminatedStream(query)
        throw e
    }
    query
  }

  /** Notify (by the StreamingQuery) that the query has been terminated */
  private[sql] def notifyQueryTermination(terminatedQuery: StreamingQuery): Unit = {
    unregisterTerminatedStream(terminatedQuery)
    awaitTerminationLock.synchronized {
      if (lastTerminatedQuery == null || terminatedQuery.exception.nonEmpty) {
        lastTerminatedQuery = terminatedQuery
      }
      awaitTerminationLock.notifyAll()
    }
    stateStoreCoordinator.deactivateInstances(terminatedQuery.runId)
  }

  private def unregisterTerminatedStream(terminatedQuery: StreamingQuery): Unit = {
    activeQueriesSharedLock.synchronized {
      // remove from shared state only if the streaming execution also matches
      sparkSession.sharedState.activeStreamingQueries.remove(
        terminatedQuery.id, terminatedQuery)
      activeQueries -= terminatedQuery.id
    }
  }
}
