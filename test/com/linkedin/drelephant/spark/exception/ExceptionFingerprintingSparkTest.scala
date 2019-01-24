/*
 * Copyright 2016 LinkedIn Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.linkedin.drelephant.spark.exception

import java.text.SimpleDateFormat
import java.util
import java.util.{HashMap, Map}

import com.linkedin.drelephant.ElephantContext
import com.linkedin.drelephant.analysis.{AnalyticJob, ApplicationType, HeuristicResult, Severity}
import com.linkedin.drelephant.exceptions.spark.Classifier.LogClass
import com.linkedin.drelephant.exceptions.spark.{ExceptionFingerprinting, ExceptionFingerprintingFactory, ExceptionFingerprintingSpark}
import com.linkedin.drelephant.spark.fetchers.statusapiv1.{StageDataImpl, StageStatus}
import com.linkedin.drelephant.spark.heuristics.ConfigurationHeuristicsConstants._
import com.linkedin.drelephant.spark.exception.ExceptionFingerprintingSparkUtilities._
import com.linkedin.drelephant.spark.heuristics.SparkTestUtilities._
import com.linkedin.drelephant.tuning.IPSOManagerTestRunner
import common.DBTestUtil.initParamGenerater
import org.scalatest.{FunSpec, Matchers}
import common.TestConstants._
import play.{Application, GlobalSettings}
import play.test.FakeApplication
import controllers._
import play.test.FakeApplication
import org.apache.hadoop.conf.Configuration
import play.Application
import play.GlobalSettings

import com.google.common.annotations.VisibleForTesting
import com.linkedin.drelephant.DrElephant
import com.linkedin.drelephant.ElephantContext
import java.util
import common.DBTestUtil._
import common.TestConstants._
import java.util.concurrent.TimeUnit
import models.TuningAlgorithm
import org.slf4j.LoggerFactory
import play.Application
import play.GlobalSettings
import controllers._
import play.test.FakeApplication
import org.apache.hadoop.conf.Configuration
import org.junit.Assert._
import play.test.Helpers._
import org.junit.Before
import org.junit.Test
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors


class ExceptionFingerprintingSparkTest extends FunSpec with Matchers {
  private val sdf = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss")
  val dbConn = new util.HashMap[String, String]
  dbConn.put(DB_DEFAULT_DRIVER_KEY, DB_DEFAULT_DRIVER_VALUE)
  dbConn.put(DB_DEFAULT_URL_KEY, DB_DEFAULT_URL_VALUE)
  dbConn.put(EVOLUTION_PLUGIN_KEY, EVOLUTION_PLUGIN_VALUE)
  dbConn.put(APPLY_EVOLUTIONS_DEFAULT_KEY, APPLY_EVOLUTIONS_DEFAULT_VALUE)

  val gs = new GlobalSettings() {
    override def onStart(app: Application): Unit = {
      //LOGGER.info("Starting FakeApplication")
    }
  }
  val fakeApp = fakeApplication(dbConn, gs)

  describe(".apply") {
    it("check for user enabled exception") {
      val stage = createStage(1, StageStatus.FAILED, Some("array issues"), "details")
      val stages = Seq(stage)
      val executors = getExecutorSummary()
      val properties = getProperties()
      val data = createSparkApplicationData(stages, executors, Some(properties))
      val exceptionFingerprinting = ExceptionFingerprintingFactory.getExceptionFingerprinting(ExceptionFingerprintingFactory.ExecutionEngineTypes.SPARK, data)
      val className = checkTye(exceptionFingerprinting)

      val analyticJob = getAnalyticalJob(false,
        "http://hostname:8042/node/containerlogs/container_e24_1547063162911_185371_01_000001/dssadmin",
        "ltx1-hcl5294.grid.linkedin.com:8042")
      val exceptionInfoList = exceptionFingerprinting.processRawData(analyticJob)
      val classificationValue = exceptionFingerprinting.classifyException(exceptionInfoList)
      className should be("ExceptionFingerprintingSpark")
      exceptionInfoList.size() should be(1)
      classificationValue.name() should be("USER_ENABLED")
    }
    it("check for auto tuning  enabled exception") {
      val stage = createStage(1, StageStatus.FAILED, Some("java.lang.OutOfMemoryError: Exception thrown in " +
        "awaitResult: \n  at org.apache.spark.util.ThreadUtils$.awaitResult(ThreadUtils.scala:194)\n  " +
        "at org.apache.spark.deploy.yarn.ApplicationMaster.runDriver(ApplicationMaster.scala:401)"), "details")
      val stages = Seq(stage)
      val executors = getExecutorSummary()
      val properties = getProperties()
      val data = createSparkApplicationData(stages, executors, Some(properties))
      val exceptionFingerprinting = ExceptionFingerprintingFactory.getExceptionFingerprinting(ExceptionFingerprintingFactory.ExecutionEngineTypes.SPARK, data)
      val className = checkTye(exceptionFingerprinting)

      val analyticJob = getAnalyticalJob(false,
        "http://hostname:8042/node/containerlogs/container_e24_1547063162911_185371_01_000001/dssadmin",
        "ltx1-hcl5294.grid.linkedin.com:8042")
      val exceptionInfoList = exceptionFingerprinting.processRawData(analyticJob)
      val classificationValue = exceptionFingerprinting.classifyException(exceptionInfoList)
      className should be("ExceptionFingerprintingSpark")
      exceptionInfoList.size() should be(1)
      classificationValue.name() should be("AUTOTUINING_ENABLED")
    }
    it("check for build URL for query driver logs ") {
      val  sparkExceptionFingerPrinting = new ExceptionFingerprintingSpark(null)
      val analyticJob = getAnalyticalJob(false,
        "http://hostname:8042/node/containerlogs/container_e24_1547063162911_185371_01_000001/dssadmin",
        "ltx1-hcl5294.grid.linkedin.com:8042")
      val exceptionInfoList = sparkExceptionFingerPrinting.processRawData(analyticJob)
      val queryURL = sparkExceptionFingerPrinting.buildURLtoQuery()
      queryURL should be ("http://0.0.0.0:19888/jobhistory/nmlogs/hostname:0/container_e24_1547063162911_185371_01_000001" +
        "/container_e24_1547063162911_185371_01_000001/dssadmin/stderr/?start=0")
    }
    it("check for eligibilty of applying exception fingerprinting ") {
      val analyticJob = getAnalyticalJob(true,
        "http://hostname:8042/node/containerlogs/container_e24_1547063162911_185371_01_000001/dssadmin",
        "ltx1-hcl5294.grid.linkedin.com:8042")
      analyticJob.getAppType().getName should be("SPARK")
      val fetcher = ElephantContext.instance.getFetcherForApplicationType(analyticJob.getAppType())
      val isApplicationApplied = analyticJob.applyExceptionFingerPrinting(fetcher,null,null)
      isApplicationApplied should be(false)
      analyticJob.setSucceeded(false)
      val isApplicationAppliedNext = analyticJob.applyExceptionFingerPrinting(fetcher,null,null)
      isApplicationAppliedNext should be(true)
    }
    it("check for complete exception fingerprinting "){
      val stage = createStage(1, StageStatus.FAILED, Some("java.lang.OutOfMemoryError: Exception thrown in " +
        "awaitResult: \n  at org.apache.spark.util.ThreadUtils$.awaitResult(ThreadUtils.scala:194)\n  " +
        "at org.apache.spark.deploy.yarn.ApplicationMaster.runDriver(ApplicationMaster.scala:401)"), "details")
      val stages = Seq(stage)
      val executors = getExecutorSummary()
      val properties = getProperties()
      val data = createSparkApplicationData(stages, executors, Some(properties))
      val analyticJob = getAnalyticalJob(false,
        "http://hostname:8042/node/containerlogs/container_e24_1547063162911_185371_01_000001/dssadmin",
        "ltx1-hcl5294.grid.linkedin.com:8042")
      running(testServer(TEST_SERVER_PORT, fakeApp), new ExceptionFingerprintingRunnerTest(data,analyticJob))
    }





  }
}
