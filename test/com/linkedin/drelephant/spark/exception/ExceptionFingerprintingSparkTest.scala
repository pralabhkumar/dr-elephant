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
import com.linkedin.drelephant.exceptions.spark.{ExceptionFingerprintingFactory, ExceptionFingerprintingSpark}
import com.linkedin.drelephant.spark.fetchers.statusapiv1.{StageStatus}
import com.linkedin.drelephant.spark.exception.ExceptionFingerprintingSparkUtilities._
import com.linkedin.drelephant.spark.heuristics.SparkTestUtilities._
import org.scalatest.{FunSpec, Matchers}
import org.apache.hadoop.conf.Configuration
import java.util
import common.TestConstants._
import play.Application
import play.GlobalSettings
import play.test.FakeApplication
import org.apache.hadoop.conf.Configuration
import play.test.Helpers._
import com.linkedin.drelephant.exceptions.spark.Constant._
import com.linkedin.drelephant.exceptions.spark.ExceptionUtils._
import Array._

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
      val exceptionFingerprinting = ExceptionFingerprintingFactory.getExceptionFingerprinting(ExecutionEngineTypes.SPARK, data)
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
      val exceptionFingerprinting = ExceptionFingerprintingFactory.getExceptionFingerprinting(ExecutionEngineTypes.SPARK, data)
      val className = checkTye(exceptionFingerprinting)

      val analyticJob = getAnalyticalJob(false,
        "http://hostname:8042/node/containerlogs/container_e24_1547063162911_185371_01_000001/dssadmin",
        "ltx1-hcl5294.grid.linkedin.com:8042")
      val exceptionInfoList = exceptionFingerprinting.processRawData(analyticJob)
      val classificationValue = exceptionFingerprinting.classifyException(exceptionInfoList)
      className should be("ExceptionFingerprintingSpark")
      exceptionInfoList.size() should be(1)
      classificationValue.name() should be("AUTOTUNING_ENABLED")
    }
    it("check for build URL for query driver logs ") {
      val sparkExceptionFingerPrinting = new ExceptionFingerprintingSpark(null)
      val analyticJob = getAnalyticalJob(false,
        "http://hostname:8042/node/containerlogs/container_e24_1547063162911_185371_01_000001/dssadmin",
        "ltx1-hcl5294.grid.linkedin.com:8042")
      val exceptionInfoList = sparkExceptionFingerPrinting.processRawData(analyticJob)
      val queryURL = sparkExceptionFingerPrinting.buildURLtoQuery()
      queryURL should be("http://0.0.0.0:19888/jobhistory/nmlogs/hostname:0/container_e24_1547063162911_185371_01_000001" +
        "/container_e24_1547063162911_185371_01_000001/dssadmin/stderr/?start=0")
    }
    it("check for eligibilty of applying exception fingerprinting ") {
      val analyticJob = getAnalyticalJob(true,
        "http://hostname:8042/node/containerlogs/container_e24_1547063162911_185371_01_000001/dssadmin",
        "ltx1-hcl5294.grid.linkedin.com:8042")
      analyticJob.getAppType().getName should be("SPARK")
      val isApplicationApplied = analyticJob.applyExceptionFingerPrinting(null, null)
      isApplicationApplied should be(false)
      analyticJob.setSucceeded(false)
      val isApplicationAppliedNext = analyticJob.applyExceptionFingerPrinting(null, null)
      isApplicationAppliedNext should be(true)
    }
    it("check for complete exception fingerprinting ") {
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
      running(testServer(TEST_SERVER_PORT, fakeApp), new ExceptionFingerprintingRunnerTest(data, analyticJob))
    }
    it("check for exception regex ") {
      val dataContainsException = Array("java.io.FileNotFoundException: File /jobs/emailopt/\n",
        "java.lang.OutOfMemoryError: Java heap space\n",
        "Reason: Container killed by YARN for\n","java.lang.OutOfMemoryError: Exception thrown in awaitResult:\n"
          + "  at org.apache.spark.util.ThreadUtils$.awaitResult(ThreadUtils.scala:194)")
      val dataContainsNoException = Array("SLF4J: Actual binding is of type [org.slf4j.impl.Log4jLoggerFactory]\n")
      for (data <- dataContainsException) {
        isExceptionContains(data) should be(true)
      }
      for (data <- dataContainsNoException) {
        isExceptionContains(data) should be(false)
      }
    }


  }
}
