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
package io.github.michalmela.sparkdbscan

import java.net.URI

import scala.io.Source

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.apache.spark.mllib.linalg.Vectors

class LocalDbscanArcherySuite extends AnyFunSuite with Matchers {

  private val dataFile = "labeled_data.csv"

  test("should cluster") {

    val labeled: Map[DbscanPoint, Double] =
      new LocalDbscanArchery(eps = 0.3F, minPoints = 10)
        .fit(getRawData(dataFile))
        .map(l => (l, l.cluster.toDouble))
        .toMap

    val expected: Map[DbscanPoint, Double] = getExpectedData(dataFile).toMap

    labeled.foreach {
      case (key, value) => {
        val t = expected(key)
        if (t != value) {
          println(s"expected: $t but got $value for $key")
        }

      }
    }

    labeled should equal(expected)

  }

  def getExpectedData(file: String): Iterator[(DbscanPoint, Double)] = {
    Source
      .fromFile(getFile(file))
      .getLines()
      .map(s => {
        val vector = Vectors.dense(s.split(',').map(_.toDouble))
        val point = DbscanPoint(vector)
        (point, vector(2))
      })
  }

  def getRawData(file: String): Iterable[DbscanPoint] = {

    Source
      .fromFile(getFile(file))
      .getLines()
      .map(s => DbscanPoint(Vectors.dense(s.split(',').map(_.toDouble))))
      .toIterable
  }

  def getFile(filename: String): URI = {
    getClass.getClassLoader.getResource(filename).toURI
  }

}
