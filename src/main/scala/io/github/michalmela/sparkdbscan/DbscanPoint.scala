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

import org.apache.spark.mllib.linalg.Vector

case class DbscanPoint(val vector: Vector) {

  def x = vector(0)
  def y = vector(1)

  def distanceSquared(other: DbscanPoint): Double = {
    val dx = other.x - x
    val dy = other.y - y
    (dx * dx) + (dy * dy)
  }

}
