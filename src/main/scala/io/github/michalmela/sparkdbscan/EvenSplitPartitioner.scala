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

import scala.annotation.tailrec

import org.apache.spark.internal.Logging

/**
 * Helper methods for calling the partitioner
 */
object EvenSplitPartitioner {

  def partition(
                 toSplit: Set[(DbscanRectangle, Int)],
                 maxPointsPerPartition: Long,
                 minimumRectangleSize: Double): List[(DbscanRectangle, Int)] = {
    new EvenSplitPartitioner(maxPointsPerPartition, minimumRectangleSize)
      .findPartitions(toSplit)
  }

}

class EvenSplitPartitioner(
  maxPointsPerPartition: Long,
  minimumRectangleSize: Double) extends Logging {

  type RectangleWithCount = (DbscanRectangle, Int)

  def findPartitions(toSplit: Set[RectangleWithCount]): List[RectangleWithCount] = {

    val boundingRectangle = findBoundingRectangle(toSplit)

    def pointsIn = pointsInRectangle(toSplit, _: DbscanRectangle)

    val toPartition = List((boundingRectangle, pointsIn(boundingRectangle)))
    val partitioned = List[RectangleWithCount]()

    logTrace("About to start partitioning")
    val partitions = partition(toPartition, partitioned, pointsIn)
    logTrace("Done")

    // remove empty partitions
    partitions.filter({ case (partition, count) => count > 0 })
  }

  @tailrec
  private def partition(
    remaining: List[RectangleWithCount],
    partitioned: List[RectangleWithCount],
    pointsIn: (DbscanRectangle) => Int): List[RectangleWithCount] = {

    remaining match {
      case (rectangle, count) :: rest =>
        if (count > maxPointsPerPartition) {

          if (canBeSplit(rectangle)) {
            logTrace(s"About to split: $rectangle")
            def cost = (r: DbscanRectangle) => ((pointsIn(rectangle) / 2) - pointsIn(r)).abs
            val (split1, split2) = split(rectangle, cost)
            logTrace(s"Found split: $split1, $split2")
            val s1 = (split1, pointsIn(split1))
            val s2 = (split2, pointsIn(split2))
            partition(s1 :: s2 :: rest, partitioned, pointsIn)

          } else {
            logWarning(s"Can't split: ($rectangle -> $count) (maxSize: $maxPointsPerPartition)")
            partition(rest, (rectangle, count) :: partitioned, pointsIn)
          }

        } else {
          partition(rest, (rectangle, count) :: partitioned, pointsIn)
        }

      case Nil => partitioned

    }

  }

  def split(
             rectangle: DbscanRectangle,
             cost: (DbscanRectangle) => Int): (DbscanRectangle, DbscanRectangle) = {

    val smallestSplit =
      findPossibleSplits(rectangle)
        .reduceLeft {
          (smallest, current) =>

            if (cost(current) < cost(smallest)) {
              current
            } else {
              smallest
            }

        }

    (smallestSplit, (complement(smallestSplit, rectangle)))

  }

  /**
   * Returns the box that covers the space inside boundary that is not covered by box
   */
  private def complement(box: DbscanRectangle, boundary: DbscanRectangle): DbscanRectangle =
    if (box.x == boundary.x && box.y == boundary.y) {
      if (boundary.x2 >= box.x2 && boundary.y2 >= box.y2) {
        if (box.y2 == boundary.y2) {
          DbscanRectangle(box.x2, box.y, boundary.x2, boundary.y2)
        } else if (box.x2 == boundary.x2) {
          DbscanRectangle(box.x, box.y2, boundary.x2, boundary.y2)
        } else {
          throw new IllegalArgumentException("rectangle is not a proper sub-rectangle")
        }
      } else {
        throw new IllegalArgumentException("rectangle is smaller than boundary")
      }
    } else {
      throw new IllegalArgumentException("unequal rectangle")
    }

  /**
   * Returns all the possible ways in which the given box can be split
   */
  private def findPossibleSplits(box: DbscanRectangle): Set[DbscanRectangle] = {

    val xSplits = (box.x + minimumRectangleSize) until box.x2 by minimumRectangleSize

    val ySplits = (box.y + minimumRectangleSize) until box.y2 by minimumRectangleSize

    val splits =
      xSplits.map(x => DbscanRectangle(box.x, box.y, x, box.y2)) ++
        ySplits.map(y => DbscanRectangle(box.x, box.y, box.x2, y))

    logTrace(s"Possible splits: $splits")

    splits.toSet
  }

  /**
   * Returns true if the given rectangle can be split into at least two rectangles of minimum size
   */
  private def canBeSplit(box: DbscanRectangle): Boolean = {
    (box.x2 - box.x > minimumRectangleSize * 2 ||
      box.y2 - box.y > minimumRectangleSize * 2)
  }

  def pointsInRectangle(space: Set[RectangleWithCount], rectangle: DbscanRectangle): Int = {
    space.view
      .filter({ case (current, _) => rectangle.contains(current) })
      .foldLeft(0) {
        case (total, (_, count)) => total + count
      }
  }

  def findBoundingRectangle(rectanglesWithCount: Set[RectangleWithCount]): DbscanRectangle = {

    val invertedRectangle =
      DbscanRectangle(Double.MaxValue, Double.MaxValue, Double.MinValue, Double.MinValue)

    rectanglesWithCount.foldLeft(invertedRectangle) {
      case (bounding, (c, _)) =>
        DbscanRectangle(
          bounding.x.min(c.x), bounding.y.min(c.y),
          bounding.x2.max(c.x2), bounding.y2.max(c.y2))
    }

  }

}
