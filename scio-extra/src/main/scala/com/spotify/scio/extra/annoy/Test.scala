/*
 * Copyright 2017 Spotify AB.
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

package com.spotify.scio.extra.annoy

import annoy4s._
import com.sun.jna.Native

import scala.collection.JavaConverters._

// scalastyle:off
object Test {
  private val dim = 40
  private val data = Array.fill(1000)(Array.fill(dim)(scala.util.Random.nextFloat()))
  private val resultSize = 10

  val annoyLib = Native.loadLibrary("annoy", classOf[AnnoyLibrary]).asInstanceOf[AnnoyLibrary]

  def main(args: Array[String]): Unit = {
    build()
    test()
  }

  def build(): Unit = {
    println("BUILDING")
    val index = annoyLib.createAngular(dim)
    (0 until 1000).foreach(i => annoyLib.addItem(index, i, data(i)))
    annoyLib.build(index, 10)
    annoyLib.save(index, "annoy.tree")
    annoyLib.deleteIndex(index)
  }

  def test(): Unit = {
    println("TESTING")

    println("QUERYING annoy-java")
    import com.spotify.annoy._
    val jIndex = new ANNIndex(dim, "annoy.tree", IndexType.ANGULAR)
    val jResult = jIndex.getNearest(data(0), resultSize).asScala.asInstanceOf[Seq[Int]]
    println("=" * 80)
    print(jResult)

    println("QUERYING annoy4s")
    val sIndex = annoyLib.createAngular(dim)
    println("A")
    annoyLib.load(sIndex, "annoy.tree")
    println("B")
    val sResult = Array.fill(resultSize)(-1)
    val distances = Array.fill(resultSize)(-1.0f)
    println("C")
    annoyLib.getNnsByVector(sIndex, data(0), resultSize, -1, sResult, distances)
    println("=" * 80)
    print(sResult)
  }

  def print(result: Seq[Int]): Unit = {
    result.foreach { i =>
      if (i < 0) {
        println(s"$i\tNULL")
      } else {
        val v1 = data(0)
        val v2 = data(i)
        val dp = (v1 zip v2).map(p => p._1 * p._2).sum
        val ss = math.sqrt(v1.map(x => x * x).sum * v2.map(x => x * x).sum)
        val d = dp / ss
        println(s"$i\t$d")
      }
    }
  }

}
