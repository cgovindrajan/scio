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

package com.spotify.scio.examples.extra

import com.spotify.scio.ContextAndArgs
import com.spotify.scio.extra.annoy._
import com.spotify.scio.values.SCollection
/**
  *
  */
object AnnoyExample {

  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    val r = new scala.util.Random(10)
    val data = (0 until 1000).map(x => (x, Array.fill(40)(r.nextFloat())))
    val angularMain:SCollection[(Int, Array[Float])] = sc.parallelize(data)

    // side input
    val angularSideInput = angularMain.asAnnoy(Angular, 40, 10).asAnnoySideInput

    // querying
    sc.parallelize(Seq(0 until 1000)).withSideInputs(angularSideInput)
      .map{ (x, s) =>
        val annoyIndex:AnnoyIndex = s(angularSideInput)
        x.foreach { i =>
          annoyIndex.getItem(i)
          annoyIndex.getDistance(i, (i + 1) % 1000)
          annoyIndex.getNnsByItem(i, 10, 2)
          annoyIndex.getNnsByVector(data(0)._2,10,2)
        }
        annoyIndex.saveAndClose()
      }
    sc.close()
  }

}
