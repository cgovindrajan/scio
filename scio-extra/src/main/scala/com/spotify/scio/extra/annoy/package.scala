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
package com.spotify.scio.extra

import java.util.UUID

import annoy4s._
import com.spotify.scio.ScioContext
import com.spotify.scio.values.{SCollection, SideInput}
import com.sun.jna.Native
import org.apache.beam.sdk.transforms.{DoFn, View}
import org.apache.beam.sdk.values.PCollectionView
import org.slf4j.LoggerFactory


/**
  * Main package for Annoy side input APIs.
  *
  * {{{
  * import com.spotify.scio.extra.annoy._
  * }}}
  * To save an `SCollection[(Int, Array[Float])]` to an Annoy file:
  * {{{
  * val s = sc.parallelize(Seq(1 -> Array(1.0), 2-> Array(3.5))
  *
  * // temporary location
  * val s1: SCollection[AnnoyUri] = s.asAnnoy(metric, dim)
  *
  * // specific location
  * val s2: SCollection[AnnoyUri] = s.asAnnoy(metric, dim, "gs://<bucket/<path>/<filename>")
  * }}}
  *
  * The result `SCollection[AnnoyUri]` can be converted to a side input:
  * {{{
  * val s: SCollection[AnnoyUri] = sc.parallelize(Seq("a" -> "one", "b" -> "two")).asAnnoy
  * val side: SideInput[AnnoyIndex] = s.asAnnoySideInput(metric, dim)
  * }}}
  *
  * These two steps can be done with a syntactic sugar:
  * {{{
  * val side: SideInput[AnnoyIndex] = sc
  *   .parallelize(Seq("a" -> "one", "b" -> "two"))
  *   .asAnnoySideInput(metric, dim)
  * }}}
  *
  * An existing Sparkey file can also be converted to a side input directly:
  * {{{
  * sc.annoySideInput(metric, dim, "gs://<bucket>/<path>/<filename>")
  * }}}
  *
  * Querying:
  * {{{
  * val main: SCollection[String] = sc.parallelize(Seq("a", "b", "c"))
  * val side: SideInput[SparkeyReader] = sc
  *   .parallelize(Seq("a" -> "one", "b" -> "two"))
  *   .asSparkeySideInput
  *
  * main.withSideInputs(side)
  *   .map { (x, s) =>
  *     s(side).getItem(1)
  *     s(side).getNnsByVector(1)
  *     s(side).getNnsByItem(1)
  *   }
  * }}}
  */
package object annoy {
  //TODO nicer way to do this?
  sealed trait Metric
  case object Angular extends Metric
  case object Euclidean extends Metric
  case object Manhattan extends Metric

  /** Enhanced version of [[ScioContext]] with Annoy methods */
  implicit class AnnoyScioContext(val self: ScioContext) extends AnyVal {
    /**
      * Create a SideInput of `AnnoyIndex` from an [[AnnoyUri]] base path, to be used with
      * [[com.spotify.scio.values.SCollection.withSideInputs SCollection.withSideInputs]]
      */
    def annoySideInput(metric: Metric, dim: Int, nTrees: Int, basePath: String)
    : SideInput[AnnoyIndex] = {
      val uri = AnnoyUri(metric, dim, nTrees, basePath, self.options)
      val view = self.parallelize(Seq(uri)).applyInternal(View.asSingleton())
      new AnnoySideInput(view)
    }
  }

  implicit class AnnoyPairSCollection(val self: SCollection[(Int, Array[Float])]) {
    private val logger = LoggerFactory.getLogger(this.getClass)

    /**
      * Write the key-value pairs of this SCollection as an Annoy file to a specific location.
      *
      * @return A singleton SCollection containing the [[AnnoyUri]] of the saved files
      */
    def asAnnoy(metric: Metric, dim: Int, nTrees: Int, basePath: String): SCollection[AnnoyUri] = {
      val uri = AnnoyUri(metric, dim, nTrees, basePath, self.context.options)
      require(!uri.exists, s"Annoy URI ${uri.basePath} already exists")
      logger.info(s"Saving as Annoy: $uri")

      self.transform { in =>
        in.groupBy(_ => ())
          .map { case(_, xs) =>
            val annoyIndex = new AnnoyIndex(uri)
            val it = xs.iterator
            while (it.hasNext) {
              val kv = it.next()
              annoyIndex.addItem(kv._1, kv._2)
            }
            annoyIndex.saveAndClose(Some(uri.basePath))
            uri
          }
      }
    }

    /**
      * Write the key-value pairs of this SCollection as an Annoy file to a temporary location.
      *
      *   @return A Singleton SCollection containing the [[AnnoyUri]] of the saved files
      */
    def asAnnoy(metric: Metric, dim: Int, nTrees: Int): SCollection[AnnoyUri] = {
      val uuid = UUID.randomUUID()
      val basePath = self.context.options.getTempLocation + s"/annoy-build-$uuid"
      this.asAnnoy(metric, dim, nTrees, basePath)
    }

    def asAnnoySideInput(metric: Metric, dim: Int, nTrees: Int): SideInput[AnnoyIndex] =
      self.asAnnoy(metric, dim, nTrees).asAnnoySideInput

  }


  /**
    * Enhanced version of [[com.spotify.scio.values.SCollection SCollection]] with Annoy methods
    */
  implicit class AnnoySCollection(val self: SCollection[AnnoyUri]) extends AnyVal {
    def asAnnoySideInput: SideInput[AnnoyIndex] = {
      val view = self.applyInternal(View.asSingleton())
      new AnnoySideInput(view)
    }
  }

  private class AnnoySideInput(val view: PCollectionView[AnnoyUri])
    extends SideInput[AnnoyIndex] {
    override def get[I,O](context: DoFn[I, O]#ProcessContext): AnnoyIndex =
      context.sideInput(view).getIndex
  }

  /** Wrapper around Annoy4s */
  class AnnoyIndex(uri: AnnoyUri) {

    val annoy4sIndex = uri.metric match {
      case Angular => AnnoyIndex.lib.createAngular(uri.dim)
      case Euclidean => AnnoyIndex.lib.createAngular(uri.dim)
      case Manhattan => AnnoyIndex.lib.createAngular(uri.dim)
    }

    var loaded = false

    def load(filename: Option[String] = None): AnnoyIndex = {
      loaded = AnnoyIndex.lib.load(annoy4sIndex, filename.getOrElse(uri.basePath))
      this
    }

    def unload(): Unit = {
      AnnoyIndex.lib.unload(annoy4sIndex)
      loaded = false
    }

    def build(nTrees: Int): Unit = AnnoyIndex.lib.build(annoy4sIndex, nTrees)

    def delete(): Unit  = AnnoyIndex.lib.deleteIndex(annoy4sIndex)

    def save(filename: String): Unit  = {
      AnnoyIndex.lib.save(annoy4sIndex, filename)
    }

    def saveAndClose(filename: Option[String] = None): Unit =  loaded match {
      case true => this.unload()
      case _ =>
        this.build(uri.nTrees)
        uri.save(this, filename)
        this.delete()
    }


    def addItem(item: Int, w: Array[Float]): Unit = AnnoyIndex.lib.addItem(annoy4sIndex, item, w)

    def getDistance(i: Int, j: Int): Float = AnnoyIndex.lib.getDistance(annoy4sIndex, i, j)

    // TODO defaults?
    def getNnsByItem(item: Int, n: Int, searchK: Int)
    : (Array[Int], Array[Float]) =  {
      val result = Array.fill(n)(-1)
      val distances = Array.fill(n)(-1.0f)
      AnnoyIndex.lib.getNnsByItem(annoy4sIndex, item, n, searchK, result, distances)
      (result, distances)
    }

    // TODO defaults?
    def getNnsByVector(w: Array[Float], n: Int, searchK: Int): (Array[Int], Array[Float]) = {
      val result = Array.fill(n)(-1)
      val distances = Array.fill(n)(-1.0f)
      AnnoyIndex.lib.getNnsByVector(annoy4sIndex, w, n, searchK, result, distances)
      (result, distances)
    }

    def getItem(item: Int): Array[Float] = {
      val v = new Array[Float](uri.dim)
      AnnoyIndex.lib.getItem(annoy4sIndex, item, v)
      v
    }


    /**
      * @param b set to true for verbose output
      */
    def verbose(b: Boolean): Unit = AnnoyIndex.lib.verbose(annoy4sIndex, b)

    def size: Int = AnnoyIndex.lib.getNItems(annoy4sIndex)

  }

  object AnnoyIndex {
    val lib = Native.loadLibrary("annoy", classOf[AnnoyLibrary])
      .asInstanceOf[AnnoyLibrary]
  }
}


