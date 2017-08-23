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

import java.io.File
import java.net.URI
import java.nio.file.{Files, Paths}

import com.spotify.scio.util.{RemoteFileUtil, ScioUtil}
import org.apache.beam.sdk.extensions.gcp.options.GcsOptions
import org.apache.beam.sdk.options.PipelineOptions

import scala.collection.JavaConverters._

trait AnnoyUri extends Serializable {
  val basePath: String
  val metric: Metric
  val dim: Int
  val nTrees: Int
  private[annoy] def getIndex: AnnoyIndex
  private[annoy] def save(annoyIndex: AnnoyIndex, filename: Option[String]): Unit
  private[annoy] def exists: Boolean
  override def toString: String = basePath
}

private[annoy] object AnnoyUri {
  def apply(metric: Metric, dim: Int, nTrees: Int, basePath: String, opts: PipelineOptions)
  : AnnoyUri =
    if (ScioUtil.isLocalUri(new URI(basePath))) {
      new LocalAnnoyUri(metric, dim, nTrees, basePath)
    } else {
      new RemoteAnnoyUri(metric, dim, nTrees, basePath, opts.as(classOf[GcsOptions]))
    }
}

private class LocalAnnoyUri(val metric: Metric, val dim: Int, val nTrees: Int,
                            val basePath: String) extends AnnoyUri {
  override private[annoy] def exists: Boolean = new File(basePath).exists()
  override private[annoy] def getIndex: AnnoyIndex = new AnnoyIndex(this).load()

  override private[annoy] def save(annoyIndex: AnnoyIndex, filename: Option[String]): Unit
  = {
    annoyIndex.save(filename.getOrElse(basePath))
  }
}

private class RemoteAnnoyUri(val metric: Metric, val dim: Int, val nTrees: Int,
                             val basePath: String, options: PipelineOptions) extends AnnoyUri {
  val rfu: RemoteFileUtil = RemoteFileUtil.create(options)

  override private[annoy] def exists: Boolean = rfu.remoteExists(new URI(basePath))

  override private[annoy] def getIndex: AnnoyIndex =
  {
    val paths = rfu.download(new URI(basePath)).asScala
    new AnnoyIndex(this).load(Some(paths.head.getFileName.toString))
  }

  override private[annoy] def save(annoyIndex: AnnoyIndex, filename: Option[String]): Unit
  = {
    val tempFile = Files.createTempDirectory("annoy").resolve("data").toString
    annoyIndex.save(tempFile)
    rfu.upload(Paths.get(tempFile), new URI(filename.getOrElse(basePath)))
  }
}


