/*
 * Copyright (c) 2018 EPFL IC LABOS.
 * 
 * This file is part of Hurricane
 * (see https://labos.epfl.ch/hurricane).
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package ch.epfl.labos.hurricane.examples

import akka.actor.Props
import akka.stream.scaladsl.{Flow, Sink}
import ch.epfl.labos.hurricane.Config
import ch.epfl.labos.hurricane.app._
import ch.epfl.labos.hurricane.common._
import ch.epfl.labos.hurricane.frontend._
import ch.epfl.labos.hurricane.serialization.{IntFormat, LineByLineStringFormat}
import ch.epfl.labos.hurricane.util.{CompletionWatcher, DataSource, HdfsDrainer}
import com.thedeanda.lorem.LoremIpsum

import scala.collection.immutable.Seq
import scala.concurrent.ExecutionContext
import scala.language.postfixOps

/**
  * Created by JaSmiNa on 08.04.17.
  */
object Generator extends HurricaneApplication {

  override def blueprints(appConf: AppConf): Seq[Seq[Blueprint]] =
    Seq(
      (0 until Config.HurricaneConfig.FrontendConfig.NodesConfig.machines) map (_ => Blueprint("generate", appConf, false))
    )

  override def instantiate(blueprint: Blueprint)(implicit dispatcher: ExecutionContext): Props =
    blueprint.name.trim.toLowerCase match {
      case "generate" => generate(blueprint.appConf)
    }

  override def merge(phase: String, appConf: AppConf, inputs: Seq[Bag], outputs: Seq[Bag]): Option[Blueprint] =
    None

  def generate(appConf: AppConf)(implicit dispatcher: ExecutionContext) =
    HurricaneGraphWork.props("Generating data") {
      val output = Bag(appConf.file)
      val k = Config.HurricaneConfig.FrontendConfig.parallelism
      val size = appConf.size / k

      val hdfsOutput = appConf.genprops.get("hdfs").map(_.trim.toLowerCase).map{ case "0" | "no" | "disabled" => false; case _ => true } getOrElse false
      val textOutput = appConf.genprops.get("text").map(_.trim.toLowerCase).map{ case "0" | "no" | "disabled" => false; case _ => true } getOrElse false
      val wcOutput = appConf.genprops.get("wc").map(_.trim.toLowerCase).map{ case "0" | "no" | "disabled" => false; case _ => true } getOrElse false
      val relOutput = appConf.genprops.get("rel").map(_.trim.toLowerCase).map{ case "1" => Some(1); case "2" => Some(2) case _ => None} getOrElse None

      val source =
        appConf.genprops get "gentype" map (_.trim.toLowerCase) match {
          case Some("uniform") if appConf.genprops.contains("lower") && appConf.genprops.contains("upper") =>
            () => DataSource.uniform(appConf.genprops("lower").toInt, appConf.genprops("upper").toInt)
          case Some("poisson") if appConf.genprops.contains("p") =>
            () => DataSource.poisson(appConf.genprops("p").toDouble)
          case Some("zipf") if appConf.genprops.contains("n") && appConf.genprops.contains("s") =>
            () => DataSource.zipf(appConf.genprops("n").toInt, appConf.genprops("s").toDouble)
          case _ =>
            () => DataSource.zero
        }

      if(hdfsOutput) {
        val uri = appConf.genprops("uri")
        val filePath = appConf.genprops("file")
        val (promise, future) = CompletionWatcher.watch()
        val hdfsDrainer = Sink.actorSubscriber(HdfsDrainer.props(uri, filePath, Some(promise)))
        val hdfsFlow = Flow[Chunk].parallelize(k)(Flow[Chunk].cTransform[Int, String](_.map(_.toString))(IntFormat, LineByLineStringFormat)).to(hdfsDrainer)
        ParSource
          .multi(k)(source)
          .take(appConf.size / Config.HurricaneConfig.BackendConfig.DataConfig.chunkSize)
          .to(hdfsFlow).mapMaterializedValue(_ => future)
      } else if(textOutput && relOutput.isDefined) {
        val lorem = LoremIpsum.getInstance
        val first = (0 until 1024) map (_ => lorem.getFirstName) toArray
        val last = (0 until 1024) map (_ => lorem.getLastName) toArray
        val cities = (0 until 1024) map (_ => lorem.getCity) toArray
        val states = (0 until 1024) map (_ => lorem.getStateAbbr) toArray
        val random = new scala.util.Random
        val firstWithNoise = (0 until 16*1024) map (i => first(i % 1024) + (if(random.nextInt(10) == 0) "" else "-" + first((42 * i) % 1024)))

        val relFlow =
          relOutput match {
            case Some(1) =>
              Flow[Chunk].cTransform[Int, String](_.map(
                i => s"${first(math.abs((3*i) % 1024))},${last(math.abs((5*i) % 1024))},${cities(math.abs((7*i) % 1024))},${states(math.abs((11*i) % 1024))}"
              )
              )(IntFormat, LineByLineStringFormat)
            case Some(2) =>
              Flow[Chunk].cTransform[Int, String](_.map(
                i => s"${first(math.abs((3*i) % 1024))},${math.abs(i % 100)}"
              )
              )(IntFormat, LineByLineStringFormat)
            case _ =>
              Flow[Chunk].cTransform[Int, String](_.map(_.toString))(IntFormat, LineByLineStringFormat)
          }

        ParSource
          .multi(k)(source)
          .parallelize(k)(relFlow)
          .take(appConf.size / Config.HurricaneConfig.BackendConfig.DataConfig.chunkSize)
          .toBag(output)
      } else if(textOutput) {
        ParSource
          .multi(k)(source)
          .parallelize(k)(
            Flow[Chunk].cTransform[Int, String](_.map(_.toString))(IntFormat, LineByLineStringFormat)
          )
          .take(appConf.size / Config.HurricaneConfig.BackendConfig.DataConfig.chunkSize)
          .toBag(output)
      } else if(wcOutput) {
        ParSource
          .multi(k)(() => DataSource.lorem)
          .take(appConf.size / Config.HurricaneConfig.BackendConfig.DataConfig.chunkSize)
          .toBag(output)
      } else {
        ParSource
          .multi(k)(source)
          .take(appConf.size / Config.HurricaneConfig.BackendConfig.DataConfig.chunkSize)
          .toBag(output)
      }
    }

}
