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

import akka.actor._
import ch.epfl.labos.hurricane.Config
import ch.epfl.labos.hurricane.app._
import ch.epfl.labos.hurricane.common._
import ch.epfl.labos.hurricane.frontend._
import akka.stream.scaladsl._
import ch.epfl.labos.hurricane.util.DataSource
import scala.collection.immutable.Seq
import scala.concurrent._

object Microbenchmarks extends HurricaneApplication {

  override def blueprints(appConf: AppConf): Seq[Seq[Blueprint]] =
    Seq(
      (0 until Config.HurricaneConfig.FrontendConfig.NodesConfig.machines) map (_ => Blueprint("drain", appConf, false)),
      (0 until Config.HurricaneConfig.FrontendConfig.NodesConfig.machines) map (_ => Blueprint("rewind", appConf, false)),
      (0 until Config.HurricaneConfig.FrontendConfig.NodesConfig.machines) map (_ => Blueprint("fill", appConf, false))
    )

  override def instantiate(blueprint: Blueprint)(implicit dispatcher: ExecutionContext): Props =
    blueprint.name.trim.toLowerCase match {
      case "drain" => drain(Bag(blueprint.appConf.file), blueprint.appConf.size)
      case "fill" => fill(Bag(blueprint.appConf.file))
      case "rewind" => HurricaneRewind.props(Bag(blueprint.appConf.file))
    }

  def drain(output: Bag, size: Long)(implicit dispatcher: ExecutionContext) =
    HurricaneGraphWork.props("Drain microbenchmark") {
      ParSource.multi(Config.HurricaneConfig.FrontendConfig.parallelism)(() => DataSource.zero(size)).toBag(output)
    }

  def fill(input: Bag)(implicit dispatcher: ExecutionContext) =
    HurricaneGraphWork.props("Fill microbenchmark") {
      input.toMat(Sink.ignore)(Keep.left)
    }

  def rewind(bag: Bag)(implicit dispatcher: ExecutionContext) =
    HurricaneRewind.props(bag)

}
