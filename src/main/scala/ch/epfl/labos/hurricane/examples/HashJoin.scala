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

import akka.Done
import akka.actor._
import akka.stream._
import akka.stream.scaladsl._
import ch.epfl.labos.hurricane.Config
import ch.epfl.labos.hurricane.app._
import ch.epfl.labos.hurricane.common._
import ch.epfl.labos.hurricane.frontend._
import ch.epfl.labos.hurricane.serialization.LineByLineStringFormat

import scala.collection.immutable.Seq
import scala.concurrent._
import scala.util.{Failure, Success}

object HashJoin extends HurricaneApplication {

  val separator = ','

  override def blueprints(appConf: AppConf): Seq[Seq[Blueprint]] =
    if(appConf.hashprops.getOrElse("nobcast", "0") == "1") {
      Seq(
        Seq(Blueprint("phase1", appConf, false), Blueprint("phase1b", appConf, false))
      )
    } else {
      Seq(
        (0 until Config.HurricaneConfig.FrontendConfig.NodesConfig.machines) map (_ => Blueprint("phase1", appConf, false))
      )
    }

  override def instantiate(blueprint: Blueprint)(implicit dispatcher: ExecutionContext): Props =
    blueprint.name.trim.toLowerCase match {
      case "phase1" => phase1(Bag(blueprint.appConf.hashprops getOrElse ("rel1", "rel1.csv")), Bag(blueprint.appConf.hashprops getOrElse ("rel2", "rel2.csv")), Bag(blueprint.appConf.file))
      case "phase1b" => phase1(Bag(blueprint.appConf.hashprops getOrElse ("rel3", "rel3.csv")), Bag(blueprint.appConf.hashprops getOrElse ("rel4", "rel4.csv")), Bag(blueprint.appConf.file + "_b"))
    }

  def phase1(in1: Bag, in2: Bag, output: Bag)(implicit dispatcher: ExecutionContext): Props =
    HashJoinActor.props(in1, in2, output)

  class HashJoinState(state: Map[String, String], output: Boolean = true) extends JoinState[String, String] {
    override def offer(item: String): Option[String] = {
      if(output) Some(item.trim + separator + item.trim) else None
    }
      //state get key(item) map (o => item.trim + separator + o.trim)

    private def key(item: String): String = {
      val idx = item.indexOf(',')
      if(idx > 0) {
        item.substring(0, idx)
      } else {
        "#!MAGICVALUETHATDOESNOTMATCHWITHANYTHING!#"
      }
    }
  }

  object HashJoinActor {

    val name: String = "hashjoin"

    def props(in1: Bag, in2: Bag, output: Bag): Props = Props(classOf[HashJoinActor], in1.id, in2.id, output.id)

    case class State(relation: Map[String, String])

  }

  class HashJoinActor(in1: Bag, in2: Bag, output: Bag) extends HurricaneWork {

    import HashJoinActor._
    import context.dispatcher

    implicit val materializer = ActorMaterializer()

    override val name = s"HashJoin Phase 1 ${in1.id} |x| ${in2.id} -> ${output.id}"

    override def start(): Unit = {
      val me = self
      in2
        .parallelize(Config.HurricaneConfig.FrontendConfig.parallelism)(
          Flow[Chunk]
            .map(_.iterator[String](LineByLineStringFormat))
            .map(_ flatMap (str => str.split(separator.toString, 2) match { case Array(k, v) => Some(k -> v); case _ => None }))
            .mapConcat(_.to[scala.collection.immutable.Iterable])
        ).runWith(Sink.seq) onComplete {
        case Success(relation) =>
          log.info(s"Successfully read in relation ${in2.id}!")
          self ! State(relation.toMap)
        case Failure(e) =>
          log.error(e, s"Failed to read in relation ${in2.id}!")
          context stop self
      }
    }

    override def workerReceive = {
      case State(relation) =>
        val state = new HashJoin.HashJoinState(relation, in2.id != "rel4")

        val graph = in1
          .parallelize(Config.HurricaneConfig.FrontendConfig.parallelism)(
            Flow[Chunk]
              .cJoin[String,String](state)(LineByLineStringFormat, LineByLineStringFormat)
          ).toBag(output)

        val me = self
        graph.mapMaterializedValue(_.onComplete(_ => me ! Done)).run()

      case Done =>
        context stop self
    }

  }

}

