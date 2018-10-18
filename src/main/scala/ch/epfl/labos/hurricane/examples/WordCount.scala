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
import akka.stream.scaladsl._
import ch.epfl.labos.hurricane._
import ch.epfl.labos.hurricane.app._
import ch.epfl.labos.hurricane.common._
import ch.epfl.labos.hurricane.frontend._
import ch.epfl.labos.hurricane.serialization._

import scala.collection._
import scala.collection.immutable.Seq
import scala.concurrent._

object WordCount extends HurricaneApplication {

  val fanout = 30

  val groupf: String => Int = { str =>
    val pos = str.charAt(0) - 'a'
    if(pos >= 0 && pos < 26) pos else 26
  }

  override def blueprints(appConf: AppConf): Seq[Seq[Blueprint]] =
    Seq(
      (0 until Config.HurricaneConfig.FrontendConfig.NodesConfig.machines) map (_ => Blueprint("phase1", appConf, Bag(appConf.file), (0 until fanout) map (i => Bag(s"wc.tmp.$i")), false)),
      (0 until fanout) map (i => Blueprint("rewind", appConf, Bag(s"wc.tmp.$i"), false)),
      (0 until fanout) map (i => Blueprint("phase2", appConf, Bag(s"wc.tmp.$i"), Bag(s"wc.$i"), true))
    )

  override def instantiate(blueprint: Blueprint)(implicit dispatcher: ExecutionContext): Props =
    blueprint.name.trim.toLowerCase match {
      case "phase1" => phase1(blueprint.inputs.head, blueprint.outputs)
      case "phase2" => phase2(blueprint.inputs.head, blueprint.outputs.head)
      case "phase2.merge" => phase2merge(blueprint.inputs, blueprint.outputs.head)
      case "rewind" => HurricaneRewind.props(blueprint.inputs.head)
    }

  override def merge(phase: String, appConf: AppConf, inputs: Seq[Bag], outputs: Seq[Bag]): Option[Blueprint] =
    phase match {
      case "phase2" => Some(Blueprint("phase2.merge", appConf, inputs, outputs, false))
      case _ => None
    }

  def phase1(input: Bag, outputs: Seq[Bag])(implicit dispatcher: ExecutionContext): Props =
    HurricaneGraphWork.props("WordCount Phase 1") {
      input
        .parallelize(Config.HurricaneConfig.FrontendConfig.parallelism)(
          Flow[Chunk]
            .cTransform[String,String](_.map(_.trim.toLowerCase).filter(w => w.length > 0 && !w.contains('\n')))
            .cSplitBy[String](fanout, groupf)
        ).toBags(outputs)
    }

  def phase2(input: Bag, output: Bag)(implicit dispatcher: ExecutionContext): Props =
    HurricaneGraphWork.props(s"WordCount Phase 2 ${input.id}") {
      val state = new WordCountState

      input
        .parallelize(Config.HurricaneConfig.FrontendConfig.parallelism)(
          Flow[Chunk]
            .cGroupBy[String,Int](state)
        ).toBag(output)
    }

  def phase2merge(inputs: Seq[Bag], output: Bag)(implicit dispatcher: ExecutionContext): Props =
    HurricaneGraphWork.props(s"WordCount Phase 2 Merge ${inputs.map(_.id).mkString(" + ")}") {
      val state = new WordCountState

      HurricaneGraph.sources(inputs)
        .mapConcat(identity)
        .cGroupBy[String,Int](state, valueParser = Some(state.parseValue))
        .toBag(output)
    }

  class WordCountState extends GroupByState[String, Int] {

    private val state = mutable.Map.empty[String, Int]

    override def update(item: String): Any = {
      state += item -> (state.getOrElse(item, 0) + 1)
    }

    override def updateValue(item: String, value: Int): Any = {
      state += item -> (state.getOrElse(item, 0) + value)
    }

    override def chunkIterator: Iterator[Chunk] =
      new Iterator[Chunk] {
        val iter = state.iterator
        var chunk = ChunkPool.allocate()
        var pusher = chunk.pusher[String]

        override def hasNext: Boolean = iter.hasNext

        override def next(): Chunk = {
          var res = 0
          do {
            val (k,v) = iter.next()
            res = pusher.put(outF(k, v))
          } while(iter.hasNext && res >= 0)
          val ret = chunk
          ret.chunkSize(pusher.pushed)
          chunk = ChunkPool.allocate()
          pusher = chunk.pusher[String]
          ret
        }
      }

    protected def outF(k: String, v: Int): String =
      s"$k:$v\n"

    def parseValue(in: String): (String, Int) =
      in.split(':').toList match {
        case key :: value :: Nil => key -> value.toInt
        case _ => throw new RuntimeException(s"Could not parse value for key-value string $in!")
      }

  }

}
