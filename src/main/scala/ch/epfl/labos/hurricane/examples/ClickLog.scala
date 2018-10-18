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
import ch.epfl.labos.hurricane.serialization.{IntFormat, LineByLineStringFormat}

import scala.collection.immutable.Seq
import scala.collection.mutable
import scala.concurrent._

object ClickLog extends HurricaneApplication {

  val fanout = 64
  val ipshift = (math.log(fanout) / math.log(2)).toInt
  val shift = 32 - ipshift

  override def blueprints(appConf: AppConf): Seq[Seq[Blueprint]] =
    Seq(
      (0 until (if(Config.HurricaneConfig.FrontendConfig.cloningEnabled) 1 else Config.HurricaneConfig.FrontendConfig.NodesConfig.machines)) map (_ => if(appConf.textMode) Blueprint("phase1t", appConf, Bag(appConf.file), (0 until fanout) map (i => Bag(s"regions.tmp.$i")), false) else Blueprint("phase1", appConf, Bag(appConf.file), (0 until fanout) map (i => Bag(s"regions.tmp.$i")), false)),
      (0 until fanout) map (i => Blueprint("rewind", appConf, Bag(s"regions.tmp.$i"), false)),
      (0 until fanout) map (i => Blueprint("phase2", appConf, Bag(s"regions.tmp.$i"), Bag(s"regions.$i"), true)),
      (0 until fanout) map (i => Blueprint("rewind", appConf, Bag(s"regions.$i"), false)),
      (0 until fanout) map (i => Blueprint("phase3", appConf, Bag(s"regions.$i"), Bag(s"regions.count.$i"), false))
    )

  override def instantiate(blueprint: Blueprint)(implicit dispatcher: ExecutionContext): Props =
    blueprint.name.trim.toLowerCase match {
      case "phase1" => phase1(blueprint.inputs.head, blueprint.outputs)
      case "phase1t" => phase1text(blueprint.inputs.head, blueprint.outputs)
      case "phase2" => phase2(blueprint.inputs.head, blueprint.outputs.head)
      case "phase2.merge" => phase2merge(blueprint.inputs, blueprint.outputs.head)
      case "phase3" => phase3(blueprint.inputs.head, blueprint.outputs.head)
      case "rewind" => HurricaneRewind.props(blueprint.inputs.head)
    }

  override def merge(phase: String, appConf: AppConf, inputs: Seq[Bag], outputs: Seq[Bag]): Option[Blueprint] =
    phase match {
      case "phase2" => Some(Blueprint("phase2.merge", appConf, inputs, outputs, false))
      case _ => None
    }

  def phase1(input: Bag, outputs: Seq[Bag])(implicit dispatcher: ExecutionContext): Props =
    HurricaneGraphWork.props("ClickLog Phase 1") {
      input
        .parallelize(Config.HurricaneConfig.FrontendConfig.parallelism)(
          Flow[Chunk]
            .cSplitBy[Int](fanout, _ >>> shift)
        ).toBags(outputs)
    }

  def phase1text(input: Bag, outputs: Seq[Bag])(implicit dispatcher: ExecutionContext): Props =
    HurricaneGraphWork.props("ClickLog Phase 1") {
      input
        .parallelize(Config.HurricaneConfig.FrontendConfig.parallelism)(
          Flow[Chunk]
            .cTransform[String, Int](_.map(_.trim).filterNot(s => s.length == 0 || s == "-").map(_.toInt))(LineByLineStringFormat, IntFormat)
            .cSplitBy[Int](fanout, _ >>> shift)
        ).toBags(outputs)
    }

  def phase2(input: Bag, output: Bag)(implicit dispatcher: ExecutionContext): Props =
    HurricaneGraphWork.props(s"ClickLog Phase 2 ${input.id}") {
      val state = new ClickLogState

      input
        .parallelize(Config.HurricaneConfig.FrontendConfig.parallelism)(
          Flow[Chunk]
            .cGroupBy[Int,Int](state)
        ).toBag(output)
    }

  def phase2merge(inputs: Seq[Bag], output: Bag)(implicit dispatcher: ExecutionContext): Props =
    HurricaneGraphWork.props(s"ClickLog Phase 2 Merge ${inputs.map(_.id).mkString(" + ")}") {
      HurricaneGraph.sources(inputs)
          .via(HurricaneGraph.reduce[Long,Long]{ case (x1, x2) => x1 | x2 })
        .toBag(output)
    }

  def phase3(input: Bag, output: Bag)(implicit dispatcher: ExecutionContext): Props =
    HurricaneGraphWork.props(s"ClickLog Phase 3 ${input.id}") {
      val counter = new ClickLogCounter

      input
        .parallelize(Config.HurricaneConfig.FrontendConfig.parallelism)(
          Flow[Chunk]
            .cMap[Long,Long](l => java.lang.Long.bitCount(l).toLong)
            .cGroupBy[Long,Long](counter)
        ).toBag(output)
    }

  class ClickLogState extends GroupByState[Int, Int] {

    private val state = new mutable.BitSet(1 << shift)

    override def update(item: Int): Any = {
      state += (item >>> ipshift)
    }

    override def updateValue(item: Int, value: Int): Any = {
      state += (item >>> ipshift) // ignore value
    }

    override def chunkIterator: Iterator[Chunk] =
      new Iterator[Chunk] {
        var chunk = ChunkPool.allocate()
        var pusher = chunk.pusher[Long]
        val iter = state.toBitMask.iterator

        override def hasNext: Boolean = iter.hasNext

        override def next(): Chunk = {
          var res = 0
          do {
            res = pusher.put(iter.next)
          } while(iter.hasNext && res >= 0)
          val ret = chunk
          ret.chunkSize(pusher.pushed)
          chunk = ChunkPool.allocate()
          pusher = chunk.pusher[Long]
          ret
        }
      }

  }

  class ClickLogCounter extends GroupByState[Long,Long] {

    private var count_ = 0L

    override def update(item: Long): Any = {
      count_ += item
    }

    override def updateValue(item: Long, value: Long): Any = {
      count_ += item // ignore value
    }

    override def chunkIterator: Iterator[Chunk] =
      new Iterator[Chunk] {
        val chunk = ChunkPool.allocate()
        val pusher = chunk.pusher[Long]
        var done = false

        override def hasNext: Boolean = done

        override def next(): Chunk = {
          pusher.put(count_)
          done = true
          chunk
        }
      }

  }

}
