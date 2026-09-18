// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.postgres.backend

import com.digitalasset.canonical.specific.Offset
import com.digitalasset.pqs.postgres.backend.copy.Watermark
import zio.{Chunk, Ref, ZIO}
import zio.stream.ZStream
import zio.test.*

object WritePathSpec extends ZIOSpecDefault:

  private def watermark(ix: Long): Watermark = Watermark(ix, Offset.Absolute(ix * 10), Seq(ix))

  def spec = suite("write path")(
    test("IdPlaceholder.factory allocates consecutive ids from start + 1"):
      val factory = IdPlaceholder.factory(10L)
      assertTrue(factory.mk.id == 11L, factory.mk.id == 12L, factory.mk.id == 13L)
    ,
    test("waitPoint passes elements through in order"):
      for out <- ZStream.range(0, 100).via(waitPoint("test_wp", capacity = 8, chunkSize = 4)).runCollect
      yield assertTrue(out == Chunk.fromIterable(0 until 100))
    ,
    test("checkpoint order and latency samples survive every completion order"):
      ZIO
        .foreach((1L to 4L).toVector.permutations.toVector) { order =>
          ZStream
            .fromIterable(order)
            .map(ix => Chunk(watermark(ix)))
            .rechunk(1)
            .via(copy.reorderCheckpoints(ZIO.succeed(watermark(0))))
            .runCollect
            .map(out =>
              assertTrue(
                out.last.ix == 4L,
                out.map(_.ix) == out.map(_.ix).distinct.sorted,
                out.forall(wm => wm.offset == Offset.Absolute(wm.ix * 10)),
                out.flatMap(_.seenAts) == Chunk(1L, 2L, 3L, 4L)
              )
            )
        }
        .map(_.reduce(_ && _))
    ,
    test("a gap keeps later checkpoints unpublished, including at end of stream"):
      for out <- ZStream(Chunk(watermark(13)), Chunk(watermark(11)), Chunk(watermark(14)))
          .rechunk(1)
          .via(copy.reorderCheckpoints(ZIO.succeed(watermark(10))))
          .runCollect
      yield assertTrue(out == Chunk(watermark(11)))
    ,
    test("checkpoint state is fresh for each stream run"):
      val pipeline = copy.reorderCheckpoints(ZIO.succeed(watermark(10)))
      val stream   = ZStream(Chunk(watermark(12)), Chunk(watermark(11))).rechunk(1).via(pipeline)
      for
        first  <- stream.runCollect
        second <- stream.runCollect
      yield assertTrue(first == second, first.last.ix == 12L, first.flatMap(_.seenAts) == Chunk(11L, 12L))
    ,
    test("watermark persistence failure stops subsequent progress"):
      val failure = new RuntimeException("commit failed")
      for
        attempted <- Ref.make(List.empty[Long])
        exit <- ZStream(watermark(1), watermark(2))
          .via(copy.handleWatermarks(wm => attempted.update(_ :+ wm.ix) *> ZIO.fail(failure)))
          .runDrain
          .exit
        indexes <- attempted.get
      yield assertTrue(exit.causeOption.flatMap(_.failureOption).contains(failure), indexes == List(1L))
    ,
    test("batching preserves all models across the threshold and flushes the final batch"):
      val models = Chunk.fromIterable((1L to 10005L).map(watermark))
      for out <- ZStream
          .fromIterable(models)
          .map(wm => Chunk[copy.Model](wm))
          .rechunk(1)
          .via(copy.batchStatements)
          .runCollect
      yield assertTrue(out.flatten == models)
  )
