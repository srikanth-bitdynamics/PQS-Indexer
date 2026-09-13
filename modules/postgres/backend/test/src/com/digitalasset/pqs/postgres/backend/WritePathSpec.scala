// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.postgres.backend

import zio.Chunk
import zio.stream.ZStream
import zio.test.*

object WritePathSpec extends ZIOSpecDefault:

  def spec = suite("write path")(
    test("IdPlaceholder.factory allocates consecutive ids from start + 1"):
      val factory = IdPlaceholder.factory(10L)
      assertTrue(factory.mk.id == 11L, factory.mk.id == 12L, factory.mk.id == 13L)
    ,
    test("waitPoint passes elements through in order"):
      for out <- ZStream.range(0, 100).via(waitPoint("test_wp", capacity = 8, chunkSize = 4)).runCollect
      yield assertTrue(out == Chunk.fromIterable(0 until 100))
  )
