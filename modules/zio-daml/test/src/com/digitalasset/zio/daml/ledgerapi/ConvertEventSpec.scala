// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.zio.daml.ledgerapi

import com.daml.ledger.api.v2.event.{ArchivedEvent, Event}
import com.daml.ledger.api.v2.value.Identifier as ProtoIdentifier
import com.digitalasset.canonical.UserRight.AsAnyParty
import com.digitalasset.canonical.specific.Event.Archived
import com.digitalasset.canonical.{ContractFilter, ContractId, MetadataFilter, Party}
import com.digitalasset.transcode.schema.*
import com.digitalasset.zio.daml.KnownEntityIdentifiers
import com.digitalasset.zio.daml.ledgerapi.specific.Codecs
import zio.Chunk
import zio.test.*

object ConvertEventSpec extends ZIOSpecDefault:

  private val templateId =
    Identifier(
      PackageId("pkg"),
      PackageName("Sample"),
      PackageVersion("1.0.0"),
      ModuleName("Sample"),
      EntityName("Asset")
    )

  private val template: Template[Unit] =
    Template(
      templateId = templateId,
      payload = (),
      key = None,
      isInterface = false,
      implements = Seq.empty,
      choices = Seq.empty
    )

  private given Codecs = Dictionary(Seq.empty)

  private given KnownEntityIdentifiers =
    new KnownEntityIdentifiers(
      schema = Seq(template),
      contractFilter = ContractFilter(IdentifierFilter.AcceptAll),
      metadataFilter = MetadataFilter(IdentifierFilter.AcceptAll)
    )

  def spec = suite("convertEvent")(
    test("retains witness parties on an archived event"):
      val archived = ArchivedEvent(
        offset = 42L,
        nodeId = 3,
        contractId = "cid-1",
        templateId = Some(ProtoIdentifier("pkg", "Sample", "Asset")),
        witnessParties = Seq("Alice", "Bob"),
        packageName = "Sample",
        implementedInterfaces = Seq.empty
      )
      for converted <- specific.convertEvent(Event(Event.Event.Archived(archived)), archived.offset, AsAnyParty)
      yield converted match
        case a: Archived =>
          assertTrue(
            a.witnesses == Chunk(Party("Alice"), Party("Bob")),
            a.contractId == ContractId("cid-1"),
            a.templateId == templateId
          )
        case _ =>
          assertTrue(false)
  )
