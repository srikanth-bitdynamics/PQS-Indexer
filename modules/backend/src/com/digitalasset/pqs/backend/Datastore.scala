// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.backend

import com.digitalasset.canonical.UserRight
import com.digitalasset.canonical.specific.{Event, Offset, ReassignmentEvent, Transaction, TreeEvent}
import com.digitalasset.pqs.backend.Datastore.ProcessingSink
import zio.{Task, ZIO}
import zio.stream.ZSink

/** Defines common interface for pluggable data stores. The bridge will ask for the last known position of the data
  * store.
  *
  * The data store should return None if no check point is present (i.e. it is fresh). In this case bridge will ask to
  * process ACS first to rehydrate the state.
  *
  * The bridge will then ask to process the remaining (potentially infinite) stream of transactions starting from the
  * last known offset or offset acquired in the ACS.
  */
trait Datastore:
  /** Register this instance as active writer and removes partial transactions after the latest watermark/checkpoint.
    *
    * Abrupt termination of PQS can leave partial transactions there, so cleaning them up ensures transactions can be
    * safely re-inserted from the latest watermark onwards.
    */
  def registerActiveWriterAndCleanupTransactions: Task[Unit]

  /** Retrieve first known position of the data store if present. This is offset and ordinal index of the first
    * transaction.
    */
  def getFirstCheckpoint: Task[Datastore.Checkpoint]

  /** Retrieve last known position of the data store if present. This is offset and ordinal index of the last
    * transaction.
    */
  def getLastCheckpoint: Task[Datastore.Checkpoint]

  /** Process contract payloads from the ACS. The first item is Genesis, the last item is the absolute offset, which is
    * expected to be returned in the subsequent call to `getLastCheckpoint`
    */
  def processAcs: ProcessingSink[Event.Created | Offset]

  /** Process the remaining transactions */
  def processTransactions
      : ProcessingSink[(Transaction[Event | TreeEvent | ReassignmentEvent], Datastore.TransactionIndex)]

  /** Capabilities this datastore opts into. The defaults describe the document backend. */
  def capabilities: Datastore.Capabilities = Datastore.Capabilities()

  /** Record the ingestion coverage the pipeline resolved for this run. Datastores that do not track coverage ignore it.
    */
  def recordCoverage(record: Datastore.CoverageRecord): Task[Unit] = ZIO.unit
end Datastore

object Datastore:
  type TransactionIndex  = Long
  type Checkpoint        = (Offset, TransactionIndex)
  type ProcessingSink[A] = ZSink[Any, Throwable, A, Nothing, Unit]

  final case class Capabilities(reassignments: Boolean = false, coverage: Boolean = false)

  enum Datasource:
    case TransactionStream, TransactionTreeStream

  /** Ingestion scope the pipeline resolved for a run, from which a datastore derives its coverage metadata. */
  final case class CoverageRecord(
      requestedStart: String,
      normalizedStart: Offset,
      actualStart: Offset,
      ledgerStart: Offset,
      ledgerEnd: Offset,
      dbEnd: Offset,
      acsSeedOffset: Option[Offset],
      datasource: Datasource,
      rights: UserRight,
      contractFilter: String,
      metadataFilter: String
  )
end Datastore
