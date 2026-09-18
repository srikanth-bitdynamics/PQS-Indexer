// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.postgres.relational

import com.digitalasset.pqs.app.*
import com.digitalasset.pqs.logging.FileLogging
import com.digitalasset.pqs.postgres.backend
import zio.{Task, ZIO, ZLayer}
import zio.Console.printLine
import zio.config.magnolia.{Descriptor, describe}
import zio.jdbc.*

trait RelationalMaintenanceCli extends ComposableApp:
  protected def maintenanceCommand: CliTree[Task[Unit]] =
    ("prune" @@ Command("Remove archived relational history through an offset; stop ingestion first")
      - cliConfig[RelPruneConfig] `map` (config =>
        runMaintenance(config.project(_.postgres), config.project(_.logger)) {
          config.build.flatMap { env =>
            val options = env.get.prune
            val dryRun = options.mode match
              case RelPruneMode.DryRun => true
              case RelPruneMode.Force  => false
            backend
              .transact(
                sql"""select pruning_boundary_offset is not null, deleted_contracts, deleted_exercises,
                    deleted_events, deleted_transactions from __rel_prune(${options.offset}, $dryRun)"""
                  .query[(Boolean, Long, Long, Long, Long)]
                  .selectOne
              )
              .flatMap {
                case Some((true, contracts, exercises, events, transactions)) =>
                  printLine(
                    s"${if dryRun then "Dry run" else "Pruned"} through ${options.offset}: $contracts contracts, $exercises exercises, $events events, $transactions transactions"
                  )
                case _ => printLine("Nothing to prune")
              }
          }
        }
      )) |
      ("redact" @@ Command("Remove stored payloads; stop ingestion first") - (
        ("contract" @@ Command("Redact an archived contract, its interface views and exercise payloads")
          - cliConfig[RelRedactContractConfig] `map` (config =>
            runMaintenance(config.project(_.postgres), config.project(_.logger)) {
              config.build.flatMap { env =>
                val options = env.get.redact
                backend
                  .transact(
                    sql"select redact_contract(${options.contractId}, ${options.redactionId})"
                      .query[Long]
                      .selectOne
                  )
                  .flatMap(n => printLine(s"Redacted ${n.getOrElse(0L)} contract(s)"))
              }
            }
          )) |
          ("exercise" @@ Command("Redact an exercise argument and result by event offset and node")
            - cliConfig[RelRedactExerciseConfig] `map` (config =>
              runMaintenance(config.project(_.postgres), config.project(_.logger)) {
                config.build.flatMap { env =>
                  val options = env.get.redact
                  backend
                    .transact(
                      sql"select redact_exercise(${options.offset}, ${options.node}, ${options.redactionId})"
                        .query[Long]
                        .selectOne
                    )
                    .flatMap(n => printLine(s"Redacted ${n.getOrElse(0L)} exercise(s)"))
                }
              }
            ))
      ))

  private def runMaintenance(
      postgres: ZLayer[Any, Throwable, backend.PostgresConfig],
      logger: ZLayer[Any, Throwable, FileLogging.Config]
  )(body: ZIO[zio.Scope & ZConnectionPool, Throwable, Unit]): Task[Unit] =
    ZIO
      .scoped(body)
      .provide(postgres, backend.instanceId, backend.connectionPool)
      .bootstrap(logger.orElse(FileLogging.default) >>> com.digitalasset.pqs.cli.bootstrap)

enum RelPruneMode:
  case DryRun, Force

final case class RelPruneOptions(
    @describe("Inclusive ledger offset through which archived history is removed") offset: Long,
    mode: RelPruneMode = RelPruneMode.DryRun
)
final case class RelRedactContractOptions(contractId: String, redactionId: String)
final case class RelRedactExerciseOptions(offset: Long, node: Int, redactionId: String)

final case class RelPruneConfig(postgres: backend.PostgresConfig, logger: FileLogging.Config, prune: RelPruneOptions)
object RelPruneConfig:
  given Descriptor[RelPruneConfig] = Descriptor.derived

final case class RelRedactContractConfig(
    postgres: backend.PostgresConfig,
    logger: FileLogging.Config,
    redact: RelRedactContractOptions
)
object RelRedactContractConfig:
  given Descriptor[RelRedactContractConfig] = Descriptor.derived

final case class RelRedactExerciseConfig(
    postgres: backend.PostgresConfig,
    logger: FileLogging.Config,
    redact: RelRedactExerciseOptions
)
object RelRedactExerciseConfig:
  given Descriptor[RelRedactExerciseConfig] = Descriptor.derived
