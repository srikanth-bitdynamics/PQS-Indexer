package com.digitalasset.pqs.postgres.relational

import com.digitalasset.pqs.app.*
import com.digitalasset.pqs.logging.FileLogging
import com.digitalasset.pqs.postgres.backend
import com.digitalasset.pqs.postgres.relational.projection.IndexManager
import zio.Console.printLine
import zio.config.magnolia.Descriptor
import zio.jdbc.ZConnectionPool
import zio.{Task, ZIO, ZLayer}

trait RelationalIndexCli extends ComposableApp:

  protected def indexCommand: CliTree[Task[Unit]] =
    "index" @@ Command("Manage payload-local managed indexes for the active projection")
      - (indexPlanCmd | indexBuildCmd | indexAdoptCmd | indexRetireCmd | indexListCmd)

  private def indexPlanCmd =
    "plan" @@ Command("Show the managed indexes planned for the active projection; create nothing")
      - cliConfig[IndexCliConfig] `map` (config => runIndex(config)(IndexManager.planReport))

  private def indexBuildCmd =
    "build" @@ Command("Create planned indexes concurrently and validate them")
      - cliConfig[IndexCliConfig] `map` (config => runIndex(config)(IndexManager.build))

  private def indexAdoptCmd =
    "adopt" @@ Command("Adopt validated indexes and mark superseded ones for retirement")
      - cliConfig[IndexCliConfig] `map` (config => runIndex(config)(IndexManager.adopt))

  private def indexRetireCmd =
    "retire" @@ Command("Drop indexes marked for retirement concurrently")
      - cliConfig[IndexCliConfig] `map` (config => runIndex(config)(IndexManager.retire))

  private def indexListCmd =
    "list" @@ Command("List managed indexes and their lifecycle status")
      - cliConfig[IndexCliConfig] `map` (config => runIndex(config)(IndexManager.listReport))

  private def runIndex(config: ZLayer[Any, Throwable, IndexCliConfig])(
      op: ZIO[ZConnectionPool, Throwable, String]
  ): Task[Unit] =
    op.flatMap(printLine(_))
      .provide(
        com.digitalasset.pqs.appversion.LogVersion,
        config.project(_.postgres),
        backend.instanceId,
        backend.connectionPool
      )
      .bootstrap(config.project(_.logger).orElse(FileLogging.default) >>> com.digitalasset.pqs.cli.bootstrap)

final case class IndexCliConfig(
    postgres: backend.PostgresConfig,
    logger: FileLogging.Config
)
object IndexCliConfig:
  given Descriptor[IndexCliConfig] = Descriptor.derived
