// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.postgres.backend

import com.digitalasset.canonical.{ContractId, Party}
import com.digitalasset.canonical.specific.Offset
import com.digitalasset.pqs.o11y.traces
import com.digitalasset.pqs.o11y.metrics.latency
import com.digitalasset.pqs.o11y.traces.{DetachedSpan, given}
import io.opentelemetry.api.trace.SpanContext
import org.apache.commons.text.translate.LookupTranslator
import org.postgresql.PGConnection
import ujson.Value
import zio.ZIO.{logDebug, logInfo, logTrace}
import zio.jdbc.shims.postgres.PGRestorableConnection
import zio.jdbc.{JdbcDecoder, ZConnection}
import zio.metrics.{Metric, MetricLabel}
import zio.metrics.MetricKeyType.Histogram.Boundaries
import zio.stream.{ZChannel, ZPipeline, ZSink}
import zio.{Chunk, ChunkBuilder, Schedule, ZIO, durationInt}

import java.io.StringReader
import java.lang.System.lineSeparator
import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneOffset}
import scala.collection.mutable
import scala.jdk.CollectionConverters.*
import scala.language.implicitConversions

object encoding:
  trait ValueConverter[A] { def convert(value: A): String }

  def values: RowValues = RowValues()

  given conv: Conversion[RowValues, String] = _.toString

  final class RowValues:
    private val sb                = StringBuilder()
    override def toString: String = sb.result()
    def apply[A](value: A)(using c: ValueConverter[A]): RowValues =
      if sb.nonEmpty then sb.append("\t")
      sb.append(c.convert(value))
      this

  implicit val offsetEncoder: JdbcDecoder[Offset] = (ix, rs) => (ix, Offset.Absolute(rs.getLong(ix)))

  extension (offset: Offset) def toSqlValue: Long = offset.toLongOffset

  // https://www.postgresql.org/docs/current/sql-copy.html
  private val escaper = new LookupTranslator(
    Map(
      "\b" -> "\\b",
      "\f" -> "\\f",
      "\n" -> "\\n",
      "\r" -> "\\r",
      "\t" -> "\\t",
      ""  -> "\\v",
      "\\" -> "\\\\"
    ).asJava
  )

  def escape(value: String): String = escaper.translate(value)

  given booleanConverter: ValueConverter[Boolean]       = value => value.toString
  given numericConverter[A: Numeric]: ValueConverter[A] = value => value.toString
  given stringConverter: ValueConverter[String]         = value => escape(value)
  given contractIdConverter: ValueConverter[ContractId] = value => value
  given partyConverter: ValueConverter[Party]           = value => value
  given idConverter: ValueConverter[IdPlaceholder]      = value => value.id.toString
  given jsonConverter: ValueConverter[Value]            = value => escape(value.toString)

  given byteArrayConverter: ValueConverter[Array[Byte]] =
    val HEX_DIGITS = Array('0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F')
    value =>
      if value.isEmpty then ""
      else
        val sb = StringBuilder()
        sb.append("\\\\x")
        value.foreach(b => sb.append(HEX_DIGITS(b >> 4 & 15)).append(HEX_DIGITS(b & 15)))
        sb.result()

  given instantConverter: ValueConverter[Instant] =
    val fmt = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSXX")
    value => value.atZone(ZoneOffset.UTC).format(fmt)

  given optionalConverter[A: ValueConverter]: ValueConverter[Option[A]] =
    case Some(value) => summon[ValueConverter[A]].convert(value)
    case None        => "\\N"

  given iterableConverter[A: ValueConverter]: ValueConverter[Seq[A]] =
    value => value.map(summon[ValueConverter[A]].convert).mkString("{", ",", "}")
end encoding

object copy:
  trait CopyTable { def name: String }

  trait Model { def labels: Set[MetricLabel] }
  trait Copy extends Model:
    def _table: CopyTable
    def _sql: String
    def _row: String
  trait TransactionCopy extends Copy:
    def ix: Long
    def offset: Offset
    def span: Option[DetachedSpan]

  final case class Watermark(
      ix: Long,
      offset: Offset,
      seenAts: Seq[Long],
      txSpans: Seq[DetachedSpan] = Seq.empty,
      persistSpans: Seq[SpanContext] = Seq.empty
  ) extends Model:
    val labels = l("type" -> "watermark")

  given watermarkOrdering: Ordering[Watermark] = Ordering.by(_.ix)

  def l(kv: (String, Any)*): Set[MetricLabel] = kv.map((k, v) => MetricLabel(k, v.toString)).toSet

  extension (models: Iterable[Model])
    def onlyTransactions(): Iterable[TransactionCopy] = models.view.collect { case t: TransactionCopy => t }
    def onlyCopies(): Iterable[Copy]                  = models.view.collect { case t: Copy => t }

  extension (tx: TransactionCopy)
    def ifTraced[R, E, A](zio: DetachedSpan => ZIO[R, E, A]) = ZIO.whenCase(tx.span) { case Some(s) => zio(s) }

  private val BatchEntitiesThreshold = 10_000
  private val BatchReleaseWindow     = 200.millis

  /** Groups multiple SQL actions into large batches of SQL IO to be executed in single transactions unordered. */
  def batchStatements =
    ZPipeline[Chunk[Model]]
      .aggregateAsyncWithin(
        ZSink.foldChunks( // start with:
          ChunkBuilder.make[Model]() -> 0
        ) { // continue while:
          (acc, size) => size < BatchEntitiesThreshold
        } { // accumulate:
          case ((acc, size), in) =>
            var s = size
            for chunk <- in; elem <- chunk do { acc.addOne(elem); s += 1 }
            (acc, s)
        },
        Schedule.spaced(BatchReleaseWindow) // release batch regularly even if not full
      )
      .map(_._1.result())
      .tap { models =>
        ZIO.foreachDiscard(models.onlyTransactions())(_.ifTraced(_.addEvent("released transaction model into batch")))
      }
      .tap(x => logDebug(s"Aggregated ${x.length} SQL fragments into single batch"))

  /** Upstream statements were executed out of order, this pipeline restores the consecutive order of indexes */
  def reorderCheckpoints(initial: ZIO[Any, Throwable, Watermark]) =
    type AccumulatorChannel =
      ZChannel[Any, Nothing, Chunk[Chunk[Watermark]], Any, Nothing, Chunk[Watermark], Unit]
    def accumulator(state: mutable.ArrayBuffer[Watermark]): AccumulatorChannel = ZChannel.readWithCause(
      in => {
        for chunk <- in do state.addAll(chunk)
        state.sortInPlace()
        val consecutive = (state.view zip state.view.drop(1)).takeWhile { (prev, next) => prev.ix + 1 == next.ix }
        consecutive.lastOption match
          case Some((_, value)) =>
            // Gather all span refs (to individual txs & batches) up to advancing watermark
            // ignoring head of `state` since it had already advanced by now
            val advancing = state.view.slice(1, consecutive.size + 1).toVector
            val seenAts   = advancing.flatMap(_.seenAts)
            val txs       = advancing.flatMap(_.txSpans)
            val batches   = advancing.flatMap(_.persistSpans).distinct
            // `value` becomes the new head of `state` :)
            state.remove(0, consecutive.size)
            val effectiveWatermark = value.copy(seenAts = seenAts, txSpans = txs, persistSpans = batches)
            ZChannel.write(Chunk(effectiveWatermark)) *> accumulator(state)
          case None =>
            accumulator(state)
      },
      err => ZChannel.refailCause(err),
      _ => ZChannel.unit
    )
    ZPipeline.unwrap(
      initial
        .map(start =>
          ZPipeline.fromChannel[Any, Nothing, Chunk[Watermark], Watermark](
            accumulator(mutable.ArrayBuffer(start))
          )
        )
    )

  /** Update watermarks */
  def handleWatermarks(updateWatermark: Watermark => ZIO[Any, Throwable, Any]) =
    val trackWatermark = latency("pipeline_progress_watermark", "Latency of watermark progression")
    val watermarkIx = Metric
      .gauge("watermark_ix", "Current watermark index (transaction ordinal number for consistent reads)")
      .contramap[Long](_.toDouble)
    val txProcessingLatency = Metric
      .histogram(
        "total_tx_handling_latency",
        "Total transaction handling latency in pqs",
        Boundaries.exponential(0.001, math.pow(10, 1.0 / 3), 13)
      )
      .contramap[Long](_.toDouble / 1e9)
    ZPipeline[Watermark].mapZIO(wm =>
      traces.span("advance datastore watermark") {
        trackWatermark(updateWatermark(wm))
          @@ traces.attributes(
            "pqs.watermark.offset" -> wm.offset.toLongOffset,
            "pqs.watermark.ix"     -> wm.ix
          )
          *> ZIO.foreachDiscard(wm.txSpans) { s =>
            s.linkToCurrentSpan("target" -> "↧ advance watermark")
              *> s.addEvent(
                "advanced datastore watermark",
                "offset" -> wm.offset.toLongOffset,
                "index"  -> wm.ix
              )
              *> s.end()
          }
          *> ZIO.foreachDiscard(wm.persistSpans) { s =>
            ZIO.unit @@ traces.link(s, "target" -> "↥ persist to datastore")
          }
          *> zio.Clock.nanoTime.flatMap(now =>
            ZIO.foreachDiscard(wm.seenAts) { seenAt => txProcessingLatency.update(now - seenAt) }
          )
          *> watermarkIx.update(wm.ix)
          *> logInfo(s"Advanced watermark: ix = ${wm.ix}, offset = ${wm.offset.toLongOffset}")
      }
    )

  object Model:
    private val counter = Metric.counter("pipeline_events", "Processed ledger events")

    def prepareStatement(
        all: Iterable[Model],
        statTables: Seq[CopyTable]
    ): ZIO[ZConnection, Throwable, Chunk[Watermark]] =
      val copies                      = mutable.LinkedHashMap.empty[String, mutable.ListBuffer[String]]
      val watermarks                  = ChunkBuilder.make[Watermark]()
      val txs                         = all.onlyTransactions().toVector
      val txsByIx                     = txs.iterator.map(tx => tx.ix -> tx).toMap
      val batchContents               = all.onlyCopies().groupMapReduce(_._table)(_ => 1L)(_ + _)
      def statAttribute(t: CopyTable) = s"pqs.${t.name}.rows_count" -> batchContents.getOrElse(t, 0L)
      all.foreach {
        case c: Copy      => copies.getOrElseUpdate(c._sql, mutable.ListBuffer.empty).addOne(c._row)
        case w: Watermark => watermarks.addOne(w.copy(txSpans = txsByIx.get(w.ix).flatMap(_.span).toList))
      }

      val forcedCopies = copies.view
        .map { (sql, rows) => (sql, rows.view.mkString(lineSeparator())) }
        .toSeq
        .sortBy(_._1)
      val copyIO = ZIO.serviceWithZIO[ZConnection](
        _.access { conn =>
          @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
          val api = conn.asInstanceOf[PGRestorableConnection].underlying.asInstanceOf[PGConnection].getCopyAPI
          forcedCopies.foreach { (sql, rows) => api.copyIn(sql, StringReader(rows)) }
        } <* logTrace(
          s"SQL:${lineSeparator()}" +
            s"${forcedCopies.map(x => s"${x._1}${lineSeparator()}${x._2}").mkString(lineSeparator())}"
        )
      )

      val metricsIO = ZIO.foreachDiscard(
        all.view.filter(_.labels.nonEmpty).groupMapReduce(_.labels)(_ => 1)(_ + _)
      )(
        counter.tagged(_).update(_)
      )

      traces.span("execute SQL") {
        copyIO @@ traces.attributes(statTables.map(statAttribute)*)
      } *>
        metricsIO *>
        ZIO.foreachDiscard(txs.flatMap(_.span)) { s =>
          s.linkToCurrentSpan("target" -> "↧ persist to datastore") *>
            s.addEvent("flushed transaction model SQL to datastore")
        } *>
        traces.currentSpan().map { s => watermarks.result().map(_.copy(persistSpans = Seq(s.getSpanContext))) }
  end Model
end copy
