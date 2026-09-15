package com.digitalasset.pqs.postgres.relational.projection

import ujson.Value
import zio.ZIO
import zio.jdbc.*

import java.security.MessageDigest

object IndexManager:
  import IndexPlanner.OrderKey

  val contractsColumns: Set[String] = Set("created_tx_ix", "created_at_offset", "contract_pk")
  private val includeColumn         = "contract_pk"

  final case class CoveredShape(
      filter: Seq[String],
      order: Seq[OrderKey],
      physical: Seq[String],
      filterComplete: Boolean,
      orderExternal: Boolean,
      fullCoverage: Boolean,
      residual: Seq[OrderKey]
  )

  final case class PlannedIndex(
      table: String,
      name: String,
      keys: Seq[OrderKey],
      include: Seq[String],
      covered: Seq[CoveredShape]
  ):
    def columns: Seq[String] = keys.map(_.column)

    def definition: String =
      val cols = keys.map(k => quoteIdent(k.column) + (if k.ascending then "" else " desc")).mkString(", ")
      val incl = include.map(quoteIdent).mkString(", ")
      s"create index concurrently if not exists ${quoteIdent(name)} on ${quoteIdent(table)} ($cols) include ($incl)"

  final case class Plan(indexes: Seq[PlannedIndex], diagnostics: Seq[String], notes: Seq[String])

  private final case class Classified(
      filter: Seq[String],
      order: Seq[OrderKey],
      payloadFilter: Seq[String],
      externalFilter: Seq[String],
      payloadOrder: Seq[OrderKey],
      residualOrder: Seq[OrderKey],
      unknown: Seq[String]
  )

  def plan(
      definitions: Map[String, ProjectionDefinition],
      shapes: Map[String, Map[String, Seq[String]]],
      baseTables: Map[String, String]
  ): Plan =
    val perName = definitions.toSeq.sortBy(_._1).map { (name, definition) =>
      planProjection(name, definition, shapes.getOrElse(name, Map.empty), baseTables)
    }
    Plan(
      perName.flatMap(_._1),
      perName.flatMap(_._2).distinct.sorted,
      perName.flatMap(_._3).distinct.sorted
    )

  private def planProjection(
      name: String,
      definition: ProjectionDefinition,
      shapesForName: Map[String, Seq[String]],
      baseTables: Map[String, String]
  ): (Seq[PlannedIndex], Seq[String], Seq[String]) =
    val perTemplate = definition.templates.distinct.sorted.map { qualified =>
      baseTables.get(qualified) match
        case None => (Seq.empty[PlannedIndex], Seq.empty[String], Seq.empty[String])
        case Some(table) =>
          val promoted = shapesForName.getOrElse(qualified, Seq.empty).toSet
          planTable(name, qualified, table, promoted, definition.queries)
    }
    (perTemplate.flatMap(_._1), perTemplate.flatMap(_._2), perTemplate.flatMap(_._3))

  private def planTable(
      name: String,
      qualified: String,
      table: String,
      promoted: Set[String],
      queries: Seq[ProjectionQuery]
  ): (Seq[PlannedIndex], Seq[String], Seq[String]) =
    val classifieds = queries.map(classify(_, promoted))
    val diagnostics = classifieds.filter(_.unknown.nonEmpty).map { c =>
      s"projection '$name' on $qualified: query ${shapeText(c)} references non-promoted column(s): " +
        c.unknown.distinct.sorted.mkString(", ") + "; skipped"
    }
    val usable                = classifieds.filter(_.unknown.isEmpty)
    val (indexable, external) = usable.partition(c => c.payloadFilter.nonEmpty || c.payloadOrder.nonEmpty)
    val notes = external.map(c =>
      s"projection '$name' on $qualified: query ${shapeText(c)} has no payload-indexable columns; " +
        "served by __rel_contracts lifecycle indexes"
    )
    val payloadReqs = indexable.map(c => ProjectionQuery(c.payloadFilter, c.payloadOrder.map(tokenOf)))
    val specs       = IndexPlanner.plan(payloadReqs, promoted).indexes
    val attributed  = indexable.map(c => (c, specs.filter(covers(_, c)).maxByOption(_.columns.length)))
    val indexes = specs.flatMap { spec =>
      attributed.collect { case (c, Some(s)) if specEq(s, spec) => c } match
        case Seq() => Seq.empty[PlannedIndex]
        case forSpec =>
          val keys = spec.equality.map(OrderKey(_, true)) ++ spec.ordered
          Seq(PlannedIndex(table, indexName(table, keys), keys, Seq(includeColumn), forSpec.map(covered(_, spec))))
    }
    (indexes, diagnostics, notes)

  private def classify(query: ProjectionQuery, promoted: Set[String]): Classified =
    val parsed         = query.order.map(IndexPlanner.parseOrder)
    val payloadFilter  = query.filter.filter(promoted.contains)
    val externalFilter = query.filter.filter(contractsColumns.contains)
    val unknownFilter  = query.filter.filterNot(c => promoted.contains(c) || contractsColumns.contains(c))
    val payloadOrder   = parsed.takeWhile(k => promoted.contains(k.column))
    val rest           = parsed.drop(payloadOrder.length)
    val residualOrder  = rest.filter(k => contractsColumns.contains(k.column))
    val unknownOrder   = rest.filterNot(k => contractsColumns.contains(k.column)).map(_.column)
    Classified(
      query.filter,
      parsed,
      payloadFilter,
      externalFilter,
      payloadOrder,
      residualOrder,
      unknownFilter ++ unknownOrder
    )

  private def covered(c: Classified, spec: IndexPlanner.IndexSpec): CoveredShape =
    CoveredShape(
      c.filter,
      c.order,
      spec.columns,
      c.externalFilter.isEmpty,
      c.residualOrder.nonEmpty,
      c.externalFilter.isEmpty && c.residualOrder.isEmpty,
      c.residualOrder
    )

  private def covers(spec: IndexPlanner.IndexSpec, c: Classified): Boolean =
    val equalitySet = spec.equality.toSet
    c.payloadFilter.forall(equalitySet.contains) && isPrefix(c.payloadOrder, spec.ordered)

  private def isPrefix(xs: Seq[OrderKey], ys: Seq[OrderKey]): Boolean =
    xs.length <= ys.length && xs.corresponds(ys.take(xs.length))(sameKey)

  private def specEq(a: IndexPlanner.IndexSpec, b: IndexPlanner.IndexSpec): Boolean =
    a.equality.corresponds(b.equality)(strEq) && a.ordered.corresponds(b.ordered)(sameKey)

  private def sameKey(a: OrderKey, b: OrderKey): Boolean =
    strEq(a.column, b.column) && ((a.ascending, b.ascending) match
      case (true, true)   => true
      case (false, false) => true
      case _              => false
    )

  private def strEq(a: String, b: String): Boolean =
    a.compareTo(b) match
      case 0 => true
      case _ => false

  private def tokenOf(k: OrderKey): String = if k.ascending then k.column else s"${k.column} desc"

  private def shapeText(c: Classified): String =
    s"filter=[${c.filter.mkString(", ")}] order=[${c.order.map(tokenOf).mkString(", ")}]"

  private def quoteIdent(name: String): String = "\"" + name.replace("\"", "\"\"") + "\""

  private def indexName(table: String, keys: Seq[OrderKey]): String =
    val tokens = keys.map(k => if k.ascending then k.column else s"${k.column}_desc")
    val raw    = s"$table|${tokens.mkString(",")}"
    val slug   = s"${table}_${tokens.mkString("_")}".toLowerCase.replaceAll("[^a-z0-9]", "_")
    val prefix = "rix_"
    val budget = 63 - prefix.length - 14
    s"$prefix${slug.take(budget)}_h${md5(raw).take(12)}"

  private def md5(s: String): String =
    MessageDigest.getInstance("MD5").digest(s.getBytes("UTF-8")).map(b => f"${b & 0xff}%02x").mkString

  private def shapeJson(s: CoveredShape): Value =
    val requested = s.filter.map(f => ujson.Obj("column" -> f, "kind" -> "eq")) ++
      s.order.map(k =>
        ujson.Obj("column" -> k.column, "kind" -> "order", "direction" -> (if k.ascending then "asc" else "desc"))
      )
    val orderCoverage = if s.order.isEmpty then "absent" else if s.orderExternal then "external" else "complete"
    ujson.Obj(
      "queryShape"     -> ujson.Arr(requested*),
      "physicalIndex"  -> ujson.Arr(s.physical.map(ujson.Str(_))*),
      "filterCoverage" -> (if s.filterComplete then "complete" else "partial"),
      "orderCoverage"  -> orderCoverage,
      "fullCoverage"   -> s.fullCoverage,
      "residual" -> ujson.Arr(
        s.residual.map(k =>
          ujson.Obj(
            "column"    -> k.column,
            "direction" -> (if k.ascending then "asc" else "desc"),
            "relation"  -> "__rel_contracts"
          )
        )*
      )
    )

  private enum Validation:
    case Valid(name: String)
    case Invalid(name: String)
    case Missing(name: String)

  private final case class Active(
      version: Long,
      definitions: Map[String, ProjectionDefinition],
      shapes: Map[String, Map[String, Seq[String]]]
  )

  def build: ZIO[ZConnectionPool, Throwable, String] =
    transaction(planActive).flatMap {
      case None => ZIO.succeed("No active projection; nothing to build")
      case Some((version, p)) =>
        for
          _        <- transaction(ZIO.foreachDiscard(p.indexes)(insertBuilding(version, _)))
          failures <- ZIO.foreach(p.indexes)(idx => runConcurrently(idx.definition).either.map(idx.name -> _))
          outcomes <- transaction(ZIO.foreach(p.indexes)(validateOne))
          invalid = outcomes.toSeq.collect { case Validation.Invalid(n) => n }
          _    <- ZIO.foreachDiscard(invalid)(n => runConcurrently(dropDdl(n)).ignore)
          _    <- transaction(ZIO.foreachDiscard(invalid)(markRetired))
          rows <- transaction(listRows)
        yield renderBuild(p, failures.toSeq, rows)
    }

  def adopt: ZIO[ZConnectionPool, Throwable, String] =
    transaction(planActive).flatMap {
      case None => ZIO.succeed("No active projection; nothing to adopt")
      case Some((version, p)) =>
        transaction(
          for
            adopted <- sql"""update __rel_managed_index set status = 'active'::rel_index_status, adopted = true
                             where projection_version = $version and status = 'valid'::rel_index_status""".update
            superseded <- supersede(p.indexes.map(_.name))
          yield s"Adopted $adopted index(es); marked $superseded obsolete index(es) for retirement"
        )
    }

  def retire: ZIO[ZConnectionPool, Throwable, String] =
    for
      names <- transaction(
        sql"select index_name from __rel_managed_index where status = 'retiring'::rel_index_status"
          .query[String]
          .selectAll
          .map(_.toSeq)
      )
      _ <- ZIO.foreachDiscard(names)(n => runConcurrently(dropDdl(n)).ignore)
      retired <- transaction(
        (sql"""update __rel_managed_index set status = 'retired'::rel_index_status
               where status = 'retiring'::rel_index_status and index_name = any(""" ++ textArray(
          names
        ) ++ sql")").update
      )
    yield s"Retired $retired index(es)"

  def listReport: ZIO[ZConnectionPool, Throwable, String] =
    transaction(listRows).map { rows =>
      rows.toSeq match
        case Seq() => "No managed indexes"
        case ordered =>
          ordered
            .map((v, table, name, status, adopted) => s"v$v\t$table\t$name\t$status\tadopted=$adopted")
            .mkString(System.lineSeparator)
    }

  def planReport: ZIO[ZConnectionPool, Throwable, String] =
    transaction(planActive).map {
      case None               => "No active projection"
      case Some((_, planned)) => renderPlan(planned)
    }

  private def planActive: ZIO[ZConnection, Throwable, Option[(Long, Plan)]] =
    loadActive.flatMap {
      case None    => ZIO.none
      case Some(a) => baseTables.map(bt => Some((a.version, plan(a.definitions, a.shapes, bt))))
    }

  private def loadActive: ZIO[ZConnection, Throwable, Option[Active]] =
    sql"""select projection_version, definition::text, resolved_shape::text
          from __query_projection where status = 'active'"""
      .query[(Long, String, String)]
      .selectOne
      .map(
        _.map((version, definition, shape) =>
          Active(version, ProjectionDefinition.fromJson(ujson.read(definition)), parseShapes(ujson.read(shape)))
        )
      )

  private def parseShapes(json: Value): Map[String, Map[String, Seq[String]]] =
    json.obj.map { (name, byQualified) =>
      name -> byQualified.obj.map((qualified, cols) => qualified -> cols.arr.map(_("name").str).toSeq).toMap
    }.toMap

  private def baseTables: ZIO[ZConnection, Throwable, Map[String, String]] =
    sql"""select package_name || ':' || module_name || ':' || entity_name, base_table
          from __rel_entity where kind = 'template' and base_table is not null"""
      .query[(String, String)]
      .selectAll
      .map(_.toMap)

  private def insertBuilding(version: Long, idx: PlannedIndex): ZIO[ZConnection, Throwable, Unit] =
    val shapes = textArray(idx.covered.map(s => ujson.write(shapeJson(s))))
    (sql"""insert into __rel_managed_index
             (projection_version, table_name, index_name, definition, columns, opclasses,
              status, adopted, covered_query_shapes, created_at)
           select $version, ${idx.table}, ${idx.name}, ${idx.definition}, """ ++ textArray(idx.columns) ++
      sql", null, 'building'::rel_index_status, false, " ++ shapes ++ sql""", now()
           where not exists (
             select 1 from __rel_managed_index m
             where m.index_name = ${idx.name} and m.status <> 'retired'::rel_index_status)""").update.unit

  private def validateOne(idx: PlannedIndex): ZIO[ZConnection, Throwable, Validation] =
    sql"""select i.indisvalid and i.indisready from pg_class c
          join pg_index i on i.indexrelid = c.oid where c.relname = ${idx.name}"""
      .query[Boolean]
      .selectOne
      .flatMap {
        case Some(true) =>
          sql"""update __rel_managed_index set status = 'valid'::rel_index_status, validated_at = now()
                where index_name = ${idx.name} and status = 'building'::rel_index_status""".update
            .as(Validation.Valid(idx.name))
        case Some(false) => ZIO.succeed(Validation.Invalid(idx.name))
        case None        => ZIO.succeed(Validation.Missing(idx.name))
      }

  private def markRetired(name: String): ZIO[ZConnection, Throwable, Unit] =
    sql"""update __rel_managed_index set status = 'retired'::rel_index_status
          where index_name = $name and status <> 'retired'::rel_index_status""".update.unit

  private def supersede(names: Seq[String]): ZIO[ZConnection, Throwable, Long] =
    (sql"""update __rel_managed_index set status = 'retiring'::rel_index_status
           where status in ('active'::rel_index_status, 'valid'::rel_index_status)
             and index_name <> all(""" ++ textArray(names) ++ sql")").update

  private def listRows: ZIO[ZConnection, Throwable, Seq[(Long, String, String, String, Boolean)]] =
    sql"""select coalesce(projection_version, 0), table_name, index_name, status::text, adopted
          from __rel_managed_index order by index_id"""
      .query[(Long, String, String, String, Boolean)]
      .selectAll
      .map(_.toSeq)

  private def runConcurrently(ddl: String): ZIO[ZConnectionPool, Throwable, Unit] =
    ZIO.serviceWithZIO[ZConnectionPool] { pool =>
      ZIO.scoped {
        pool.transaction.build.flatMap { env =>
          env.get[ZConnection].access { c =>
            val previous = c.getAutoCommit
            c.setAutoCommit(true)
            try
              val statement = c.createStatement()
              try
                statement.execute(ddl)
                ()
              finally statement.close()
            finally c.setAutoCommit(previous)
          }
        }
      }
    }

  private def dropDdl(name: String): String = s"drop index concurrently if exists ${quoteIdent(name)}"

  private def textArray(items: Seq[String]): SqlFragment =
    items match
      case Seq() => sql"array[]::text[]"
      case _     => sql"array[" ++ items.map(i => sql"$i").mkFragment(sql", ") ++ sql"]::text[]"

  private def renderBuild(
      planned: Plan,
      failures: Seq[(String, Either[Throwable, Unit])],
      rows: Seq[(Long, String, String, String, Boolean)]
  ): String =
    val failed = failures.collect { case (name, Left(error)) => s"  failed to build $name: ${error.getMessage}" }
    val header =
      s"Built ${planned.indexes.size} planned index(es); ${rows.count((_, _, _, s, _) => strEq(s, "valid"))} valid"
    (Seq(header) ++ failed ++ planned.diagnostics.map("  " + _) ++ planned.notes.map("  " + _))
      .mkString(System.lineSeparator)

  private def renderPlan(planned: Plan): String =
    val indexLines = planned.indexes.map { idx =>
      val shapeLines = idx.covered.map { s =>
        val order = if s.order.isEmpty then "absent" else if s.orderExternal then "external" else "complete"
        s"    shape filter=[${s.filter.mkString(", ")}] order=[${s.order.map(tokenOf).mkString(", ")}]" +
          s" -> filter=${if s.filterComplete then "complete" else "partial"} order=$order full=${s.fullCoverage}"
      }
      (s"  ${idx.name} on ${idx.table} (${idx.columns.mkString(", ")}) include (${idx.include.mkString(", ")})" +:
        shapeLines).mkString(System.lineSeparator)
    }
    val diag = planned.diagnostics.map("  diagnostic: " + _)
    val note = planned.notes.map("  note: " + _)
    (Seq(s"Planned ${planned.indexes.size} index(es):") ++ indexLines ++ diag ++ note).mkString(System.lineSeparator)
end IndexManager
