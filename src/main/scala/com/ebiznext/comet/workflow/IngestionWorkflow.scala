/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *    http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 *
 */

package com.ebiznext.comet.workflow

import better.files.File
import com.ebiznext.comet.config.{DatasetArea, Settings}
import com.ebiznext.comet.job.atlas.{AtlasConfig, AtlasJob}
import com.ebiznext.comet.job.bqload.{BigQueryLoadConfig, BigQueryLoadJob}
import com.ebiznext.comet.job.index.{IndexConfig, IndexJob}
import com.ebiznext.comet.job.infer.{InferConfig, InferSchema}
import com.ebiznext.comet.job.ingest._
import com.ebiznext.comet.job.metrics.{MetricsConfig, MetricsJob}
import com.ebiznext.comet.job.transform.AutoTask
import com.ebiznext.comet.schema.handlers.{LaunchHandler, SchemaHandler, StorageHandler}
import com.ebiznext.comet.schema.model.Format._
import com.ebiznext.comet.schema.model._
import com.ebiznext.comet.utils.Utils
import com.typesafe.scalalogging.StrictLogging
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession

import scala.util.{Failure, Success, Try}

/**
  * The whole worklfow works as follow :
  *   - loadLanding : Zipped files are uncompressed or raw files extracted from the local filesystem.
  * -loadPending :
  * files recognized with filename patterns are stored in the ingesting area and submitted for ingestion
  * files with unrecognized filename patterns are stored in the unresolved area
  *   - ingest : files are finally ingested and saved as parquet/orc/... files and hive tables
  *
  * @param storageHandler : Minimum set of features required for the underlying filesystem
  * @param schemaHandler  : Schema interface
  * @param launchHandler  : Cron Manager interface
  */
class IngestionWorkflow(
  storageHandler: StorageHandler,
  schemaHandler: SchemaHandler,
  launchHandler: LaunchHandler
) extends StrictLogging {
  val domains: List[Domain] = schemaHandler.domains

  /**
    * Load file from the landing area
    * files are loaded one domain at a time
    * each domain has its own directory
    * compressed files are uncompressed if a corresponding ack file exist.
    * raw file should also have a corresponding ack file
    * before moving the files to the pending area, the ack files are deleted
    */
  def loadLanding(): Unit = {
    domains.foreach { domain =>
      val storageHandler = Settings.storageHandler
      val inputDir = new Path(domain.directory)
      logger.info(s"Scanning $inputDir")
      storageHandler.list(inputDir, domain.getAck()).foreach { path =>
        val ackFile = path
        val fileStr = ackFile.toString
        val prefixStr =
          if (domain.getAck().isEmpty) fileStr.substring(0, fileStr.lastIndexOf('.'))
          else fileStr.stripSuffix(domain.getAck())
        val tmpDir = new Path(prefixStr)
        val rawFormats =
          if (domain.getAck().isEmpty) List(ackFile)
          else
            domain.getExtensions().map(ext => new Path(prefixStr + ext))
        val existRawFile = rawFormats.find(file => storageHandler.exists(file))
        logger.info(s"Found ack file $ackFile")
        if (domain.getAck().nonEmpty)
          storageHandler.delete(ackFile)
        if (existRawFile.isDefined) {
          existRawFile.foreach { file =>
            logger.info(s"Found raw file $existRawFile")
            storageHandler.mkdirs(tmpDir)
            val tmpFile = new Path(tmpDir, file.getName)
            storageHandler.move(file, tmpFile)
          }
        } else if (storageHandler.fs.getScheme() == "file") {
          val tgz = new Path(prefixStr + ".tgz")
          val gz = new Path(prefixStr + ".gz")
          val zip = new Path(prefixStr + ".zip")
          if (storageHandler.exists(gz)) {
            logger.info(s"Found compressed file $gz")
            File(Path.getPathWithoutSchemeAndAuthority(gz).toString).unGzipTo(File(tmpDir.toString))
            storageHandler.delete(gz)
          } else if (storageHandler.exists(tgz)) {
            logger.info(s"Found compressed file $tgz")
            File(Path.getPathWithoutSchemeAndAuthority(tgz).toString)
              .unGzipTo(File(tmpDir.toString))
            storageHandler.delete(tgz)
          } else if (storageHandler.exists(zip)) {
            logger.info(s"Found compressed file $zip")
            File(Path.getPathWithoutSchemeAndAuthority(zip).toString).unzipTo(File(tmpDir.toString))
            storageHandler.delete(zip)
          } else {
            logger.error(s"No archive found for ack ${ackFile.toString}")
          }
        } else {
          logger.error(s"No file found for ack ${ackFile.toString}")
        }
        if (storageHandler.exists(tmpDir)) {
          val destFolder = DatasetArea.pending(domain.name)
          storageHandler.list(tmpDir).foreach { file =>
            val source = new Path(file.toString)
            logger.info(s"Importing ${file.toString}")
            val destFile = new Path(destFolder, file.getName)
            storageHandler.moveFromLocal(source, destFile)
          }
          storageHandler.delete(tmpDir)
        }
      }
    }
  }

  /**
    * Split files into resolved and unresolved datasets. A file is unresolved
    * if a corresponding schema is not found.
    * Schema matching is based on the dataset filename pattern
    *
    * @param includes Load pending dataset of these domain only
    * @param excludes : Do not load datasets of these domains
    *                 if both lists are empty, all domains are included
    */
  def loadPending(includes: List[String] = Nil, excludes: List[String] = Nil): Unit = {
    val includedDomains = (includes, excludes) match {
      case (Nil, Nil) =>
        domains
      case (_, Nil) =>
        domains.filter(domain => includes.contains(domain.name))
      case (Nil, _) =>
        domains.filter(domain => !excludes.contains(domain.name))
      case (_, _) => throw new Exception("Should never happen ")
    }
    logger.info(s"Domains that will be watched: ${domains.map(_.name).mkString(",")}")

    includedDomains.foreach { domain =>
      logger.info(s"Watch Domain: ${domain.name}")
      val (resolved, unresolved) = pending(domain.name)
      unresolved.foreach {
        case (_, path) =>
          val targetPath =
            new Path(DatasetArea.unresolved(domain.name), path.getName)
          logger.info(s"Unresolved file : ${path.getName}")
          storageHandler.move(path, targetPath)
      }

      // We group files with the same schema to ingest them together in a single step.
      val groupedResolved: Map[Schema, Iterable[Path]] = resolved.map {
        case (Some(schema), path) => (schema, path)
        case (None, _)            => throw new Exception("Should never happen")
      } groupBy (_._1) mapValues (it => it.map(_._2))

      groupedResolved.foreach {
        case (schema, pendingPaths) =>
          logger.info(s"""Ingest resolved file : ${pendingPaths
            .map(_.getName)
            .mkString(",")} with schema ${schema.name}""")
          val ingestingPaths = pendingPaths.map { pendingPath =>
            val ingestingPath = new Path(DatasetArea.ingesting(domain.name), pendingPath.getName)
            if (!storageHandler.move(pendingPath, ingestingPath)) {
              logger.error(s"Could not move $pendingPath to $ingestingPath")
            }
            ingestingPath
          }
          try {
            if (Settings.comet.grouped)
              launchHandler.ingest(this, domain, schema, ingestingPaths.toList)
            else
              ingestingPaths.foreach(launchHandler.ingest(this, domain, schema, _))
          } catch {
            case t: Throwable =>
              t.printStackTrace()
            // Continue to next pending file
          }
      }
    }
  }

  /**
    *
    * @param domainName : Domaine name
    * @return resolved && unresolved schemas / path
    */
  private def pending(
    domainName: String
  ): (Iterable[(Option[Schema], Path)], Iterable[(Option[Schema], Path)]) = {
    val pendingArea = DatasetArea.pending(domainName)
    logger.info(s"List files in $pendingArea")
    val paths = storageHandler.list(pendingArea)
    logger.info(s"Found ${paths.mkString(",")}")
    val domain = schemaHandler.getDomain(domainName).toList
    val schemas: Iterable[(Option[Schema], Path)] =
      for {
        domain <- domain
        schema <- paths.map { path =>
          (domain.findSchema(path.getName), path) // getName without timestamp
        }
      } yield {
        logger.info(
          s"Found Schema ${schema._1.map(_.name).getOrElse("None")} for file ${schema._2}"
        )
        schema
      }
    schemas.partition(_._1.isDefined)
  }

  /**
    * Ingest the file (called by the cron manager at ingestion time for a specific dataset
    *
    * @param domainName     : domain name of the dataset
    * @param schemaName     schema name of the dataset
    * @param ingestingPaths : Absolute path of the file to ingest (present in the ingesting area of the domain)
    */
  def ingest(domainName: String, schemaName: String, ingestingPaths: List[Path]): Unit = {
    for {
      domain <- domains.find(_.name == domainName)
      schema <- domain.schemas.find(_.name == schemaName)
    } yield ingesting(domain, schema, ingestingPaths)
    ()
  }

  private def ingesting(domain: Domain, schema: Schema, ingestingPath: List[Path]): Unit = {
    logger.info(
      s"Start Ingestion on domain: ${domain.name} with schema: ${schema.name} on file: $ingestingPath"
    )
    val metadata = domain.metadata
      .getOrElse(Metadata())
      .`import`(schema.metadata.getOrElse(Metadata()))
    logger.info(
      s"Ingesting domain: ${domain.name} with schema: ${schema.name} on file: $ingestingPath with metadata $metadata"
    )
    val ingestionResult = Try(metadata.getFormat() match {
      case DSV =>
        new DsvIngestionJob(domain, schema, schemaHandler.types, ingestingPath, storageHandler)
          .run()
      case SIMPLE_JSON =>
        new SimpleJsonIngestionJob(
          domain,
          schema,
          schemaHandler.types,
          ingestingPath,
          storageHandler
        ).run()
      case JSON =>
        new JsonIngestionJob(domain, schema, schemaHandler.types, ingestingPath, storageHandler)
          .run()
      case POSITION =>
        new PositionIngestionJob(domain, schema, schemaHandler.types, ingestingPath, storageHandler)
          .run()
      case CHEW =>
        ChewerJob.run(
          s"${Settings.comet.chewerPrefix}.${domain.name}.${schema.name}",
          domain,
          schema,
          schemaHandler.types,
          ingestingPath,
          storageHandler
        )

      case _ =>
        throw new Exception("Should never happen")
    })
    ingestionResult match {
      case Success(_) =>
        if (Settings.comet.archive) {
          ingestingPath.foreach { ingestingPath =>
            val archivePath =
              new Path(DatasetArea.archive(domain.name), ingestingPath.getName)
            logger.info(s"Backing up file $ingestingPath to $archivePath")
            val _ = storageHandler.move(ingestingPath, archivePath)
          }
        } else {
          logger.info(s"Deleting file $ingestingPath")
          ingestingPath.foreach(storageHandler.delete)
        }
      case Failure(exception) =>
        Utils.logException(logger, exception)
    }

    val meta = schema.mergedMetadata(domain.metadata)
    meta.getIndexSink() match {
      case Some(IndexSink.ES) if Settings.comet.elasticsearch.active =>
        val properties = meta.properties
        index(
          IndexConfig(
            timestamp = properties.flatMap(_.get("timestamp")),
            id = properties.flatMap(_.get("id")),
            format = "parquet",
            domain = domain.name,
            schema = schema.name
          )
        )

      case Some(IndexSink.BQ) =>
        val (createDisposition: String, writeDisposition: String) = getBQDisposition(
          meta.getWriteMode()
        )
        bqload(
          BigQueryLoadConfig(
            sourceFile = new Path(DatasetArea.accepted(domain.name), schema.name).toString,
            outputTable = schema.name,
            outputDataset = domain.name,
            sourceFormat = "parquet",
            createDisposition = createDisposition,
            writeDisposition = writeDisposition,
            location = meta.getProperties().get("location"),
            outputPartition = meta.getProperties().get("timestamp"),
            days = meta.getProperties().get("days").map(_.toInt)
          ),
          Some(schema)
        )
      case _ =>
      // ignore
    }
  }

  private def getBQDisposition(writeMode: WriteMode) = {
    val (createDisposition, writeDisposition) = writeMode match {
      case WriteMode.OVERWRITE =>
        ("CREATE_IF_NEEDED", "WRITE_TRUNCATE")
      case WriteMode.APPEND =>
        ("CREATE_IF_NEEDED", "WRITE_APPEND")
      case WriteMode.ERROR_IF_EXISTS =>
        ("CREATE_IF_NEEDED", "WRITE_EMPTY")
      case WriteMode.IGNORE =>
        ("CREATE_NEVER", "WRITE_EMPTY")
      case _ =>
        ("CREATE_IF_NEEDED", "WRITE_TRUNCATE")
    }
    (createDisposition, writeDisposition)
  }

  def index(job: AutoJobDesc, task: AutoTaskDesc): Unit = {
    val targetArea = task.area.getOrElse(job.getArea())
    val targetPath = new Path(DatasetArea.path(task.domain, targetArea.value), task.dataset)
    val properties = task.properties
    launchHandler.index(
      this,
      IndexConfig(
        timestamp = properties.flatMap(_.get("timestamp")),
        id = properties.flatMap(_.get("id")),
        format = "parquet",
        domain = task.domain,
        schema = task.dataset,
        dataset = Some(targetPath)
      )
    )
  }

  def infer(config: InferConfig) = {
    new InferSchema(
      config.domainName,
      config.schemaName,
      config.inputPath,
      config.outputPath,
      config.header
    )
  }

  /**
    * Successively run each task of a job
    *
    * @param jobname : job name as defined in the YML file.
    */
  def autoJob(jobname: String): Unit = {
    val job = schemaHandler.jobs(jobname)
    autoJob(job)
  }

  /**
    * Successively run each task of a job
    *
    * @param job : job as defined in the YML file.
    */
  def autoJob(job: AutoJobDesc): Unit = {
    job.tasks.foreach { task =>
      val action = new AutoTask(
        job.name,
        job.getArea(),
        job.format,
        job.coalesce.getOrElse(false),
        job.udf,
        job.views,
        task,
        storageHandler
      )
      action.run() match {
        case Success(_) =>
          task.getIndexSink() match {
            case Some(IndexSink.ES) if Settings.comet.elasticsearch.active =>
              index(job, task)
            case Some(IndexSink.BQ) =>
              val (createDisposition, writeDisposition) = this.getBQDisposition(task.write)
              bqload(
                BigQueryLoadConfig(
                  sourceFile = task.getTargetPath(job.getArea()).toString,
                  outputTable = task.dataset,
                  outputDataset = task.domain,
                  sourceFormat = "parquet",
                  createDisposition = createDisposition,
                  writeDisposition = writeDisposition,
                  location = task.properties.flatMap(_.get("location")),
                  outputPartition = task.properties.flatMap(_.get("timestamp")),
                  days = task.properties.flatMap(_.get("days").map(_.toInt))
                )
              )
            case _ =>
            // ignore

          }
        case Failure(exception) =>
          exception.printStackTrace()
      }
    }
  }

  def index(config: IndexConfig): Try[SparkSession] = {
    new IndexJob(config, Settings.storageHandler).run()
  }

  def bqload(config: BigQueryLoadConfig, maybeSchema: Option[Schema] = None): Try[SparkSession] = {
    val res = new BigQueryLoadJob(config, Settings.storageHandler, maybeSchema).run()
    res match {
      case Success(_) => ;
      case Failure(e) => logger.info("BQLoad Failed", e)
    }
    res
  }

  def atlas(config: AtlasConfig): Unit = {
    new AtlasJob(config, Settings.storageHandler).run()
  }

  /**
    * Runs the metrics job
    *
    * @param cliConfig : Client's configuration for metrics computing
    */
  def metric(cliConfig: MetricsConfig): Unit = {
    //Lookup for the domain given as prompt arguments, if is found then find the given schema in this domain
    val cmdArgs = for {
      domain <- Settings.schemaHandler.getDomain(cliConfig.domain)
      schema <- domain.schemas.find(_.name == cliConfig.schema)
    } yield (domain, schema)

    cmdArgs match {
      case Some((domain: Domain, schema: Schema)) => {
        val stage: Stage = cliConfig.stage.getOrElse(Stage.UNIT)
        new MetricsJob(
          domain,
          schema,
          stage,
          storageHandler
        ).run()
      }
      case None => logger.error("The domain or schema you specified doesn't exist! ")
    }
  }
}
