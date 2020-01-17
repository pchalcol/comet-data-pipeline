package com.ebiznext.comet.job.bqload

import java.util.UUID

import com.ebiznext.comet.config.Settings
import com.ebiznext.comet.schema.handlers.StorageHandler
import com.ebiznext.comet.schema.model.Schema
import com.ebiznext.comet.utils.SparkJob
import com.google.cloud.bigquery.JobInfo.{CreateDisposition, WriteDisposition}
import com.google.cloud.bigquery.TimePartitioning.Type
import com.google.cloud.bigquery._
import com.google.cloud.bigquery.testing.RemoteBigQueryHelper
import com.google.cloud.hadoop.io.bigquery.output.{
  BigQueryOutputConfiguration,
  BigQueryTimePartitioning,
  IndirectBigQueryOutputFormat
}
import com.google.cloud.hadoop.io.bigquery.{BigQueryConfiguration, BigQueryFileFormat}
import com.google.gson.JsonParser
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.util.{Success, Try}

class BigQueryLoadJob(
  cliConfig: BigQueryLoadConfig,
  storageHandler: StorageHandler,
  maybeSchema: scala.Option[Schema] = None
) extends SparkJob {

  override def name: String = s"bqload-${cliConfig.outputTable}"

  /**
    * Just to force any spark job to implement its entry point using within the "run" method
    *
    * @return : Spark Session used for the job
    */
  def runSpark(): Try[SparkSession] = {
    val conf = session.sparkContext.hadoopConfiguration
    logger.info(s"BigQuery Config $cliConfig")

    val projectId = conf.get("fs.gs.project.id")
    val bucket = conf.get("fs.gs.system.bucket")
    // val outputTableId = projectId + ":wordcount_dataset.wordcount_output"
    val outputTableId = s"$projectId:${cliConfig.outputDataset}.${cliConfig.outputTable}"
    // Temp output bucket that is deleted upon completion of job.
    val outputGcsPath = ("gs://" + bucket + "/tmp/" + UUID.randomUUID())
    // Temp output bucket that is deleted upon completion of job.
    val jsonPath = ("gs://" + bucket + "/tmp/" + UUID.randomUUID())
    val inputPath = cliConfig.sourceFile
    logger.info(s"Input path $inputPath")
    logger.info(s"Json path $jsonPath")

    maybeSchema.fold {
      BigQueryOutputConfiguration.configureWithAutoSchema(
        conf,
        outputTableId,
        outputGcsPath,
        BigQueryFileFormat.NEWLINE_DELIMITED_JSON,
        classOf[TextOutputFormat[_, _]]
      )
    } { schema =>
      BigQueryOutputConfiguration.configure(
        conf,
        outputTableId,
        schema.bqType(),
        outputGcsPath,
        BigQueryFileFormat.NEWLINE_DELIMITED_JSON,
        classOf[TextOutputFormat[_, _]]
      )
    }

    conf.set(
      "mapreduce.job.outputformat.class",
      classOf[IndirectBigQueryOutputFormat[_, _]].getName
    )
    conf.set(BigQueryConfiguration.OUTPUT_TABLE_WRITE_DISPOSITION_KEY, cliConfig.writeDisposition)
    conf.set(BigQueryConfiguration.OUTPUT_TABLE_CREATE_DISPOSITION_KEY, cliConfig.createDisposition)
    cliConfig.outputPartition.foreach { outputPartition =>
      import com.google.api.services.bigquery.model.TimePartitioning
      val timeField =
        if (List("_PARTITIONDATE", "_PARTITIONTIME").contains(outputPartition))
          new TimePartitioning().setType("DAY").setRequirePartitionFilter(true)
        else
          new TimePartitioning()
            .setType("DAY")
            .setRequirePartitionFilter(true)
            .setField(outputPartition)
      val timePartitioning =
        new BigQueryTimePartitioning(
          timeField
        )
      conf.set(BigQueryConfiguration.OUTPUT_TABLE_PARTITIONING_KEY, timePartitioning.getAsJson)
    }

    Try {
      val bigqueryHelper = RemoteBigQueryHelper.create
      val bigquery = bigqueryHelper.getOptions().getService();
      val datasetId = DatasetId.of(projectId, cliConfig.outputDataset)
      val dataset = scala.Option(bigquery.getDataset(datasetId))
      dataset.getOrElse {
        val datasetInfo = DatasetInfo
          .newBuilder(cliConfig.outputDataset)
          .setLocation(cliConfig.getLocation())
          .build
        bigquery.create(datasetInfo)
      }
      logger.info(s"dataset read")
      val sourceJson = if (cliConfig.sourceFormat.equalsIgnoreCase("parquet")) {
        val parquetDF = session.read.parquet(inputPath)
        logger.info("Read parquet File")
        parquetDF.write.json(jsonPath)
        logger.info(s"Written to $jsonPath")
        jsonPath
      } else if (cliConfig.sourceFormat.equalsIgnoreCase("json")) {
        inputPath
      } else {
        throw new Exception(s"Unknown format ${cliConfig.sourceFormat}")
      }
      logger.info(s"Source Json $sourceJson")

      session.sparkContext
        .textFile(sourceJson)
        .map(text => (null, new JsonParser().parse(text).getAsJsonObject))
        .saveAsNewAPIHadoopDataset(conf)
//      Settings.storageHandler.delete(new Path(sourceJson))
      // Check the table
      val tableId = TableId.of(cliConfig.outputDataset, cliConfig.outputTable)
      val destinationTable = bigquery.getTable(tableId).getDefinition[StandardTableDefinition]
      logger.info("Loaded %d rows.\n", destinationTable.getNumRows)
      session
    }
  }

  /**
    * Just to force any spark job to implement its entry point using within the "run" method
    *
    * @return : Spark Session used for the job
    */
  def runBigQuery(): Try[SparkSession] = {

    val conf = session.sparkContext.hadoopConfiguration
    logger.info(s"BigQuery Config $cliConfig")

    val projectId = conf.get("fs.gs.project.id")
    val bucket = conf.get("fs.gs.system.bucket")

    logger.info(s"Temporary GCS path $bucket")
    session.conf.set("temporaryGcsBucket", bucket)

    val bigqueryHelper = RemoteBigQueryHelper.create
    val bigquery = bigqueryHelper.getOptions.getService

    def getOrCreateDataset() = {
      val datasetId = DatasetId.of(projectId, cliConfig.outputDataset)
      val dataset = scala.Option(bigquery.getDataset(datasetId))
      dataset.getOrElse {
        val datasetInfo = DatasetInfo
          .newBuilder(cliConfig.outputDataset)
          .setLocation(cliConfig.getLocation())
          .build
        bigquery.create(datasetInfo)
      }
    }

    // TODO
    // com.google.cloud.bigquery.JobInfo.WriteDisposition : WRITE_TRUNCATE, WRITE_APPEND, WRITE_EMPTY
    // com.google.cloud.bigquery.JobInfo.CreateDisposition : CREATE_IF_NEEDED, CREATE_NEVER
    /*
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
  }*/

    def getOrCreateTable(): Table = {
      import com.google.cloud.bigquery.TableId
      import com.google.cloud.bigquery.StandardTableDefinition
      import com.google.cloud.bigquery.TableInfo

      val tableId = TableId.of(cliConfig.outputDataset, cliConfig.outputTable)

      scala.Option(bigquery.getTable(tableId)) getOrElse {

        val tableDefinitionBuilder = maybeSchema.fold(StandardTableDefinition.newBuilder()) {
          schema =>
            StandardTableDefinition.of(schema.bqSchema()).toBuilder
        }

        cliConfig.outputPartition.foreach { outputPartition =>
          import com.google.cloud.bigquery.TimePartitioning
          val timeField =
            if (List("_PARTITIONDATE", "_PARTITIONTIME").contains(outputPartition))
              TimePartitioning
                .newBuilder(TimePartitioning.Type.DAY)
                .setRequirePartitionFilter(true)
                .build()
            else
              TimePartitioning
                .newBuilder(TimePartitioning.Type.DAY)
                .setRequirePartitionFilter(true)
                .setField(outputPartition)
                .build()
          tableDefinitionBuilder.setTimePartitioning(timeField)
        }

        val tableDefinition = tableDefinitionBuilder.build()
        val tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build
        val table = bigquery.create(tableInfo)

        table
      }
    }

    getOrCreateDataset()
    val table = getOrCreateTable()

    println(
      s"===================> ${table.getTableId.getDataset}.${table.getTableId.getTable} <==================="
    )

    Try {
      val inputDataset = s"gs://$bucket${cliConfig.sourceFile}/*.parquet"
      println(s"source ds: $inputDataset")
      println(s"TableId: ${table.getTableId}")

      val config = LoadJobConfiguration
        .newBuilder(table.getTableId, inputDataset, FormatOptions.parquet())
        .build()
      val jobInfo = JobInfo.of(config)

      val job = bigquery.create(jobInfo)
      job.waitFor()

      val stdTableDefinition =
        bigquery.getTable(table.getTableId).getDefinition().asInstanceOf[StandardTableDefinition]
      println(s"${stdTableDefinition.getNumRows}")

      session
    }
  }

  /**
    * Load data to BigQuery using Spark BQ Connector.
    * @return
    */
  def runBQSparkConnector(): Try[SparkSession] = {

    val conf = session.sparkContext.hadoopConfiguration
    logger.info(s"BigQuery Config $cliConfig")

    val projectId = conf.get("fs.gs.project.id")
    val bucket = conf.get("fs.gs.system.bucket")

    logger.info(s"Temporary GCS path $bucket")
    session.conf.set("temporaryGcsBucket", bucket)

    val bigqueryHelper = RemoteBigQueryHelper.create
    val bigquery = bigqueryHelper.getOptions.getService

    def getOrCreateDataset() = {
      val datasetId = DatasetId.of(projectId, cliConfig.outputDataset)
      val dataset = scala.Option(bigquery.getDataset(datasetId))
      dataset.getOrElse {
        val datasetInfo = DatasetInfo
          .newBuilder(cliConfig.outputDataset)
          .setLocation(cliConfig.getLocation())
          .build
        bigquery.create(datasetInfo)
      }
    }

    def getOrCreateTable(): Table = {
      import com.google.cloud.bigquery.TableId
      import com.google.cloud.bigquery.StandardTableDefinition
      import com.google.cloud.bigquery.TableInfo

      val tableId = TableId.of(cliConfig.outputDataset, cliConfig.outputTable)

      scala.Option(bigquery.getTable(tableId)) getOrElse {

        val tableDefinitionBuilder = maybeSchema.fold(StandardTableDefinition.newBuilder()) {
          schema =>
            StandardTableDefinition.of(schema.bqSchema()).toBuilder
        }

        cliConfig.outputPartition.foreach { outputPartition =>
          import com.google.cloud.bigquery.TimePartitioning
          val timeField =
            if (List("_PARTITIONDATE", "_PARTITIONTIME").contains(outputPartition))
              TimePartitioning
                .newBuilder(TimePartitioning.Type.DAY)
                .setRequirePartitionFilter(true)
                .build()
            else
              TimePartitioning
                .newBuilder(TimePartitioning.Type.DAY)
                .setRequirePartitionFilter(true)
                .setField(outputPartition)
                .build()
          tableDefinitionBuilder.setTimePartitioning(timeField)
        }

        val tableDefinition = tableDefinitionBuilder.build()
        val tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build
        val table = bigquery.create(tableInfo)

        table
      }
    }

    getOrCreateDataset()
    val table = getOrCreateTable()

    println(
      s"===================> ${table.getTableId.getDataset}.${table.getTableId.getTable} <==================="
    )

    val inputPath = cliConfig.sourceFile
    logger.info(s"Input path $inputPath")

    Try {

      lazy val sourceDF = session.read.parquet(inputPath)

      val bqTable = s"${cliConfig.outputDataset}.${cliConfig.outputTable}"
      val parquetDF = maybeSchema.fold(sourceDF) { schema =>
        session.createDataFrame(sourceDF.rdd, schema.sparkFunctionalSchema())
      }

      println("=====================> DF Schema Before bqload <==========================")
      parquetDF.printSchema()

      parquetDF.write
        .format("bigquery")
        .option("table", bqTable)
        .mode(SaveMode.Append)
        .save()
      // df.write.format.options.mode(SaveMode.Overwrite).save()

      // FIXME executed by all spark executors => must be executed once

      session
    }
  }

  /**
    * Just to force any spark job to implement its entry point using within the "run" method
    *
    * @return : Spark Session used for the job
    */
//  override def run(): Try[SparkSession] = runSpark()
//  override def run(): Try[SparkSession] = runBQSparkConnector()
  override def run(): Try[SparkSession] = runBigQuery()

}
