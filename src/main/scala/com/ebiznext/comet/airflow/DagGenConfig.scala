package com.ebiznext.comet.airflow

import better.files.File
import scopt.{OParser, OParserBuilder, RenderingMode}

case class DagGenConfig(
  // TODO
  domains: Seq[String] = Nil,
  schemas: Seq[String] = Nil,
  template: File = File("."),
  outputDir: File = File(".")
)

object DagGenConfig {

  val builder: OParserBuilder[DagGenConfig] = OParser.builder[DagGenConfig]

  def exists(name: String)(path: String): Either[String, Unit] =
    if (File(path).exists) Right(())
    else Left(s"$name at path $path does not exist")

  val parser: OParser[Unit, DagGenConfig] = {
    import builder._
    OParser.sequence(
      programName("comet"),
      head("comet", "1.x"),
      note(
        """TODO""".stripMargin
      ),
      cmd("dags-gen"),
      // domains and schema absent <=> --all ie all domains and all schemas
      // will be handled at once in one unique dag so ingestions will be executed sequentially
      opt[Seq[String]]("domains")
        .action((x, c) => c.copy(domains = x))
        .optional()
        .text("TODO"),
      opt[Seq[String]]("schemas")
        .action((x, c) => c.copy(schemas = x))
        .optional()
        .text("TODO"),
      opt[String]("template")
        .validate(exists("Dag template file"))
        .action((x, c) => c.copy(template = File(x)))
        .required()
        .text("Dag template file"),
      opt[String]("outputDir")
        .validate(exists("Dag output folder"))
        .action((x, c) => c.copy(outputDir = File(x)))
        .required()
        .text("Dag output folder")
    )
  }
  val usage: String = OParser.usage(parser, RenderingMode.TwoColumns)

  /** Function to parse command line arguments (domain and schema).
    *
    * @param args : Command line parameters
    * @return : an Option of MetricConfing with the parsed domain and schema names.
    */
  def parse(args: Seq[String]): Option[DagGenConfig] =
    OParser.parse(parser, args, DagGenConfig.apply())
}
