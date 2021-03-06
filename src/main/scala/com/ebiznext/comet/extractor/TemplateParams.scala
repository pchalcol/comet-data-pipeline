package com.ebiznext.comet.extractor

import java.time.format.DateTimeFormatter

import better.files.File
import com.ebiznext.comet.schema.model.{Domain, Schema, WriteMode}

/** Params for the script's mustache template
  * @param tableToExport table to export
  * @param columnsToExport cols to export
  * @param fullExport if table is going to be fully or delta exported
  * @param deltaColumn if delta exported, which is the col holding the date of last update
  * @param dsvDelimiter export result dsv delimiter
  * @param exportOutputFileBase export dsv file base name (will be completed by current datetime when executed)
  * @param scriptOutputFile where the script is produced
  */
case class TemplateParams(
  tableToExport: String,
  columnsToExport: List[String],
  fullExport: Boolean,
  deltaColumn: Option[String],
  dsvDelimiter: String,
  exportOutputFileBase: String,
  scriptOutputFile: File
) {

  val paramMap: Map[String, Any] = {

    // This is how we deal with the last element not needing a trailing a comma in a Mustache template
    val columnsParam: List[Map[String, Any]] = columnsToExport.map(_.toUpperCase) match {
      case head :: Nil => List(Map("name" -> head, "trailing_col_char" -> ""))
      case Nil         => Nil
      case atLeastTwoElemList =>
        val allButLast = atLeastTwoElemList.dropRight(1)
        val last = atLeastTwoElemList.last
        allButLast
          .map(c => Map("name" -> c, "trailing_col_char" -> ",")) :+ Map(
          "name"              -> last,
          "trailing_col_char" -> ""
        )
    }
    deltaColumn
      .foldLeft(
        List(
          "table_name"  -> tableToExport.toUpperCase,
          "delimiter"   -> dsvDelimiter,
          "columns"     -> columnsParam,
          "export_file" -> exportOutputFileBase,
          "full_export" -> fullExport
        )
      ) { case (list, deltaCol) => list :+ ("delta_column" -> deltaCol.toUpperCase) }
      .toMap
  }
}

object TemplateParams {

  val dateFormater: DateTimeFormatter = DateTimeFormatter.ISO_DATE

  /** Generating all the TemplateParams, corresponding to all the schema's tables of the domain
    *
    * @param domain               The domain
    * @param scriptsOutputFolder  Where the scripts are produced
    * @param defaultDeltaColumn   The default delta column to use
    * @param deltaColumns         A table name -> delta column to use mapping (if needing a special delta column for a given table).
    *                             Has precedence over `defaultDeltaColumn`.
    * @return
    */
  def fromDomain(
    domain: Domain,
    scriptsOutputFolder: File,
    defaultDeltaColumn: Option[String],
    deltaColumns: Map[String, String]
  ): List[TemplateParams] =
    domain.schemas.map(s =>
      fromSchema(s, scriptsOutputFolder, deltaColumns.get(s.name).orElse(defaultDeltaColumn))
    )

  /** Generate scripts template parameters, extracting the tables and the columns described in the schema
    * @param schema The schema used to generate the scripts parameters
    * @param scriptsOutputFolder  Where the scripts are produced
    * @param deltaColumn   The delta column to use for that table
    * @return The corresponding TemplateParams
    */
  def fromSchema(
    schema: Schema,
    scriptsOutputFolder: File,
    deltaColumn: Option[String]
  ): TemplateParams = {
    val scriptOutputFileName = s"EXTRACT_${schema.name}.sql"
    // exportFileBase is the csv file name base such as EXPORT_L58MA_CLIENT_DELTA_...
    // Considering a pattern like EXPORT_L58MA_CLIENT.*.csv
    // The script which is generated will append the current date time to that base (EXPORT_L58MA_CLIENT_18032020173100).
    val exportFileBase = s"${schema.pattern.toString.split("\\.\\*").head}"
    val isFullExport = schema.metadata.flatMap(_.write).contains(WriteMode.OVERWRITE)
    new TemplateParams(
      tableToExport = schema.name,
      columnsToExport = schema.attributes.filter(_.script.isEmpty).map(_.name),
      fullExport = isFullExport,
      deltaColumn = if (!isFullExport) deltaColumn else None,
      dsvDelimiter = schema.metadata.flatMap(_.separator).getOrElse(","),
      exportOutputFileBase = exportFileBase,
      scriptOutputFile = scriptsOutputFolder / scriptOutputFileName
    )
  }
}
