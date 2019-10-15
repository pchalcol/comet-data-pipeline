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

package com.ebiznext.comet.schema.model

import java.sql.Timestamp
import java.time._
import java.time.format.DateTimeFormatter
import java.time.temporal.TemporalAccessor
import java.util.regex.Pattern

import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.annotation.{JsonDeserialize, JsonSerialize}
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer
import com.fasterxml.jackson.databind.{DeserializationContext, JsonDeserializer}
import org.apache.spark.sql.types._

import scala.util.{Failure, Success, Try}

/**
  * Spark supported primitive types. These are the only valid raw types.
  * Dataframes columns are converted to these types before the dataset is ingested
  *
  * @param value : string, long, double, boolean, byte, date, timestamp, decimal with (precision=30, scale=15)
  */
@JsonSerialize(using = classOf[ToStringSerializer])
@JsonDeserialize(using = classOf[PrimitiveTypeDeserializer])
sealed abstract case class PrimitiveType(value: String) {
  def fromString(str: String, pattern: String = null): Any

  override def toString: String = value

  def sparkType: DataType
}

class PrimitiveTypeDeserializer extends JsonDeserializer[PrimitiveType] {

  def simpleTypeFromString(value: String): PrimitiveType = {
    value match {
      case "string"    => PrimitiveType.string
      case "long"      => PrimitiveType.long
      case "int"       => PrimitiveType.int
      case "short"     => PrimitiveType.short
      case "double"    => PrimitiveType.double
      case "boolean"   => PrimitiveType.boolean
      case "byte"      => PrimitiveType.byte
      case "date"      => PrimitiveType.date
      case "timestamp" => PrimitiveType.timestamp
      case "decimal"   => PrimitiveType.decimal
      case "struct"    => PrimitiveType.struct
      case _ =>
        throw new Exception(
          s"Invalid primitive type: $value not in ${PrimitiveType.primitiveTypes}"
        )
    }
  }

  override def deserialize(jp: JsonParser, ctx: DeserializationContext): PrimitiveType = {
    val value = jp.readValueAs[String](classOf[String])
    simpleTypeFromString(value)
  }
}

object PrimitiveType {

  object string extends PrimitiveType("string") {
    def fromString(str: String, pattern: String = null): Any = str

    def sparkType: DataType = StringType
  }

  object long extends PrimitiveType("long") {

    def fromString(str: String, pattern: String): Any =
      if (str == null || str.isEmpty) null else str.toLong

    def sparkType: DataType = LongType
  }

  object int extends PrimitiveType("int") {

    def fromString(str: String, pattern: String): Any =
      if (str == null || str.isEmpty) null else str.toInt

    def sparkType: DataType = IntegerType
  }

  object short extends PrimitiveType("short") {

    def fromString(str: String, pattern: String): Any =
      if (str == null || str.isEmpty) null else str.toShort

    def sparkType: DataType = ShortType
  }

  object double extends PrimitiveType("double") {

    def fromString(str: String, pattern: String): Any =
      if (str == null || str.isEmpty) null else str.toDouble

    def sparkType: DataType = DoubleType
  }

  object decimal extends PrimitiveType("decimal") {
    val defaultDecimalType = DataTypes.createDecimalType(30, 15)

    def fromString(str: String, pattern: String): Any =
      if (str == null || str.isEmpty) null else BigDecimal(str)

    override def sparkType: DataType = defaultDecimalType
  }

  object boolean extends PrimitiveType("boolean") {

    def matches(str: String, pattern: String): Boolean = {
      if (pattern.indexOf("<-TF->") >= 0) {
        val tf = pattern.split("<-TF->")
        Pattern
          .compile(tf(0), Pattern.MULTILINE)
          .matcher(str)
          .matches() ||
        Pattern
          .compile(tf(1), Pattern.MULTILINE)
          .matcher(str)
          .matches()
      } else {
        throw new Exception(s"Invalid pattern $pattern for type boolean and value $str")
      }
    }

    def fromString(str: String, pattern: String): Any = {
      if (pattern.indexOf("<-TF->") >= 0) {
        val tf = pattern.split("<-TF->")
        if (Pattern.compile(tf(0), Pattern.MULTILINE).matcher(str).matches())
          true
        else if (Pattern.compile(tf(1), Pattern.MULTILINE).matcher(str).matches())
          false
        else
          throw new Exception(s"value $str does not match $pattern")
      } else {
        throw new Exception(s"Operator <-TF-> required in pattern $pattern to validate $str")
      }
    }

    def sparkType: DataType = BooleanType
  }

  object byte extends PrimitiveType("byte") {

    def fromString(str: String, pattern: String): Any =
      if (str == null || str.isEmpty) null else str.head.toByte

    def sparkType: DataType = ByteType
  }

  object struct extends PrimitiveType("struct") {

    def fromString(str: String, pattern: String): Any =
      if (str == null || str.isEmpty) null else str.toByte

    def sparkType: DataType = new StructType(Array.empty[StructField])
  }

  private def instantFromString(str: String, format: String): Instant = {
    import java.time.format.DateTimeFormatter
    format match {
      case "epoch_second" =>
        Instant.ofEpochSecond(str.toLong)
      case "epoch_milli" =>
        Instant.ofEpochMilli(str.toLong)
      case _ =>
        val formatter = PrimitiveType.dateFormatters
          .getOrElse(format, DateTimeFormatter.ofPattern(format))
        val dateTime: TemporalAccessor = formatter.parse(str)
        Try(Instant.from(dateTime)) match {
          case Success(instant) =>
            instant

          case Failure(_) =>
            Try {
              val localDateTime = LocalDateTime.from(dateTime)
              ZonedDateTime.of(localDateTime, ZoneId.systemDefault()).toInstant
            } match {
              case Success(instant) =>
                instant
              case Failure(_) =>
                // Try to parse it as a date without time and still make it a timestamp.
                // Cloudera 5.X with Hive 1.1 workaround
                import java.text.SimpleDateFormat
                val df = new SimpleDateFormat(format)
                val date = df.parse(str)
                Instant.ofEpochMilli(date.getTime)
            }
        }
    }
  }

  object date extends PrimitiveType("date") {

    def fromString(str: String, pattern: String): Any = {
      if (str == null || str.isEmpty)
        null
      else {
        import java.text.SimpleDateFormat
        val df = new SimpleDateFormat(pattern)
        val date = df.parse(str)
        new java.sql.Date(date.getTime)

      }
    }

    def sparkType: DataType = DateType
  }

  object timestamp extends PrimitiveType("timestamp") {

    def fromString(str: String, timeFormat: String): Any = {
      if (str == null || str.isEmpty)
        null
      else {
        val instant = instantFromString(str, timeFormat)
        Timestamp.from(instant)
      }
    }

    def sparkType: DataType = TimestampType
  }

  val primitiveTypes: Set[PrimitiveType] =
    Set(string, long, int, double, decimal, boolean, byte, date, timestamp, struct)

  import DateTimeFormatter._

  val dateFormatters: Map[String, DateTimeFormatter] = Map(
    "BASIC_ISO_DATE"       -> BASIC_ISO_DATE,
    "ISO_LOCAL_DATE"       -> ISO_LOCAL_DATE,
    "ISO_OFFSET_DATE"      -> ISO_OFFSET_DATE,
    "ISO_DATE"             -> ISO_DATE,
    "ISO_LOCAL_TIME"       -> ISO_LOCAL_TIME,
    "ISO_OFFSET_TIME"      -> ISO_OFFSET_TIME,
    "ISO_TIME"             -> ISO_TIME,
    "ISO_LOCAL_DATE_TIME"  -> ISO_LOCAL_DATE_TIME,
    "ISO_OFFSET_DATE_TIME" -> ISO_OFFSET_DATE_TIME,
    "ISO_ZONED_DATE_TIME"  -> ISO_ZONED_DATE_TIME,
    "ISO_DATE_TIME"        -> ISO_DATE_TIME,
    "ISO_ORDINAL_DATE"     -> ISO_ORDINAL_DATE,
    "ISO_WEEK_DATE"        -> ISO_WEEK_DATE,
    "ISO_INSTANT"          -> ISO_INSTANT,
    "RFC_1123_DATE_TIME"   -> RFC_1123_DATE_TIME
  )

}
