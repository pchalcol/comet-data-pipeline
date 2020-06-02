package com.ebiznext.comet.airflow

import com.ebiznext.comet.config.Settings
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.StrictLogging
import org.fusesource.scalate.TemplateEngine

object DagGen extends StrictLogging {

  implicit val settings: Settings = Settings(ConfigFactory.load())
  val engine: TemplateEngine = new TemplateEngine
}
