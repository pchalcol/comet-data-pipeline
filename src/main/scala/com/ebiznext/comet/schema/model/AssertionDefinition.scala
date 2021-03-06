package com.ebiznext.comet.schema.model

case class AssertionDefinitions(assertions: Map[String, String]) {

  val assertionDefinitions: Map[String, AssertionDefinition] = {
    assertions.map { case (k, v) =>
      val assertionDefinition = AssertionDefinition.fromDefinition(k, v)
      (assertionDefinition.name, assertionDefinition)
    }
  }
}

case class AssertionDefinition(fullName: String, name: String, params: List[String], sql: String)

object AssertionDefinition {

  def extractNameAndParams(fullName: String): (String, List[String]) = {
    fullName
      .split('(') match {
      case Array(n, p) if p.length >= 1 =>
        (n.trim, p.dropRight(1).split(',').map(_.trim).filter(_.nonEmpty).toList)
      case Array(n) =>
        (n, Nil)
      case _ => throw new Exception(s"Invalid Assertion Definition syntax $fullName")
    }
  }

  def fromDefinition(fullName: String, sql: String): AssertionDefinition = {
    val (name, params) = extractNameAndParams(fullName)
    AssertionDefinition(fullName, name, params, sql)
  }

}
