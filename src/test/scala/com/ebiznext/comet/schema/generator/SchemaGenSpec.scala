package com.ebiznext.comet.schema.generator

import java.io.File

import com.ebiznext.comet.TestHelper

class SchemaGenSpec extends TestHelper {

  "Parsing a sample xlsx file" should "generate a yml file" in {

    SchemaGen.execute(getClass().getResource("/sample/SomeDomainTemplate.xlsx").getPath)
    val outputFile = new File(settings.comet.metadata + "/someDomain.yml")
    outputFile.exists() shouldBe true
  }

}
