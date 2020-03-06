package com.ebiznext.comet.schema.generator

import java.io.File

import com.ebiznext.comet.TestHelper
import com.ebiznext.comet.schema.model.{Domain, Format}

class SchemaGenSpec extends TestHelper {

  "Parsing a sample xlsx file" should "generate a yml file" in {

    SchemaGen.execute(getClass().getResource("/sample/SomeDomainTemplate.xlsx").getPath)
    val outputFile = new File(settings.comet.metadata + "/someDomain.yml")
    outputFile.exists() shouldBe true
    val result = YamlSerializer.mapper.readValue(outputFile,classOf[Domain])
    result.name shouldBe "someDomain"
    result.schemas.size shouldBe 2
    val schema1 = result.schemas.filter(_.name == "SCHEMA1").head
    schema1.metadata.flatMap(_.format) shouldBe Some(Format.POSITION)
    schema1.attributes.size shouldBe 19
    val schema2 = result.schemas.filter(_.name == "SCHEMA2").head
    schema2.metadata.flatMap(_.format) shouldBe Some(Format.DSV)
    schema2.attributes.size shouldBe 19
  }

}
