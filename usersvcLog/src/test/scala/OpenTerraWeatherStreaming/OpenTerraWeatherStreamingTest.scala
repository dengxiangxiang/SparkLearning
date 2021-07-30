package OpenTerraWeatherStreaming

import org.scalatest.FunSuite

class OpenTerraWeatherStreamingTest extends  FunSuite{
  test(""){
    OpenTerraWeatherStreaming.main(Array("s3://sxm-isa-publish-telenav/publish/CurrentWeather/","600"))
  }

}
