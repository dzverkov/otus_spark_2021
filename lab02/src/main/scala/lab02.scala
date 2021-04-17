import scala.io.Source
import io.circe._
import io.circe.generic.auto._
import java.io.{FileOutputStream, PrintStream}



case class Country(name: String, capital: List[String], region: String,  area: Float)

case class CountryRes(name: String, capital: String, area: Float)

object lab02 {

    implicit val decoder: Decoder[Country] = new Decoder[Country] {
      override def apply(hCursor: HCursor): Decoder.Result[Country] =
        for {
          name <- hCursor.downField("name").get[String]("common")
          capital <- hCursor.get[List[String]]("capital")
          region <- hCursor.get[String]("region")
          area <- hCursor.downField("area").as[Float]
        } yield {
          Country(name, capital, region, area)
        }
    }

      def main(args: Array[String]): Unit = {

        def source = Source.fromURL("https://raw.githubusercontent.com/mledoze/countries/master/countries.json")
          .getLines.mkString

        val res:List[CountryRes] = parser.decode[List[Country]](source) match {
          case Right(countries) => countries.filter(_.region == "Africa").sortBy(_.area)(Ordering[Float].reverse).take(10)
            .map(rec => CountryRes(rec.name, rec.capital.head, rec.area))
        }

        val res_json = Encoder[List[CountryRes]].apply(res)
        //val fName = "/Users/dem/Documents/Projects/otus_spark_2021/lab02/data/lab02_out.json"
        val fName = args(0)
        val writer = new PrintStream(new FileOutputStream(fName))
        writer.println(res_json)
    }

}