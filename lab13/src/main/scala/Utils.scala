import io.circe.Encoder
import org.apache.commons.csv.{CSVFormat, CSVParser}
import java.nio.charset.Charset
import java.io.File
import scala.jdk.CollectionConverters._
import io.circe.generic.auto._

object Utils {

  case class Book( name: String, author: String, user_rating: Double,
                   reviews: Int, price: Double,  year: Int, genre: String)

  def getBooks(path:String): List[String] = {
    val file = new File(path)

    val parser = CSVParser.parse( file, Charset.forName("UTF-8"), CSVFormat.DEFAULT
      .withFirstRecordAsHeader()
      .withIgnoreHeaderCase()
      .withTrim())

    val book_lst = parser.getRecords.asScala.toList.map(
      rec => Book( rec.get("Name"), rec.get("Author"), rec.get("User Rating").toDouble,
        rec.get("Reviews").toInt, rec.get("Price").toDouble, rec.get("Year").toInt, rec.get("Genre"))
    )

    for (book <- book_lst) yield Encoder[Book].apply(book).noSpaces
  }

}
