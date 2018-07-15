import org.scalatest.FlatSpec
import tavonatti.scalco.wikipedia_parsing.Utils

class DateFormatTest extends FlatSpec{

  val format = Utils.format

  "2008-04-25T15:49:58Z " should " be parsed" in {
    val time=format.parse(" 2008-04-25T15:49:58Z").getTime()
    println(time)
  }
}
