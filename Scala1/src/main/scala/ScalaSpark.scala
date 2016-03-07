/**
 * Created by dheeraj on 3/4/16.
 */
class ScalaSpark {
  def method1 = {
    val list = List(4, 5, 6)
    var sum = 0;
    list.foreach { x => if (x % 2 == 0) sum += x }
  }

}
object ScalaSpark {
  def main (args: Array[String]){
  println(new ScalaSpark().method1)
  }
}