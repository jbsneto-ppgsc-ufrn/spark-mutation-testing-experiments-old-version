import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.scalatest.BeforeAndAfterAll
import org.scalatest.FunSuite
import org.apache.spark.rdd.RDD
import com.holdenkarau.spark.testing.SharedSparkContext

class SparkTestExample extends FunSuite with SharedSparkContext {

//  @transient private var _sc: SparkContext = _
//  def sc: SparkContext = _sc
//
//  val conf = new SparkConf().setMaster("local[*]").setAppName("test")
//
//  override def beforeAll() {
//    _sc = new SparkContext(conf)
//    super.beforeAll()
//  }

  test("really simple transformation") {
    val input = List("hi", "hi holden", "bye")
    val expected = List(List("hi"), List("hi", "holden"), List("bye"))
    assert(tokenize(sc.parallelize(input)).collect().toList === expected)
  }

  def tokenize(f: RDD[String]) = {
    f.map(_.split(" ").toList)
  }

//  override def afterAll() {
//    // We clear the driver port so that we don't try and bind to the same port on // restart.
//    sc.stop()
//    System.clearProperty("spark.driver.port")
//    _sc = null
//    super.afterAll()
//  }

}