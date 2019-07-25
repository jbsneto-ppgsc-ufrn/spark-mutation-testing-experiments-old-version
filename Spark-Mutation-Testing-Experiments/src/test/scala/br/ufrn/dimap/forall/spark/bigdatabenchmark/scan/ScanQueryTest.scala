package br.ufrn.dimap.forall.spark.bigdatabenchmark.scan

import org.scalatest.FunSuite

import com.holdenkarau.spark.testing.RDDComparisons
import com.holdenkarau.spark.testing.SharedSparkContext

class ScanQueryTest extends FunSuite with SharedSparkContext with RDDComparisons {

  def appUnderTest(x: Int) = x match {
    case 0  => ScanQuery.scan(_)
    case 1  => ScanQueryMutant1.scan(_)
    case 2  => ScanQueryMutant2.scan(_)
    case 3  => ScanQueryMutant3.scan(_)
    case 4  => ScanQueryMutant4.scan(_)
    case 5  => ScanQueryMutant5.scan(_)
    case 6  => ScanQueryMutant6.scan(_)
    case 7  => ScanQueryMutant7.scan(_)
    case 8  => ScanQueryMutant8.scan(_)
    case 9  => ScanQueryMutant9.scan(_)
    case 10 => ScanQueryMutant10.scan(_)
    case 11 => ScanQueryMutant11.scan(_)
    case 12 => ScanQueryMutant12.scan(_)
    case _  => ScanQuery.scan(_)
  }

  var x = 0
  while (x <= 12) {

    var app = appUnderTest(x)

    test("test 1 with one element RDD " + (if (x == 0) "original" else ("mutant " + x.toString()))) {

      val input = List("url1,301,1")

      val expected = List(("url1", 301))

      val inputRDD = sc.parallelize(input)

      val expectedRDD = sc.parallelize(expected)

      val resultRDD = app(inputRDD)

      assert(None === compareRDD(resultRDD, expectedRDD))

    }

    test("test 2 with false condition to filter " + (if (x == 0) "original" else ("mutant " + x.toString()))) {

      val input = List("url1,1,1")

      val expected: List[(String, Int)] = List()

      val inputRDD = sc.parallelize(input)

      val expectedRDD = sc.parallelize(expected)

      val resultRDD = app(inputRDD)

      assert(None === compareRDD(resultRDD, expectedRDD))

    }

    test("test 3 with repeated elements RDD " + (if (x == 0) "original" else ("mutant " + x.toString()))) {

      val input = List("url1,301,1", "url1,301,1")

      val expected = List(("url1", 301), ("url1", 301))

      val inputRDD = sc.parallelize(input)

      val expectedRDD = sc.parallelize(expected)

      val resultRDD = app(inputRDD)

      assert(None === compareRDD(resultRDD, expectedRDD))

    }

    x = x + 1
  }

}