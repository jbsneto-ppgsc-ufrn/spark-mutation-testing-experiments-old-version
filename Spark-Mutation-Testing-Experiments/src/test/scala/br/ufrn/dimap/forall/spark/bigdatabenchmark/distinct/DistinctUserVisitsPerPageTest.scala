package br.ufrn.dimap.forall.spark.bigdatabenchmark.distinct

import org.scalatest.FunSuite

import com.holdenkarau.spark.testing.RDDComparisons
import com.holdenkarau.spark.testing.SharedSparkContext

class DistinctUserVisitsPerPageTest extends FunSuite with SharedSparkContext with RDDComparisons {

  def appUnderTest(x: Int) = x match {
    case 0  => DistinctUserVisitsPerPage.distinctUserVisitsPerPage(_)
    case 1  => DistinctUserVisitsPerPageMutant1.distinctUserVisitsPerPage(_)
    case 2  => DistinctUserVisitsPerPageMutant2.distinctUserVisitsPerPage(_)
    case 3  => DistinctUserVisitsPerPageMutant3.distinctUserVisitsPerPage(_)
    case 4  => DistinctUserVisitsPerPageMutant4.distinctUserVisitsPerPage(_)
    case 5  => DistinctUserVisitsPerPageMutant5.distinctUserVisitsPerPage(_)
    case 6  => DistinctUserVisitsPerPageMutant6.distinctUserVisitsPerPage(_)
    case 7  => DistinctUserVisitsPerPageMutant7.distinctUserVisitsPerPage(_)
    case 8  => DistinctUserVisitsPerPageMutant8.distinctUserVisitsPerPage(_)
    case 9  => DistinctUserVisitsPerPageMutant9.distinctUserVisitsPerPage(_)
    case 10 => DistinctUserVisitsPerPageMutant10.distinctUserVisitsPerPage(_)
    case 11 => DistinctUserVisitsPerPageMutant11.distinctUserVisitsPerPage(_)
    case 12 => DistinctUserVisitsPerPageMutant12.distinctUserVisitsPerPage(_)
    case 13 => DistinctUserVisitsPerPageMutant13.distinctUserVisitsPerPage(_)
    case 14 => DistinctUserVisitsPerPageMutant14.distinctUserVisitsPerPage(_)
    case 15 => DistinctUserVisitsPerPageMutant15.distinctUserVisitsPerPage(_)
    case 16 => DistinctUserVisitsPerPageMutant16.distinctUserVisitsPerPage(_)
    case _  => DistinctUserVisitsPerPage.distinctUserVisitsPerPage(_)
  }

  var x = 0
  while (x <= 16) {

    var app = appUnderTest(x)

    test("test 1 with two elements with the same destURL and sourceIP RDD " + (if (x == 0) "original" else ("mutant " + x.toString()))) {

      val input = List("0.0.0.0,test,1978-10-17,1.0,test,test,test,test,1", "0.0.0.0,test,1978-10-17,1.0,test,test,test,test,1")

      val expected = List(("test", Set("0.0.0.0")))

      val inputRDD = sc.parallelize(input)

      val expectedRDD = sc.parallelize(expected)

      val resultRDD = app(inputRDD)

      assert(None === compareRDD(resultRDD, expectedRDD))

    }

    test("test 2 with two elements with the same destURL and different sourceIP RDD " + (if (x == 0) "original" else ("mutant " + x.toString()))) {

      val input = List("0.0.0.0,test,1978-10-17,1.0,test,test,test,test,1", "0.0.0.1,test,1978-10-17,1.0,test,test,test,test,1")

      val expected = List(("test", Set("0.0.0.0", "0.0.0.1")))

      val inputRDD = sc.parallelize(input)

      val expectedRDD = sc.parallelize(expected)

      val resultRDD = app(inputRDD)

      assert(None === compareRDD(resultRDD, expectedRDD))

    }

    x = x + 1
  }

}