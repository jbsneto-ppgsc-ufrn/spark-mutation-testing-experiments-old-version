package br.ufrn.dimap.forall.spark.bigdatabenchmark.aggregation

import org.scalatest.FunSuite

import com.holdenkarau.spark.testing.RDDComparisons
import com.holdenkarau.spark.testing.SharedSparkContext

class AggregationQueryTest extends FunSuite with SharedSparkContext with RDDComparisons {

  def appUnderTest(x: Int) = x match {
    case 0  => AggregationQuery.aggregation(_)
    case 1  => AggregationQueryMutant1.aggregation(_)
    case 2  => AggregationQueryMutant2.aggregation(_)
    case 3  => AggregationQueryMutant3.aggregation(_)
    case 4  => AggregationQueryMutant4.aggregation(_)
    case 5  => AggregationQueryMutant5.aggregation(_)
    case 6  => AggregationQueryMutant6.aggregation(_)
    case 7  => AggregationQueryMutant7.aggregation(_)
    case 8  => AggregationQueryMutant8.aggregation(_)
    case 9  => AggregationQueryMutant9.aggregation(_)
    case 10 => AggregationQueryMutant10.aggregation(_)
    case 11 => AggregationQueryMutant11.aggregation(_)
    case 12 => AggregationQueryMutant12.aggregation(_)
    case 13 => AggregationQueryMutant13.aggregation(_)
    case 14 => AggregationQueryMutant14.aggregation(_)
    case 15 => AggregationQueryMutant15.aggregation(_)
    case _  => AggregationQuery.aggregation(_)
  }

  var x = 0
  while (x <= 15) {

    var app = appUnderTest(x)

    test("test 1 with one element RDD " + (if (x == 0) "original" else ("mutant " + x.toString()))) {

      val input = List("0.0.0.0,test,1978-10-17,1.0,test,test,test,test,1")

      val expected = List(("0.0.0.0", 1.0f))

      val inputRDD = sc.parallelize(input)

      val expectedRDD = sc.parallelize(expected)

      val resultRDD = app(inputRDD)

      assert(None === compareRDD(resultRDD, expectedRDD))

    }

    test("test 2 with repeated element RDD " + (if (x == 0) "original" else ("mutant " + x.toString()))) {

      val input = List("0.0.0.0,test,1978-10-17,1.0,test,test,test,test,1", "0.0.0.0,test,1978-10-17,1.0,test,test,test,test,1")

      val expected = List(("0.0.0.0", 2.0f))

      val inputRDD = sc.parallelize(input)

      val expectedRDD = sc.parallelize(expected)

      val resultRDD = app(inputRDD)

      assert(None === compareRDD(resultRDD, expectedRDD))

    }

    test("test 3 with two elements with same key and different values RDD " + (if (x == 0) "original" else ("mutant " + x.toString()))) {

      val input = List("0.0.0.0,test,1978-10-17,1.0,test,test,test,test,1", "0.0.0.0,test,1978-10-17,2.0,test,test,test,test,1")

      val expected = List(("0.0.0.0", 3.0f))

      val inputRDD = sc.parallelize(input)

      val expectedRDD = sc.parallelize(expected)

      val resultRDD = app(inputRDD)

      assert(None === compareRDD(resultRDD, expectedRDD))

    }

    x = x + 1

  }
}