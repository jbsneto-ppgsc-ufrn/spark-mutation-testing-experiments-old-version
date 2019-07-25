package br.ufrn.dimap.forall.spark.bigdatabenchmark.join

import org.scalatest.FunSuite

import com.holdenkarau.spark.testing.RDDComparisons
import com.holdenkarau.spark.testing.SharedSparkContext

class JoinQueryTest extends FunSuite with SharedSparkContext with RDDComparisons {

  def appUnderTest(x: Int) = x match {
    case 0  => JoinQuery.join(_, _)
    case 1  => JoinQueryMutant1.join(_, _)
    case 2  => JoinQueryMutant2.join(_, _)
    case 3  => JoinQueryMutant3.join(_, _)
    case 4  => JoinQueryMutant4.join(_, _)
    case 5  => JoinQueryMutant5.join(_, _)
    case 6  => JoinQueryMutant6.join(_, _)
    case 7  => JoinQueryMutant7.join(_, _)
    case 8  => JoinQueryMutant8.join(_, _)
    case 9  => JoinQueryMutant9.join(_, _)
    case 10 => JoinQueryMutant10.join(_, _)
    case 11 => JoinQueryMutant11.join(_, _)
    case 12 => JoinQueryMutant12.join(_, _)
    case 13 => JoinQueryMutant13.join(_, _)
    case 14 => JoinQueryMutant14.join(_, _)
    case 15 => JoinQueryMutant15.join(_, _)
    case 16 => JoinQueryMutant16.join(_, _)
    case 17 => JoinQueryMutant17.join(_, _)
    case 18 => JoinQueryMutant18.join(_, _)
    case 19 => JoinQueryMutant19.join(_, _)
    case 20 => JoinQueryMutant20.join(_, _)
    case 21 => JoinQueryMutant21.join(_, _)
    case 22 => JoinQueryMutant22.join(_, _)
    case 23 => JoinQueryMutant23.join(_, _)
    case 24 => JoinQueryMutant24.join(_, _)
    case 25 => JoinQueryMutant25.join(_, _)
    case 26 => JoinQueryMutant26.join(_, _)
    case 27 => JoinQueryMutant27.join(_, _)
    case _  => JoinQuery.join(_, _)
  }

  var x = 0

  while (x <= 27) {

    var app = appUnderTest(x)

    test("test 1 with " + (if (x == 0) "original" else ("mutant " + x.toString()))) {

      val inputRankings = List("url1,1,1")

      val inputUserVisits = List("0.0.0.0,url1,1980-02-01,1.0,test,test,test,test,1")

      val expected = List(("0.0.0.0", 1.0f, 1))

      val inputRankingsRDD = sc.parallelize(inputRankings)

      val inputUserVisitsRDD = sc.parallelize(inputUserVisits)

      val expectedRDD = sc.parallelize(expected)

      val resultRDD = app(inputRankingsRDD, inputUserVisitsRDD)

      assert(None === compareRDDWithOrder(resultRDD, expectedRDD))

    }
    
    test("test 2 with " + (if (x == 0) "original" else ("mutant " + x.toString()))) {

      val inputRankings = List("url1,1,1")

      val inputUserVisits = List("0.0.0.0,url1,1980-02-01,1.0,test,test,test,test,1", "0.0.0.0,url1,1979-02-01,1.0,test,test,test,test,1")

      val expected = List(("0.0.0.0", 1.0f, 1))

      val inputRankingsRDD = sc.parallelize(inputRankings)

      val inputUserVisitsRDD = sc.parallelize(inputUserVisits)

      val expectedRDD = sc.parallelize(expected)

      val resultRDD = app(inputRankingsRDD, inputUserVisitsRDD)

      assert(None === compareRDDWithOrder(resultRDD, expectedRDD))

    }
    
    test("test 3 with " + (if (x == 0) "original" else ("mutant " + x.toString()))) {

      val inputRankings = List("url1,1,1", "url2,2,2")

      val inputUserVisits = List("0.0.0.0,url1,1980-02-01,1.0,test,test,test,test,1")

      val expected = List(("0.0.0.0", 1.0f, 1))

      val inputRankingsRDD = sc.parallelize(inputRankings)

      val inputUserVisitsRDD = sc.parallelize(inputUserVisits)

      val expectedRDD = sc.parallelize(expected)

      val resultRDD = app(inputRankingsRDD, inputUserVisitsRDD)

      assert(None === compareRDDWithOrder(resultRDD, expectedRDD))

    }
    
    test("test 4 with " + (if (x == 0) "original" else ("mutant " + x.toString()))) {

      val inputRankings = List("url1,1,1")

      val inputUserVisits = List("0.0.0.0,url1,1980-02-01,1.0,test,test,test,test,1", "0.0.0.1,url2,1980-02-01,2.0,test,test,test,test,2")

      val expected = List(("0.0.0.0", 1.0f, 1))

      val inputRankingsRDD = sc.parallelize(inputRankings)

      val inputUserVisitsRDD = sc.parallelize(inputUserVisits)

      val expectedRDD = sc.parallelize(expected)

      val resultRDD = app(inputRankingsRDD, inputUserVisitsRDD)

      assert(None === compareRDDWithOrder(resultRDD, expectedRDD))

    }
    
    test("test 5 with " + (if (x == 0) "original" else ("mutant " + x.toString()))) {

      val inputRankings = List("url1,1,1", "url2,2,2")

      val inputUserVisits = List("0.0.0.0,url1,1980-02-01,1.0,test,test,test,test,1", "0.0.0.1,url2,1980-02-01,2.0,test,test,test,test,2")

      val expected = List(("0.0.0.1", 2.0f, 2), ("0.0.0.0", 1.0f, 1))

      val inputRankingsRDD = sc.parallelize(inputRankings)

      val inputUserVisitsRDD = sc.parallelize(inputUserVisits)

      val expectedRDD = sc.parallelize(expected)

      val resultRDD = app(inputRankingsRDD, inputUserVisitsRDD)

      assert(None === compareRDDWithOrder(resultRDD, expectedRDD))

    }
    
    test("test 6 with " + (if (x == 0) "original" else ("mutant " + x.toString()))) {

      val inputRankings = List("url1,1,1", "url2,2,2", "url1,1,1")

      val inputUserVisits = List("0.0.0.0,url1,1980-02-01,1.0,test,test,test,test,1", "0.0.0.1,url2,1980-02-01,2.0,test,test,test,test,2", "0.0.0.0,url1,1980-02-01,1.0,test,test,test,test,1")

      val expected = List(("0.0.0.0", 4.0f, 1), ("0.0.0.1", 2.0f, 2))

      val inputRankingsRDD = sc.parallelize(inputRankings)

      val inputUserVisitsRDD = sc.parallelize(inputUserVisits)

      val expectedRDD = sc.parallelize(expected)

      val resultRDD = app(inputRankingsRDD, inputUserVisitsRDD)

      assert(None === compareRDDWithOrder(resultRDD, expectedRDD))

    }

    x = x + 1
  }

}