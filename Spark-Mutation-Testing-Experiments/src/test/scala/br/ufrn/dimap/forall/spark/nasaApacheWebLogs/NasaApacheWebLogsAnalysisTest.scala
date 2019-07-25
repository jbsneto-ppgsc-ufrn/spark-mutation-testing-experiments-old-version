package br.ufrn.dimap.forall.spark.nasaApacheWebLogs

import org.scalatest.FunSuite

import com.holdenkarau.spark.testing.RDDComparisons
import com.holdenkarau.spark.testing.SharedSparkContext

class NasaApacheWebLogsAnalysisTest extends FunSuite with SharedSparkContext with RDDComparisons {

  def sameHostProblemUnderTest(x: Int) = x match {
    case 0  => NasaApacheWebLogsAnalysis.sameHostProblem(_, _)
    case 1  => NasaApacheWebLogsAnalysisMutant1.sameHostProblem(_, _)
    case 2  => NasaApacheWebLogsAnalysisMutant2.sameHostProblem(_, _)
    case 3  => NasaApacheWebLogsAnalysisMutant3.sameHostProblem(_, _)
    case 4  => NasaApacheWebLogsAnalysisMutant4.sameHostProblem(_, _)
    case 5  => NasaApacheWebLogsAnalysisMutant5.sameHostProblem(_, _)
    case 6  => NasaApacheWebLogsAnalysisMutant6.sameHostProblem(_, _)
    case 7  => NasaApacheWebLogsAnalysisMutant7.sameHostProblem(_, _)
    case 8  => NasaApacheWebLogsAnalysisMutant8.sameHostProblem(_, _)
    case 9  => NasaApacheWebLogsAnalysisMutant9.sameHostProblem(_, _)
    case 10 => NasaApacheWebLogsAnalysisMutant10.sameHostProblem(_, _)
    case 11 => NasaApacheWebLogsAnalysisMutant11.sameHostProblem(_, _)
    case 12 => NasaApacheWebLogsAnalysisMutant12.sameHostProblem(_, _)
    case 13 => NasaApacheWebLogsAnalysisMutant13.sameHostProblem(_, _)
    case 14 => NasaApacheWebLogsAnalysisMutant14.sameHostProblem(_, _)
    case 15 => NasaApacheWebLogsAnalysisMutant15.sameHostProblem(_, _)
    case 16 => NasaApacheWebLogsAnalysisMutant16.sameHostProblem(_, _)
    case 17 => NasaApacheWebLogsAnalysisMutant17.sameHostProblem(_, _)
    case 18 => NasaApacheWebLogsAnalysisMutant18.sameHostProblem(_, _)
    case 19 => NasaApacheWebLogsAnalysisMutant19.sameHostProblem(_, _)
    case 20 => NasaApacheWebLogsAnalysisMutant20.sameHostProblem(_, _)
    case 21 => NasaApacheWebLogsAnalysisMutant21.sameHostProblem(_, _)
    case 22 => NasaApacheWebLogsAnalysisMutant22.sameHostProblem(_, _)
    case 23 => NasaApacheWebLogsAnalysisMutant23.sameHostProblem(_, _)
    case 24 => NasaApacheWebLogsAnalysisMutant24.sameHostProblem(_, _)
    case 25 => NasaApacheWebLogsAnalysisMutant25.sameHostProblem(_, _)
    case 26 => NasaApacheWebLogsAnalysisMutant26.sameHostProblem(_, _)
    case 27 => NasaApacheWebLogsAnalysisMutant27.sameHostProblem(_, _)
    case 28 => NasaApacheWebLogsAnalysisMutant28.sameHostProblem(_, _)
    case 29 => NasaApacheWebLogsAnalysisMutant29.sameHostProblem(_, _)
    case 30 => NasaApacheWebLogsAnalysisMutant30.sameHostProblem(_, _)
    case 31 => NasaApacheWebLogsAnalysisMutant31.sameHostProblem(_, _)
    case 32 => NasaApacheWebLogsAnalysisMutant32.sameHostProblem(_, _)
    case 33 => NasaApacheWebLogsAnalysisMutant33.sameHostProblem(_, _)
    case 34 => NasaApacheWebLogsAnalysisMutant34.sameHostProblem(_, _)
    case 35 => NasaApacheWebLogsAnalysisMutant35.sameHostProblem(_, _)
    case 36 => NasaApacheWebLogsAnalysisMutant36.sameHostProblem(_, _)
    case 37 => NasaApacheWebLogsAnalysisMutant37.sameHostProblem(_, _)
    case 38 => NasaApacheWebLogsAnalysisMutant38.sameHostProblem(_, _)
    case 39 => NasaApacheWebLogsAnalysisMutant39.sameHostProblem(_, _)
    case 40 => NasaApacheWebLogsAnalysisMutant40.sameHostProblem(_, _)
    case 41 => NasaApacheWebLogsAnalysisMutant41.sameHostProblem(_, _)
    case 42 => NasaApacheWebLogsAnalysisMutant42.sameHostProblem(_, _)
    case 43 => NasaApacheWebLogsAnalysisMutant43.sameHostProblem(_, _)
    case 44 => NasaApacheWebLogsAnalysisMutant44.sameHostProblem(_, _)
    case 45 => NasaApacheWebLogsAnalysisMutant45.sameHostProblem(_, _)
    case 46 => NasaApacheWebLogsAnalysisMutant46.sameHostProblem(_, _)
    case 47 => NasaApacheWebLogsAnalysisMutant47.sameHostProblem(_, _)
    case 48 => NasaApacheWebLogsAnalysisMutant48.sameHostProblem(_, _)
    case 49 => NasaApacheWebLogsAnalysisMutant49.sameHostProblem(_, _)
    case 50 => NasaApacheWebLogsAnalysisMutant50.sameHostProblem(_, _)
    case 51 => NasaApacheWebLogsAnalysisMutant51.sameHostProblem(_, _)
    case 52 => NasaApacheWebLogsAnalysisMutant52.sameHostProblem(_, _)
    case 53 => NasaApacheWebLogsAnalysisMutant53.sameHostProblem(_, _)
    case 54 => NasaApacheWebLogsAnalysisMutant54.sameHostProblem(_, _)
    case 55 => NasaApacheWebLogsAnalysisMutant55.sameHostProblem(_, _)
    case _  => NasaApacheWebLogsAnalysis.sameHostProblem(_, _)
  }

  def unionLogsProblemUnderTest(x: Int) = x match {
    case 0  => NasaApacheWebLogsAnalysis.unionLogsProblem(_, _)
    case 1  => NasaApacheWebLogsAnalysisMutant1.unionLogsProblem(_, _)
    case 2  => NasaApacheWebLogsAnalysisMutant2.unionLogsProblem(_, _)
    case 3  => NasaApacheWebLogsAnalysisMutant3.unionLogsProblem(_, _)
    case 4  => NasaApacheWebLogsAnalysisMutant4.unionLogsProblem(_, _)
    case 5  => NasaApacheWebLogsAnalysisMutant5.unionLogsProblem(_, _)
    case 6  => NasaApacheWebLogsAnalysisMutant6.unionLogsProblem(_, _)
    case 7  => NasaApacheWebLogsAnalysisMutant7.unionLogsProblem(_, _)
    case 8  => NasaApacheWebLogsAnalysisMutant8.unionLogsProblem(_, _)
    case 9  => NasaApacheWebLogsAnalysisMutant9.unionLogsProblem(_, _)
    case 10 => NasaApacheWebLogsAnalysisMutant10.unionLogsProblem(_, _)
    case 11 => NasaApacheWebLogsAnalysisMutant11.unionLogsProblem(_, _)
    case 12 => NasaApacheWebLogsAnalysisMutant12.unionLogsProblem(_, _)
    case 13 => NasaApacheWebLogsAnalysisMutant13.unionLogsProblem(_, _)
    case 14 => NasaApacheWebLogsAnalysisMutant14.unionLogsProblem(_, _)
    case 15 => NasaApacheWebLogsAnalysisMutant15.unionLogsProblem(_, _)
    case 16 => NasaApacheWebLogsAnalysisMutant16.unionLogsProblem(_, _)
    case 17 => NasaApacheWebLogsAnalysisMutant17.unionLogsProblem(_, _)
    case 18 => NasaApacheWebLogsAnalysisMutant18.unionLogsProblem(_, _)
    case 19 => NasaApacheWebLogsAnalysisMutant19.unionLogsProblem(_, _)
    case 20 => NasaApacheWebLogsAnalysisMutant20.unionLogsProblem(_, _)
    case 21 => NasaApacheWebLogsAnalysisMutant21.unionLogsProblem(_, _)
    case 22 => NasaApacheWebLogsAnalysisMutant22.unionLogsProblem(_, _)
    case 23 => NasaApacheWebLogsAnalysisMutant23.unionLogsProblem(_, _)
    case 24 => NasaApacheWebLogsAnalysisMutant24.unionLogsProblem(_, _)
    case 25 => NasaApacheWebLogsAnalysisMutant25.unionLogsProblem(_, _)
    case 26 => NasaApacheWebLogsAnalysisMutant26.unionLogsProblem(_, _)
    case 27 => NasaApacheWebLogsAnalysisMutant27.unionLogsProblem(_, _)
    case 28 => NasaApacheWebLogsAnalysisMutant28.unionLogsProblem(_, _)
    case 29 => NasaApacheWebLogsAnalysisMutant29.unionLogsProblem(_, _)
    case 30 => NasaApacheWebLogsAnalysisMutant30.unionLogsProblem(_, _)
    case 31 => NasaApacheWebLogsAnalysisMutant31.unionLogsProblem(_, _)
    case 32 => NasaApacheWebLogsAnalysisMutant32.unionLogsProblem(_, _)
    case 33 => NasaApacheWebLogsAnalysisMutant33.unionLogsProblem(_, _)
    case 34 => NasaApacheWebLogsAnalysisMutant34.unionLogsProblem(_, _)
    case 35 => NasaApacheWebLogsAnalysisMutant35.unionLogsProblem(_, _)
    case 36 => NasaApacheWebLogsAnalysisMutant36.unionLogsProblem(_, _)
    case 37 => NasaApacheWebLogsAnalysisMutant37.unionLogsProblem(_, _)
    case 38 => NasaApacheWebLogsAnalysisMutant38.unionLogsProblem(_, _)
    case 39 => NasaApacheWebLogsAnalysisMutant39.unionLogsProblem(_, _)
    case 40 => NasaApacheWebLogsAnalysisMutant40.unionLogsProblem(_, _)
    case 41 => NasaApacheWebLogsAnalysisMutant41.unionLogsProblem(_, _)
    case 42 => NasaApacheWebLogsAnalysisMutant42.unionLogsProblem(_, _)
    case 43 => NasaApacheWebLogsAnalysisMutant43.unionLogsProblem(_, _)
    case 44 => NasaApacheWebLogsAnalysisMutant44.unionLogsProblem(_, _)
    case 45 => NasaApacheWebLogsAnalysisMutant45.unionLogsProblem(_, _)
    case 46 => NasaApacheWebLogsAnalysisMutant46.unionLogsProblem(_, _)
    case 47 => NasaApacheWebLogsAnalysisMutant47.unionLogsProblem(_, _)
    case 48 => NasaApacheWebLogsAnalysisMutant48.unionLogsProblem(_, _)
    case 49 => NasaApacheWebLogsAnalysisMutant49.unionLogsProblem(_, _)
    case 50 => NasaApacheWebLogsAnalysisMutant50.unionLogsProblem(_, _)
    case 51 => NasaApacheWebLogsAnalysisMutant51.unionLogsProblem(_, _)
    case 52 => NasaApacheWebLogsAnalysisMutant52.unionLogsProblem(_, _)
    case 53 => NasaApacheWebLogsAnalysisMutant53.unionLogsProblem(_, _)
    case 54 => NasaApacheWebLogsAnalysisMutant54.unionLogsProblem(_, _)
    case 55 => NasaApacheWebLogsAnalysisMutant55.unionLogsProblem(_, _)
    case _  => NasaApacheWebLogsAnalysis.unionLogsProblem(_, _)
  }

  var x = 0

  while (x <= 55) {

    var sameHostProblem = sameHostProblemUnderTest(x)
    var unionLogsProblem = unionLogsProblemUnderTest(x)

    test("test 1 - SHP - Same hosts with header - " + (if (x == 0) "original" else ("mutant " + x.toString()))) {

      val input1 = List("host	logname	time	method	url	response	bytes", "199.72.81.55	-	804571201	GET	/history/apollo/	200	6245	")
      val input2 = List("host	logname	time	method	url	response	bytes", "199.72.81.55	-	804571201	GET	/history/apollo/	200	6245	")

      val expected = List("199.72.81.55")

      val inputRDD1 = sc.parallelize(input1)
      val inputRDD2 = sc.parallelize(input2)

      val expectedRDD = sc.parallelize(expected)

      val resultRDD = sameHostProblem(inputRDD1, inputRDD2)

      assert(None === compareRDD(resultRDD, expectedRDD))

    }

    test("test 2 - UP - Same hosts with header - " + (if (x == 0) "original" else ("mutant " + x.toString()))) {

      val input1 = List("host	logname	time	method	url	response	bytes", "199.72.81.55	-	804571201	GET	/history/apollo/	200	6245	")
      val input2 = List("host	logname	time	method	url	response	bytes", "199.72.81.55	-	804571201	GET	/history/apollo/	200	6245	")

      val expected = List("199.72.81.55	-	804571201	GET	/history/apollo/	200	6245	")

      val inputRDD1 = sc.parallelize(input1)
      val inputRDD2 = sc.parallelize(input2)

      val expectedRDD = sc.parallelize(expected)

      val resultRDD = unionLogsProblem(inputRDD1, inputRDD2)

      assert(None === compareRDD(resultRDD, expectedRDD))

    }

    test("test 3 - SHP - Different hosts with header - " + (if (x == 0) "original" else ("mutant " + x.toString()))) {

      val input1 = List("host	logname	time	method	url	response	bytes", "199.72.81.55	-	804571201	GET	/history/apollo/	200	6245	")
      val input2 = List("host	logname	time	method	url	response	bytes", "in24.inetnebr.com	-	807249601	GET	/shuttle/missions/sts-68/news/sts-68-mcc-05.txt	200	1839		")

      val expected: List[String] = List()

      val inputRDD1 = sc.parallelize(input1)
      val inputRDD2 = sc.parallelize(input2)

      val expectedRDD = sc.parallelize(expected)

      val resultRDD = sameHostProblem(inputRDD1, inputRDD2)

      assert(None === compareRDD(resultRDD, expectedRDD))

    }

    test("test 4 - UP - Different hosts with header - " + (if (x == 0) "original" else ("mutant " + x.toString()))) {

      val input1 = List("host	logname	time	method	url	response	bytes", "199.72.81.55	-	804571201	GET	/history/apollo/	200	6245	")
      val input2 = List("host	logname	time	method	url	response	bytes", "in24.inetnebr.com	-	807249601	GET	/shuttle/missions/sts-68/news/sts-68-mcc-05.txt	200	1839		")

      val expected = List("199.72.81.55	-	804571201	GET	/history/apollo/	200	6245	", "in24.inetnebr.com	-	807249601	GET	/shuttle/missions/sts-68/news/sts-68-mcc-05.txt	200	1839		")

      val inputRDD1 = sc.parallelize(input1)
      val inputRDD2 = sc.parallelize(input2)

      val expectedRDD = sc.parallelize(expected)

      val resultRDD = unionLogsProblem(inputRDD1, inputRDD2)

      assert(None === compareRDD(resultRDD, expectedRDD))

    }

    x = x + 1

  }

}