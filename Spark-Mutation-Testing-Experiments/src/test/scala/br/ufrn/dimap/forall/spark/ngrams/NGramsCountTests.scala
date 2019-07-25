package br.ufrn.dimap.forall.spark.ngrams

import org.apache.spark.rdd.RDD
import org.scalatest.FunSuite

import com.holdenkarau.spark.testing.RDDComparisons
import com.holdenkarau.spark.testing.SharedSparkContext

class NGramsCountTests extends FunSuite with SharedSparkContext with RDDComparisons {

  def appUnderTest(x: Int) = x match {
    case 0  => NGramsCount.countNGrams(_, _)
    case 1  => NGramsCountMutant1.countNGrams(_, _)
    case 2  => NGramsCountMutant2.countNGrams(_, _)
    case 3  => NGramsCountMutant3.countNGrams(_, _)
    case 4  => NGramsCountMutant4.countNGrams(_, _)
    case 5  => NGramsCountMutant5.countNGrams(_, _)
    case 6  => NGramsCountMutant6.countNGrams(_, _)
    case 7  => NGramsCountMutant7.countNGrams(_, _)
    case 8  => NGramsCountMutant8.countNGrams(_, _)
    case 9  => NGramsCountMutant9.countNGrams(_, _)
    case 10 => NGramsCountMutant10.countNGrams(_, _)
    case 11 => NGramsCountMutant11.countNGrams(_, _)
    case 12 => NGramsCountMutant12.countNGrams(_, _)
    case 13 => NGramsCountMutant13.countNGrams(_, _)
    case 14 => NGramsCountMutant14.countNGrams(_, _)
    case 15 => NGramsCountMutant15.countNGrams(_, _)
    case 16 => NGramsCountMutant16.countNGrams(_, _)
    case 17 => NGramsCountMutant17.countNGrams(_, _)
    case 18 => NGramsCountMutant18.countNGrams(_, _)
    case 19 => NGramsCountMutant19.countNGrams(_, _)
    case 20 => NGramsCountMutant20.countNGrams(_, _)
    case 21 => NGramsCountMutant21.countNGrams(_, _)
    case 22 => NGramsCountMutant22.countNGrams(_, _)
    case 23 => NGramsCountMutant23.countNGrams(_, _)
    case 24 => NGramsCountMutant24.countNGrams(_, _)
    case 25 => NGramsCountMutant25.countNGrams(_, _)
    case 26 => NGramsCountMutant26.countNGrams(_, _)
    case 27 => NGramsCountMutant27.countNGrams(_, _)
    case _  => NGramsCount.countNGrams(_, _)
  }

  var x = 0

  var originalResultRDD: RDD[(List[String], Int)] = null

  while (x <= 27) {

    var app = appUnderTest(x)

    test("test 1 - One element with one sentence RDD - with " + (if (x == 0) "original" else ("mutant " + x.toString()))) {

      val n = 3

      val input = List("This is a sentece.")

      val expected = List((List("<start>", "<start>", "this"), 1),
        (List("<start>", "this", "is"), 1),
        (List("this", "is", "a"), 1),
        (List("is", "a", "sentece"), 1),
        (List("a", "sentece", "<end>"), 1))

      val inputRDD = sc.parallelize(input)

      val expectedRDD = sc.parallelize(expected)

      val resultRDD = app(n, inputRDD)

      assert(None === compareRDD(resultRDD, expectedRDD))

    }

    test("test 2 - RDD with Empty String - with " + (if (x == 0) "original" else ("mutant " + x.toString()))) {

      val n = 3

      val input = List("")

      val expected: List[(List[String], Int)] = List()

      val inputRDD = sc.parallelize(input)

      val expectedRDD = sc.parallelize(expected)

      val resultRDD = app(n, inputRDD)

      assert(None === compareRDD(resultRDD, expectedRDD))

    }

    test("test 3 - Two elements with the same sentence RDD - with " + (if (x == 0) "original" else ("mutant " + x.toString()))) {

      val n = 3

      val input = List("This is a sentece.", "This is a sentece.")

      val expected = List((List("<start>", "<start>", "this"), 2),
        (List("<start>", "this", "is"), 2),
        (List("this", "is", "a"), 2),
        (List("is", "a", "sentece"), 2),
        (List("a", "sentece", "<end>"), 2))

      val inputRDD = sc.parallelize(input)

      val expectedRDD = sc.parallelize(expected)

      val resultRDD = app(n, inputRDD)

      assert(None === compareRDD(resultRDD, expectedRDD))

    }

    test("test 4 - One element with two of the same sentence RDD - with " + (if (x == 0) "original" else ("mutant " + x.toString()))) {

      val n = 3

      val input = List("This is a sentece. This is a sentece.")

      val expected = List((List("<start>", "<start>", "this"), 2),
        (List("<start>", "this", "is"), 2),
        (List("this", "is", "a"), 2),
        (List("is", "a", "sentece"), 2),
        (List("a", "sentece", "<end>"), 2))

      val inputRDD = sc.parallelize(input)

      val expectedRDD = sc.parallelize(expected)

      val resultRDD = app(n, inputRDD)

      assert(None === compareRDD(resultRDD, expectedRDD))

    }

    test("test 5 - More than two of the same sentence RDD - with " + (if (x == 0) "original" else ("mutant " + x.toString()))) {

      val n = 3

      val input = List("This is a sentece.", "This is a sentece.", "This is a sentece.")

      val expected = List((List("<start>", "<start>", "this"), 3),
        (List("<start>", "this", "is"), 3),
        (List("this", "is", "a"), 3),
        (List("is", "a", "sentece"), 3),
        (List("a", "sentece", "<end>"), 3))

      val inputRDD = sc.parallelize(input)

      val expectedRDD = sc.parallelize(expected)

      val resultRDD = app(n, inputRDD)

      assert(None === compareRDD(resultRDD, expectedRDD))

    }

    x = x + 1
  }

}