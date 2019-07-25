package br.ufrn.dimap.forall.spark.movielens.ratings

import org.scalatest.FunSuite

import com.holdenkarau.spark.testing.RDDComparisons
import com.holdenkarau.spark.testing.SharedSparkContext

class MoviesRatingsAverageTest extends FunSuite with SharedSparkContext with RDDComparisons {

  def appUnderTest(x: Int) = x match {
    case 0  => MoviesRatingsAverage.moviesRatingsAverage(_, _)
    case 1  => MoviesRatingsAverageMutant1.moviesRatingsAverage(_, _)
    case 2  => MoviesRatingsAverageMutant2.moviesRatingsAverage(_, _)
    case 3  => MoviesRatingsAverageMutant3.moviesRatingsAverage(_, _)
    case 4  => MoviesRatingsAverageMutant4.moviesRatingsAverage(_, _)
    case 5  => MoviesRatingsAverageMutant5.moviesRatingsAverage(_, _)
    case 6  => MoviesRatingsAverageMutant6.moviesRatingsAverage(_, _)
    case 7  => MoviesRatingsAverageMutant7.moviesRatingsAverage(_, _)
    case 8  => MoviesRatingsAverageMutant8.moviesRatingsAverage(_, _)
    case 9  => MoviesRatingsAverageMutant9.moviesRatingsAverage(_, _)
    case 10 => MoviesRatingsAverageMutant10.moviesRatingsAverage(_, _)
    case 11 => MoviesRatingsAverageMutant11.moviesRatingsAverage(_, _)
    case 12 => MoviesRatingsAverageMutant12.moviesRatingsAverage(_, _)
    case 13 => MoviesRatingsAverageMutant13.moviesRatingsAverage(_, _)
    case 14 => MoviesRatingsAverageMutant14.moviesRatingsAverage(_, _)
    case 15 => MoviesRatingsAverageMutant15.moviesRatingsAverage(_, _)
    case 16 => MoviesRatingsAverageMutant16.moviesRatingsAverage(_, _)
    case 17 => MoviesRatingsAverageMutant17.moviesRatingsAverage(_, _)
    case 18 => MoviesRatingsAverageMutant18.moviesRatingsAverage(_, _)
    case 19 => MoviesRatingsAverageMutant19.moviesRatingsAverage(_, _)
    case 20 => MoviesRatingsAverageMutant20.moviesRatingsAverage(_, _)
    case 21 => MoviesRatingsAverageMutant21.moviesRatingsAverage(_, _)
    case 22 => MoviesRatingsAverageMutant22.moviesRatingsAverage(_, _)
    case 23 => MoviesRatingsAverageMutant23.moviesRatingsAverage(_, _)
    case 24 => MoviesRatingsAverageMutant24.moviesRatingsAverage(_, _)
    case 25 => MoviesRatingsAverageMutant25.moviesRatingsAverage(_, _)
    case _  => MoviesRatingsAverage.moviesRatingsAverage(_, _)
  }

  var x = 0

  while (x <= 25) {

    var app = appUnderTest(x)

    test("test 1 - One element - with " + (if (x == 0) "original" else ("mutant " + x.toString()))) {

      val movieNames = sc.broadcast(MoviesRatingsAverage.loadMovieNames())

      val input = List("1,19,5.0,1978-10-17")

      val expected = List("19,Ace Ventura: When Nature Calls (1995),5.0")

      val inputRDD = sc.parallelize(input)

      val expectedRDD = sc.parallelize(expected)

      val resultRDD = app(inputRDD, movieNames)

      assert(None === compareRDDWithOrder(resultRDD, expectedRDD))

    }

    test("test 2 - Two elements same movie - with " + (if (x == 0) "original" else ("mutant " + x.toString()))) {

      val movieNames = sc.broadcast(MoviesRatingsAverage.loadMovieNames())

      val input = List("1,19,5.0,1978-10-17", "1,19,3.0,1978-10-17")

      val expected = List("19,Ace Ventura: When Nature Calls (1995),4.0")

      val inputRDD = sc.parallelize(input)

      val expectedRDD = sc.parallelize(expected)

      val resultRDD = app(inputRDD, movieNames)

      assert(None === compareRDDWithOrder(resultRDD, expectedRDD))

    }

    test("test 3 - Three different movies - with " + (if (x == 0) "original" else ("mutant " + x.toString()))) {

      val movieNames = sc.broadcast(MoviesRatingsAverage.loadMovieNames())

      val input = List("1,44,4.0,1978-10-17", "2,19,5.0,1978-10-17", "3,1721,5.0,1978-10-17")

      val expected = List("1721,Titanic (1997),5.0", "44,Mortal Kombat (1995),4.0", "19,Ace Ventura: When Nature Calls (1995),5.0")

      val inputRDD = sc.parallelize(input)

      val expectedRDD = sc.parallelize(expected)

      val resultRDD = app(inputRDD, movieNames)

      assert(None === compareRDDWithOrder(resultRDD, expectedRDD))

    }

    test("test 4 - Three elements same movie and two with same data - with " + (if (x == 0) "original" else ("mutant " + x.toString()))) {

      val movieNames = sc.broadcast(MoviesRatingsAverage.loadMovieNames())

      val input = List("3,1721,5.0,1978-10-17", "3,1721,5.0,1978-10-17", "3,1721,2.0,1978-10-17")

      val expected = List("1721,Titanic (1997),4.0")

      val inputRDD = sc.parallelize(input)

      val expectedRDD = sc.parallelize(expected)

      val resultRDD = app(inputRDD, movieNames)

      assert(None === compareRDDWithOrder(resultRDD, expectedRDD))

    }

    x = x + 1
  }

}