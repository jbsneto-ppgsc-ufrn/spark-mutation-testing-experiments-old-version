package br.ufrn.dimap.forall.spark.movielens.recomendation

import org.scalatest.FunSuite

import com.holdenkarau.spark.testing.RDDComparisons
import com.holdenkarau.spark.testing.SharedSparkContext

class MoviesRecommendationTest extends FunSuite with SharedSparkContext with RDDComparisons {

  def moviesSimilaritiesTableUnderTest(x: Int) = x match {
    case 0  => MoviesRecommendation.moviesSimilaritiesTable(_)
    case 1  => MoviesRecommendationMutant1.moviesSimilaritiesTable(_)
    case 2  => MoviesRecommendationMutant2.moviesSimilaritiesTable(_)
    case 3  => MoviesRecommendationMutant3.moviesSimilaritiesTable(_)
    case 4  => MoviesRecommendationMutant4.moviesSimilaritiesTable(_)
    case 5  => MoviesRecommendationMutant5.moviesSimilaritiesTable(_)
    case 6  => MoviesRecommendationMutant6.moviesSimilaritiesTable(_)
    case 7  => MoviesRecommendationMutant7.moviesSimilaritiesTable(_)
    case 8  => MoviesRecommendationMutant8.moviesSimilaritiesTable(_)
    case 9  => MoviesRecommendationMutant9.moviesSimilaritiesTable(_)
    case 10 => MoviesRecommendationMutant10.moviesSimilaritiesTable(_)
    case 11 => MoviesRecommendationMutant11.moviesSimilaritiesTable(_)
    case 12 => MoviesRecommendationMutant12.moviesSimilaritiesTable(_)
    case 13 => MoviesRecommendationMutant13.moviesSimilaritiesTable(_)
    case 14 => MoviesRecommendationMutant14.moviesSimilaritiesTable(_)
    case 15 => MoviesRecommendationMutant15.moviesSimilaritiesTable(_)
    case 16 => MoviesRecommendationMutant16.moviesSimilaritiesTable(_)
    case 17 => MoviesRecommendationMutant17.moviesSimilaritiesTable(_)
    case 18 => MoviesRecommendationMutant18.moviesSimilaritiesTable(_)
    case 19 => MoviesRecommendationMutant19.moviesSimilaritiesTable(_)
    case 20 => MoviesRecommendationMutant20.moviesSimilaritiesTable(_)
    case 21 => MoviesRecommendationMutant21.moviesSimilaritiesTable(_)
    case 22 => MoviesRecommendationMutant22.moviesSimilaritiesTable(_)
    case 23 => MoviesRecommendationMutant23.moviesSimilaritiesTable(_)
    case 24 => MoviesRecommendationMutant24.moviesSimilaritiesTable(_)
    case 25 => MoviesRecommendationMutant25.moviesSimilaritiesTable(_)
    case 26 => MoviesRecommendationMutant26.moviesSimilaritiesTable(_)
    case 27 => MoviesRecommendationMutant27.moviesSimilaritiesTable(_)
    case 28 => MoviesRecommendationMutant28.moviesSimilaritiesTable(_)
    case 29 => MoviesRecommendationMutant29.moviesSimilaritiesTable(_)
    case 30 => MoviesRecommendationMutant30.moviesSimilaritiesTable(_)
    case 31 => MoviesRecommendationMutant31.moviesSimilaritiesTable(_)
    case 32 => MoviesRecommendationMutant32.moviesSimilaritiesTable(_)
    case 33 => MoviesRecommendationMutant33.moviesSimilaritiesTable(_)
    case 34 => MoviesRecommendationMutant34.moviesSimilaritiesTable(_)
    case 35 => MoviesRecommendationMutant35.moviesSimilaritiesTable(_)
    case 36 => MoviesRecommendationMutant36.moviesSimilaritiesTable(_)
    case 37 => MoviesRecommendationMutant37.moviesSimilaritiesTable(_)
    case _  => MoviesRecommendation.moviesSimilaritiesTable(_)
  }

  def topNMoviesRecommendationUnderTest(x: Int) = x match {
    case 0  => MoviesRecommendation.topNMoviesRecommendation(_, _, _, _)
    case 1  => MoviesRecommendationMutant1.topNMoviesRecommendation(_, _, _, _)
    case 2  => MoviesRecommendationMutant2.topNMoviesRecommendation(_, _, _, _)
    case 3  => MoviesRecommendationMutant3.topNMoviesRecommendation(_, _, _, _)
    case 4  => MoviesRecommendationMutant4.topNMoviesRecommendation(_, _, _, _)
    case 5  => MoviesRecommendationMutant5.topNMoviesRecommendation(_, _, _, _)
    case 6  => MoviesRecommendationMutant6.topNMoviesRecommendation(_, _, _, _)
    case 7  => MoviesRecommendationMutant7.topNMoviesRecommendation(_, _, _, _)
    case 8  => MoviesRecommendationMutant8.topNMoviesRecommendation(_, _, _, _)
    case 9  => MoviesRecommendationMutant9.topNMoviesRecommendation(_, _, _, _)
    case 10 => MoviesRecommendationMutant10.topNMoviesRecommendation(_, _, _, _)
    case 11 => MoviesRecommendationMutant11.topNMoviesRecommendation(_, _, _, _)
    case 12 => MoviesRecommendationMutant12.topNMoviesRecommendation(_, _, _, _)
    case 13 => MoviesRecommendationMutant13.topNMoviesRecommendation(_, _, _, _)
    case 14 => MoviesRecommendationMutant14.topNMoviesRecommendation(_, _, _, _)
    case 15 => MoviesRecommendationMutant15.topNMoviesRecommendation(_, _, _, _)
    case 16 => MoviesRecommendationMutant16.topNMoviesRecommendation(_, _, _, _)
    case 17 => MoviesRecommendationMutant17.topNMoviesRecommendation(_, _, _, _)
    case 18 => MoviesRecommendationMutant18.topNMoviesRecommendation(_, _, _, _)
    case 19 => MoviesRecommendationMutant19.topNMoviesRecommendation(_, _, _, _)
    case 20 => MoviesRecommendationMutant20.topNMoviesRecommendation(_, _, _, _)
    case 21 => MoviesRecommendationMutant21.topNMoviesRecommendation(_, _, _, _)
    case 22 => MoviesRecommendationMutant22.topNMoviesRecommendation(_, _, _, _)
    case 23 => MoviesRecommendationMutant23.topNMoviesRecommendation(_, _, _, _)
    case 24 => MoviesRecommendationMutant24.topNMoviesRecommendation(_, _, _, _)
    case 25 => MoviesRecommendationMutant25.topNMoviesRecommendation(_, _, _, _)
    case 26 => MoviesRecommendationMutant26.topNMoviesRecommendation(_, _, _, _)
    case 27 => MoviesRecommendationMutant27.topNMoviesRecommendation(_, _, _, _)
    case 28 => MoviesRecommendationMutant28.topNMoviesRecommendation(_, _, _, _)
    case 29 => MoviesRecommendationMutant29.topNMoviesRecommendation(_, _, _, _)
    case 30 => MoviesRecommendationMutant30.topNMoviesRecommendation(_, _, _, _)
    case 31 => MoviesRecommendationMutant31.topNMoviesRecommendation(_, _, _, _)
    case 32 => MoviesRecommendationMutant32.topNMoviesRecommendation(_, _, _, _)
    case 33 => MoviesRecommendationMutant33.topNMoviesRecommendation(_, _, _, _)
    case 34 => MoviesRecommendationMutant34.topNMoviesRecommendation(_, _, _, _)
    case 35 => MoviesRecommendationMutant35.topNMoviesRecommendation(_, _, _, _)
    case 36 => MoviesRecommendationMutant36.topNMoviesRecommendation(_, _, _, _)
    case 37 => MoviesRecommendationMutant37.topNMoviesRecommendation(_, _, _, _)
    case _  => MoviesRecommendation.topNMoviesRecommendation(_, _, _, _)
  }

  var x = 0

  while (x <= 37) {

    var moviesSimilaritiesTable = moviesSimilaritiesTableUnderTest(x)
    var topNMoviesRecommendation = topNMoviesRecommendationUnderTest(x)

    test("test 1 - Similarities Table with " + (if (x == 0) "original" else ("mutant " + x.toString()))) {
      val input = List("1,1,1.0,964982703", "1,3,1.0,964981247")

      val expected = List(((1, 3), (1.0, 1)))

      val inputRDD = sc.parallelize(input)

      val expectedRDD = sc.parallelize(expected)

      val resultRDD = moviesSimilaritiesTable(inputRDD)

      val result = resultRDD.collect()

      assert(None === compareRDDWithOrder(resultRDD, expectedRDD))
    }

    test("test 2 - Recomendations with " + (if (x == 0) "original" else ("mutant " + x.toString()))) {
      val input = List("1,1,1.0,964982703", "1,3,1.0,964981247")

      val expected = List("1,3", "3,1")

      val inputRDD = sc.parallelize(input)

      val expectedRDD = sc.parallelize(expected)

      val moviesSimilaritiesTable = MoviesRecommendation.moviesSimilaritiesTable(inputRDD)

      val resultRDD = topNMoviesRecommendation(moviesSimilaritiesTable, 1, 0.9, 0)

      assert(None === compareRDDWithOrder(resultRDD, expectedRDD))
    }

    test("test 3 with " + (if (x == 0) "original" else ("mutant " + x.toString()))) {
      val input = List("2,3,1.0,964981247", "2,1,1.0,964982703", "1,3,1.0,964981247", "1,1,1.0,964982703")

      val expected = List(((1, 3), (0.9999999999999998, 2)))

      val inputRDD = sc.parallelize(input)

      val expectedRDD = sc.parallelize(expected)

      val resultRDD = moviesSimilaritiesTable(inputRDD)

      val result = resultRDD.collect()

      assert(None === compareRDDWithOrder(resultRDD, expectedRDD))
    }

    test("test 4 with " + (if (x == 0) "original" else ("mutant " + x.toString()))) {
      val input = List("1,1,1.0,964982703", "1,3,1.0,964981247", "1,6,1.0,964982224", "2,1,0.0,964982703", "2,3,5.0,964981247", "2,6,5.0,964982224")

      val expected = List("3,6", "6,3")

      val inputRDD = sc.parallelize(input)

      val expectedRDD = sc.parallelize(expected)

      val moviesSimilaritiesTable = MoviesRecommendation.moviesSimilaritiesTable(inputRDD)

      val resultRDD = topNMoviesRecommendation(moviesSimilaritiesTable, 1, 0.9, 0)

      assert(None === compareRDDWithOrder(resultRDD, expectedRDD))
    }

    test("test 5 with " + (if (x == 0) "original" else ("mutant " + x.toString()))) {

      val input = List("2,6,5.0,964982224", "2,3,5.0,964981247", "2,1,0.0,964982703", "1,1,1.0,964982703", "1,3,1.0,964981247", "1,6,1.0,964982224")

      val expected = List(((1, 3), (0.19611613513818404, 2)), ((1, 6), (0.19611613513818404, 2)), ((3, 6), (1.0000000000000002, 2)))

      val inputRDD = sc.parallelize(input)

      val expectedRDD = sc.parallelize(expected)

      val resultRDD = moviesSimilaritiesTable(inputRDD)

      val result = resultRDD.collect()

      assert(None === compareRDDWithOrder(resultRDD, expectedRDD))
    }

    x = x + 1
  }

}