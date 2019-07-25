package br.ufrn.dimap.forall.spark.movielens.recomendation

import org.scalatest.FunSuite

import com.holdenkarau.spark.testing.RDDComparisons
import com.holdenkarau.spark.testing.SharedSparkContext

class ExampleTest extends FunSuite with SharedSparkContext with RDDComparisons {
  
//  test("test 1") {
//      val input = List("1,1,1.0,964982703", "1,3,1.0,964981247")
////          ,
////        "1,3,3.0,964981247",
////        "1,6,3.0,964982224",
////        "2,1,4.0,1445714835",
////        "2,3,4.0,1445715029",
////        "2,6,4.0,1445715228",
////        "3,1,5.0,1306463578",
////        "3,3,5.0,1306464275",
////        "3,6,5.0,1306463619")
//
//      val expected = List(((1, 3), (0.9999999999999999, 3)), ((1, 6), (0.9999999999999999, 3)), ((3, 6), (0.9999999999999999, 3)))
//
//      val inputRDD = sc.parallelize(input)
//
//      val expectedRDD = sc.parallelize(expected)
//
//      val resultRDD = MoviesRecommendation.moviesSimilaritiesTable(inputRDD)
//
//      val result = resultRDD.collect()
//      
//      println("Size: " + result.size)
//      
//      result.foreach(println)
//
//      assert(None === compareRDDWithOrder(resultRDD, expectedRDD))
//    }
//  
//  test("test 2 - Recomendations") {
//      val input = List("1,1,1.0,964982703", "1,3,1.0,964981247")
//
//      val expected = List("1,3")
//
//      val inputRDD = sc.parallelize(input)
//
//      val expectedRDD = sc.parallelize(expected)
//
//      val moviesSimilaritiesTable = MoviesRecommendation.moviesSimilaritiesTable(inputRDD)
//
//      val resultRDD = MoviesRecommendation.topNMoviesRecommendation(moviesSimilaritiesTable, 1, 0.9, 0)
//      
//      val result = resultRDD.collect()
//      
//      println("Size: " + result.size)
//      
//      result.foreach(println)
//
//      assert(None === compareRDDWithOrder(resultRDD, expectedRDD))
//    }
  
  test("test 3") {
      val input = List("2,1,0.0,964982703", "2,3,5.0,964981247", "2,6,5.0,964982224", "1,1,1.0,964982703", "1,3,1.0,964981247", "1,6,1.0,964982224")
//          ,
//        "1,3,3.0,964981247",
//        "1,6,3.0,964982224",
//        "2,1,4.0,1445714835",
//        "2,3,4.0,1445715029",
//        "2,6,4.0,1445715228",
//        "3,1,5.0,1306463578",
//        "3,3,5.0,1306464275",
//        "3,6,5.0,1306463619")

      val expected = List(((1, 3), (0.19611613513818404,2)), ((1, 6), (0.19611613513818404,2)), ((3, 6), (1.0000000000000002,2)))

      val inputRDD = sc.parallelize(input)

      val expectedRDD = sc.parallelize(expected)

      val resultRDD = MoviesRecommendation.moviesSimilaritiesTable(inputRDD)

      val result = resultRDD.collect()
      
      println("Size: " + result.size)
      
      result.foreach(println)

      assert(None === compareRDDWithOrder(resultRDD, expectedRDD))
    }
  
}