package com.fares.AWSGlue

import com.amazonaws.services.glue.util.JsonOptions
import com.amazonaws.services.glue.{DataSink, DataSource, DynamicFrame, GlueContext}
import org.apache.spark.{SparkConf, SparkContext}
import com.amazonaws.services.glue.util.Job
import com.amazonaws.services.glue.util.GlueArgParser
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.{col, udf}

import scala.collection.JavaConverters._

/**
  * An example that includes all necessary boilerplate code for a deployable   *
  * AWS Glue job script in Scala that can also be tested locally with          *
  * scalatest and sbt. */
object GlueApp {

  def main(sysArgs: Array[String]): Unit = {

    // Load the job arguments. "JOB_NAME" is a special argument that gets autofilled
    val args = GlueArgParser.getResolvedOptions(sysArgs, Seq("JOB_NAME", "stage").toArray)

    println("Initializing Spark and GlueContext")
    val sparkSession: SparkSession =
      if (args("stage") == "dev") { // For testing, we need to use local execution. You need Java JDK 8 for this to work!
        SparkSession.builder().master("local[*]").getOrCreate()
      } else {
        SparkSession.builder().getOrCreate()
      }
    val sparkContext: SparkContext = sparkSession.sparkContext
    sparkContext.setLogLevel("FATAL") // Can be changed to INFO, ERROR, or WARN
    val glueContext: GlueContext = new GlueContext(sparkContext)

    // Check the custom argument 'stage' to ensure if the execution is local or not
    if (args("stage") == "prod" || args("stage") == "staging") {
      Job.init(if (args("JOB_NAME") != null) args("JOB_NAME") else "test", glueContext, args.asJava)
    }

    val getFiguresCountUdf: UserDefinedFunction = udf((cards: String) => getFiguresCount(cards))
    val getSuitsWithOverOneFigureUdf = udf((cards: String) => getSuitsWithOverOneFigure(cards))
    val getSuitsWithOverFiveCardsCountUdf: UserDefinedFunction = udf(
      (cards: String) => getSuitsWithOverFiveCardsCount(cards))

    // Read JSON data from S3 and load it into a DataFrame
    var jsonDataFrame = sparkSession.read
      .json("s3://tarnib-analytics-bucket/raw_json_data/tarnib-analytics.json")
      .where(col("team_1_score") + col("team_2_score") === 13) // Only keep rounds that were played entirely
      .withColumn("player_1_figures_count", getFiguresCountUdf(col("player_1_cards")))
      .withColumn("player_2_figures_count", getFiguresCountUdf(col("player_2_cards")))
      .withColumn("player_3_figures_count", getFiguresCountUdf(col("player_3_cards")))
      .withColumn("player_4_figures_count", getFiguresCountUdf(col("player_4_cards")))
      .withColumn("player_1_suits_over_five_cards", getSuitsWithOverFiveCardsCountUdf(col("player_1_cards")))
      .withColumn("player_2_suits_over_five_cards", getSuitsWithOverFiveCardsCountUdf(col("player_2_cards")))
      .withColumn("player_3_suits_over_five_cards", getSuitsWithOverFiveCardsCountUdf(col("player_3_cards")))
      .withColumn("player_4_suits_over_five_cards", getSuitsWithOverFiveCardsCountUdf(col("player_4_cards")))
      .withColumn("player_1_suits_with_over_one_figures", getSuitsWithOverOneFigureUdf(col("player_1_cards")))
      .withColumn("player_2_suits_with_over_one_figures", getSuitsWithOverOneFigureUdf(col("player_2_cards")))
      .withColumn("player_3_suits_with_over_one_figures", getSuitsWithOverOneFigureUdf(col("player_3_cards")))
      .withColumn("player_4_suits_with_over_one_figures", getSuitsWithOverOneFigureUdf(col("player_4_cards")))

    jsonDataFrame.show(10)

    println("Your code goes here!")

    // Job actions should only happen when executed by AWS Glue, so we ensure correct stage
    if (args("stage") == "prod" || args("stage") == "staging") {
      Job.commit()
    }

  }

  /**
    * Count occurrence of figures (Ace included) in a players cards
    * @param cards - cards array string
    */
  def getFiguresCount(cards: String): Int = {
    val occurrences = Seq("14", "13", "12", "11")
    occurrences.map(card => card.r.findAllIn(cards).length).sum
  }

  /**
    * Count occurrence of suits with more than 5 cards
    * Ex: if you had 6 spades, 6 hearts, 1 diamond, the function will return 2
    * @param cards - cards array string
    */
  def getSuitsWithOverFiveCardsCount(cards: String): Int = {
    val occurrences = Seq("of_spades", "of_hearts", "of_clubs", "of_diamonds")
    occurrences.map(card => if (card.r.findAllIn(cards).length > 5) 1 else 0).sum
  }

  /**
    * Count occurrence of suits with over a figure (Ace included) in a players cards
    * @param cards - cards array string
    */
  def getSuitsWithOverOneFigure(cards: String): Int = {
    val heartsFigures = Seq("14_of_hearts", "13_of_hearts", "12_of_hearts", "11_of_hearts")
    val spadeFigures = Seq("14_of_spades", "13_of_spades", "12_of_spades", "11_of_spades")
    val clubsFigures = Seq("14_of_clubs", "13_of_clubs", "12_of_clubs", "11_of_clubs")
    val diamondsFigures = Seq("14_of_diamonds", "13_of_diamonds", "12_of_diamonds", "11_of_diamonds")
    val figures = Seq(heartsFigures, diamondsFigures, clubsFigures, spadeFigures)

    figures
      .map(suit => {
        val figuresCountInSuit = suit.map(figure => figure.r.findAllIn(cards).length).sum
        if (figuresCountInSuit > 1) 1 else 0
      })
      .sum
  }

}
