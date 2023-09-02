package com.fares.AWSGlue

import com.amazonaws.services.glue.GlueContext
import org.apache.spark.SparkContext
import com.amazonaws.services.glue.util.Job
import com.amazonaws.services.glue.util.GlueArgParser
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.{col, lit, lower, udf, when}

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
    val getTeamFiguresCountUdf: UserDefinedFunction = udf(
      (cards: String, partnerCards: String) => getTeamFiguresCount(cards, partnerCards))
    val getSuitsWithOverOneFigureUdf = udf((cards: String) => getSuitsWithOverOneFigure(cards))
    val getOverTwoAcesCount = udf((cards: String) => if (hasOverTwoAces(cards)) 1 else 0)
    val getSuitsWithOverFiveCardsCountUdf: UserDefinedFunction = udf(
      (cards: String) => getSuitsWithOverFiveCardsCount(cards))

    // Read JSON data from S3 and load it into a DataFrame
    var jsonDataFrame = sparkSession.read
      .json("s3://tarnib-analytics-bucket/raw_json_data/tarnib-analytics.json")
      .filter(col("team_1_score") + col("team_2_score") === 13) // Only keep rounds that were played entirely
      .withColumn("player_1_team_figures_count", getTeamFiguresCountUdf(col("player_1_cards"), col("player_3_cards")))
      .withColumn("player_2_team_figures_count", getTeamFiguresCountUdf(col("player_2_cards"), col("player_4_cards")))
      .withColumn("player_3_team_figures_count", getTeamFiguresCountUdf(col("player_3_cards"), col("player_1_cards")))
      .withColumn("player_4_team_figures_count", getTeamFiguresCountUdf(col("player_4_cards"), col("player_2_cards")))
      .withColumn(
        "rayan_figures_count",
        when(lower(col("player_1")) === "mugiwara", getFiguresCountUdf(col("player_1_cards")))
          .when(lower(col("player_2")) === "mugiwara", getFiguresCountUdf(col("player_2_cards")))
          .when(lower(col("player_3")) === "mugiwara", getFiguresCountUdf(col("player_3_cards")))
          .when(lower(col("player_4")) === "mugiwara", getFiguresCountUdf(col("player_4_cards")))
      )
      .withColumn(
        "fares_figures_count",
        when(lower(col("player_1")) === "blegess", getFiguresCountUdf(col("player_1_cards")))
          .when(lower(col("player_2")) === "blegess", getFiguresCountUdf(col("player_2_cards")))
          .when(lower(col("player_3")) === "blegess", getFiguresCountUdf(col("player_3_cards")))
          .when(lower(col("player_4")) === "blegess", getFiguresCountUdf(col("player_4_cards")))
      )
      .withColumn(
        "jack_figures_count",
        when(lower(col("player_1")) === "wave master", getFiguresCountUdf(col("player_1_cards")))
          .when(lower(col("player_2")) === "wave master", getFiguresCountUdf(col("player_2_cards")))
          .when(lower(col("player_3")) === "wave master", getFiguresCountUdf(col("player_3_cards")))
          .when(lower(col("player_4")) === "wave master", getFiguresCountUdf(col("player_4_cards")))
      )
      .withColumn(
        "jad_figures_count",
        when(lower(col("player_1")) === "jemba", getFiguresCountUdf(col("player_1_cards")))
          .when(lower(col("player_2")) === "jemba", getFiguresCountUdf(col("player_2_cards")))
          .when(lower(col("player_3")) === "jemba", getFiguresCountUdf(col("player_3_cards")))
          .when(lower(col("player_4")) === "jemba", getFiguresCountUdf(col("player_4_cards")))
      )
      .withColumn(
        "rayan_team_figures_count",
        when(lower(col("player_1")) === "mugiwara", col("player_1_team_figures_count"))
          .when(lower(col("player_2")) === "mugiwara", col("player_2_team_figures_count"))
          .when(lower(col("player_3")) === "mugiwara", col("player_3_team_figures_count"))
          .when(lower(col("player_4")) === "mugiwara", col("player_4_team_figures_count"))
      )
      .withColumn(
        "fares_team_figures_count",
        when(lower(col("player_1")) === "blegess", col("player_1_team_figures_count"))
          .when(lower(col("player_2")) === "blegess", col("player_2_team_figures_count"))
          .when(lower(col("player_3")) === "blegess", col("player_3_team_figures_count"))
          .when(lower(col("player_4")) === "blegess", col("player_4_team_figures_count"))
      )
      .withColumn(
        "jack_team_figures_count",
        when(lower(col("player_1")) === "wave master", col("player_1_team_figures_count"))
          .when(lower(col("player_2")) === "wave master", col("player_2_team_figures_count"))
          .when(lower(col("player_3")) === "wave master", col("player_3_team_figures_count"))
          .when(lower(col("player_4")) === "wave master", col("player_4_team_figures_count"))
      )
      .withColumn(
        "jad_team_figures_count",
        when(lower(col("player_1")) === "jemba", col("player_1_team_figures_count"))
          .when(lower(col("player_2")) === "jemba", col("player_2_team_figures_count"))
          .when(lower(col("player_3")) === "jemba", col("player_3_team_figures_count"))
          .when(lower(col("player_4")) === "jemba", col("player_4_team_figures_count"))
      )
      .withColumn(
        "rayan_over_two_aces_count",
        when(lower(col("player_1")) === "mugiwara", getOverTwoAcesCount(col("player_1_cards")))
          .when(lower(col("player_2")) === "mugiwara", getOverTwoAcesCount(col("player_2_cards")))
          .when(lower(col("player_3")) === "mugiwara", getOverTwoAcesCount(col("player_3_cards")))
          .when(lower(col("player_4")) === "mugiwara", getOverTwoAcesCount(col("player_4_cards")))
      )
      .withColumn(
        "fares_over_two_aces_count",
        when(lower(col("player_1")) === "blegess", getOverTwoAcesCount(col("player_1_cards")))
          .when(lower(col("player_2")) === "blegess", getOverTwoAcesCount(col("player_2_cards")))
          .when(lower(col("player_3")) === "blegess", getOverTwoAcesCount(col("player_3_cards")))
          .when(lower(col("player_4")) === "blegess", getOverTwoAcesCount(col("player_4_cards")))
      )
      .withColumn(
        "jack_over_two_aces_count",
        when(lower(col("player_1")) === "wave master", getOverTwoAcesCount(col("player_1_cards")))
          .when(lower(col("player_2")) === "wave master", getOverTwoAcesCount(col("player_2_cards")))
          .when(lower(col("player_3")) === "wave master", getOverTwoAcesCount(col("player_3_cards")))
          .when(lower(col("player_4")) === "wave master", getOverTwoAcesCount(col("player_4_cards")))
      )
      .withColumn(
        "jad_over_two_aces_count",
        when(lower(col("player_1")) === "jemba", getOverTwoAcesCount(col("player_1_cards")))
          .when(lower(col("player_2")) === "jemba", getOverTwoAcesCount(col("player_2_cards")))
          .when(lower(col("player_3")) === "jemba", getOverTwoAcesCount(col("player_3_cards")))
          .when(lower(col("player_4")) === "jemba", getOverTwoAcesCount(col("player_4_cards")))
      )
      .withColumn(
        "rayan_suits_over_five_cards",
        when(lower(col("player_1")) === "mugiwara", getSuitsWithOverFiveCardsCountUdf(col("player_1_cards")))
          .when(lower(col("player_2")) === "mugiwara", getSuitsWithOverFiveCardsCountUdf(col("player_2_cards")))
          .when(lower(col("player_3")) === "mugiwara", getSuitsWithOverFiveCardsCountUdf(col("player_3_cards")))
          .when(lower(col("player_4")) === "mugiwara", getSuitsWithOverFiveCardsCountUdf(col("player_4_cards")))
      )
      .withColumn(
        "fares_suits_over_five_cards",
        when(lower(col("player_1")) === "blegess", getSuitsWithOverFiveCardsCountUdf(col("player_1_cards")))
          .when(lower(col("player_2")) === "blegess", getSuitsWithOverFiveCardsCountUdf(col("player_2_cards")))
          .when(lower(col("player_3")) === "blegess", getSuitsWithOverFiveCardsCountUdf(col("player_3_cards")))
          .when(lower(col("player_4")) === "blegess", getSuitsWithOverFiveCardsCountUdf(col("player_4_cards")))
      )
      .withColumn(
        "jack_suits_over_five_cards",
        when(lower(col("player_1")) === "wave master", getSuitsWithOverFiveCardsCountUdf(col("player_1_cards")))
          .when(lower(col("player_2")) === "wave master", getSuitsWithOverFiveCardsCountUdf(col("player_2_cards")))
          .when(lower(col("player_3")) === "wave master", getSuitsWithOverFiveCardsCountUdf(col("player_3_cards")))
          .when(lower(col("player_4")) === "wave master", getSuitsWithOverFiveCardsCountUdf(col("player_4_cards")))
      )
      .withColumn(
        "jad_suits_over_five_cards",
        when(lower(col("player_1")) === "jemba", getSuitsWithOverFiveCardsCountUdf(col("player_1_cards")))
          .when(lower(col("player_2")) === "jemba", getSuitsWithOverFiveCardsCountUdf(col("player_2_cards")))
          .when(lower(col("player_3")) === "jemba", getSuitsWithOverFiveCardsCountUdf(col("player_3_cards")))
          .when(lower(col("player_4")) === "jemba", getSuitsWithOverFiveCardsCountUdf(col("player_4_cards")))
      )
      .withColumn(
        "rayan_suits_with_over_one_figures",
        when(lower(col("player_1")) === "mugiwara", getSuitsWithOverOneFigureUdf(col("player_1_cards")))
          .when(lower(col("player_2")) === "mugiwara", getSuitsWithOverOneFigureUdf(col("player_2_cards")))
          .when(lower(col("player_3")) === "mugiwara", getSuitsWithOverOneFigureUdf(col("player_3_cards")))
          .when(lower(col("player_4")) === "mugiwara", getSuitsWithOverOneFigureUdf(col("player_4_cards")))
      )
      .withColumn(
        "fares_suits_with_over_one_figures",
        when(lower(col("player_1")) === "blegess", getSuitsWithOverOneFigureUdf(col("player_1_cards")))
          .when(lower(col("player_2")) === "blegess", getSuitsWithOverOneFigureUdf(col("player_2_cards")))
          .when(lower(col("player_3")) === "blegess", getSuitsWithOverOneFigureUdf(col("player_3_cards")))
          .when(lower(col("player_4")) === "blegess", getSuitsWithOverOneFigureUdf(col("player_4_cards")))
      )
      .withColumn(
        "jack_suits_with_over_one_figures",
        when(lower(col("player_1")) === "wave master", getSuitsWithOverOneFigureUdf(col("player_1_cards")))
          .when(lower(col("player_2")) === "wave master", getSuitsWithOverOneFigureUdf(col("player_2_cards")))
          .when(lower(col("player_3")) === "wave master", getSuitsWithOverOneFigureUdf(col("player_3_cards")))
          .when(lower(col("player_4")) === "wave master", getSuitsWithOverOneFigureUdf(col("player_4_cards")))
      )
      .withColumn(
        "jad_suits_with_over_one_figures",
        when(lower(col("player_1")) === "jemba", getSuitsWithOverOneFigureUdf(col("player_1_cards")))
          .when(lower(col("player_2")) === "jemba", getSuitsWithOverOneFigureUdf(col("player_2_cards")))
          .when(lower(col("player_3")) === "jemba", getSuitsWithOverOneFigureUdf(col("player_3_cards")))
          .when(lower(col("player_4")) === "jemba", getSuitsWithOverOneFigureUdf(col("player_4_cards")))
      )

    // Apply aggregation functions to the DataFrame's columns
    val aggregatedDF = jsonDataFrame
      .select(jsonDataFrame.columns.map(colName => functions.sum(col(colName)).alias(colName)): _*)
      .withColumn("total_rounds_played", lit(jsonDataFrame.count()))
      .select(
        "total_rounds_played",
        "rayan_figures_count",
        "fares_figures_count",
        "jack_figures_count",
        "jad_figures_count",
        "rayan_team_figures_count",
        "fares_team_figures_count",
        "jad_team_figures_count",
        "jack_team_figures_count",
        "rayan_over_two_aces_count",
        "fares_over_two_aces_count",
        "jack_over_two_aces_count",
        "jad_over_two_aces_count",
        "rayan_suits_over_five_cards",
        "fares_suits_over_five_cards",
        "jack_suits_over_five_cards",
        "jad_suits_over_five_cards",
        "rayan_suits_with_over_one_figures",
        "fares_suits_with_over_one_figures",
        "jack_suits_with_over_one_figures",
        "jad_suits_with_over_one_figures",
      )

    aggregatedDF.show(10)
    aggregatedDF.printSchema()

    // Print each column in a separate line
//    aggregatedDF.columns.foreach(columnName => {
//      aggregatedDF.select(columnName).show()
//      println()
//    })

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

  /**
    * Get figure count of the team the player is in
    * @param cards
    * @param partnerCards
    */
  def getTeamFiguresCount(cards: String, partnerCards: String): Int = {
    getFiguresCount(cards) + getFiguresCount(partnerCards)
  }

  /**
    * Returns true if player had over 2 aces, false otherwise
    * @param cards
    */
  def hasOverTwoAces(cards: String): Boolean = {
    "14".r.findAllIn(cards).length > 2
  }

}
