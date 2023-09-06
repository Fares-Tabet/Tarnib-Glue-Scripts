package com.fares.AWSGlue

import com.amazonaws.services.glue.GlueContext
import org.apache.spark.SparkContext
import com.amazonaws.services.glue.util.Job
import com.amazonaws.services.glue.util.GlueArgParser
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{SaveMode, SparkSession, functions}
import org.apache.spark.sql.functions.{array_contains, col, lit, lower, udf, when}

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
        SparkSession.builder().master("local[*]").config("spark.driver.bindAddress", "127.0.0.1").getOrCreate()
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

    // Create User Defined functions to be used when adding new columns to the dataframe
    val getFiguresCountUdf: UserDefinedFunction = udf((cards: String) => getFiguresCount(cards))
    val getTeamFiguresCountUdf: UserDefinedFunction = udf(
      (cards: String, partnerCards: String) => getTeamFiguresCount(cards, partnerCards))
    val getSuitsWithOverOneFigureUdf = udf((cards: String) => getSuitsWithOverOneFigure(cards))
    val getOverTwoAcesCount = udf((cards: String) => if (hasOverTwoAces(cards)) 1 else 0)
    val getLessThanTwoFiguresCount = udf((cards: String) => if (hadLessThanTwoFigures(cards)) 1 else 0)
    val getTarnibFiguresCountUdf = udf((cards: String, tarnib: String) => getTarnibFiguresCount(cards, tarnib))
    val hasOverThreePartnersTarnibCount = udf((cards: String, tarnib: String, bidsData: String, player_pair: String) =>
      if (hasOverThreePartnersTarnib(cards, tarnib, bidsData, player_pair)) 1 else 0)
    val hasOverThreeEnemyTarnibCount = udf(
      (cards: String, tarnib: String, bidsData: String, enemy_player_pair: String) =>
        if (hasOverThreeEnemyTarnib(cards, tarnib, bidsData, enemy_player_pair)) 1 else 0)
    val hasPartnerPickedTarnibCount = udf(
      (bidsData: String, player_pair: String) => if (hasPartnerPickedTarnib(bidsData, player_pair)) 1 else 0)
    val hasEnemyPickedTarnibCount = udf(
      (bidsData: String, enemy_player_pair: String) => if (hasEnemyPickedTarnib(bidsData, enemy_player_pair)) 1 else 0)
    val getFigureScoreUdf = udf((cards: String) => getFiguresScore(cards))
    val getSuitsWithOverFiveCardsCountUdf: UserDefinedFunction = udf(
      (cards: String) => getSuitsWithOverFiveCardsCount(cards))

    // All names used by each player
    val rayanNames = Array("mugiwara")
    val faresNames = Array("blegess")
    val jackNames = Array("wave master")
    val jadNames = Array("jemba")
    val valid_players = rayanNames ++ faresNames ++ jackNames ++ jadNames

    // Read JSON data from S3 and load it into a DataFrame
    var jsonDataFrame = sparkSession.read
      .json("s3://tarnib-analytics-bucket/raw_json_data/tarnib-analytics.json")
      // Only keep rounds that were played entirely
      .filter(col("team_1_score") + col("team_2_score") === 13)
      .filter(array_contains(lit(valid_players), lower(col("player_1"))) && array_contains(
        lit(valid_players),
        lower(col("player_2"))) && array_contains(lit(valid_players), lower(col("player_3"))) && array_contains(
        lit(valid_players),
        lower(col("player_4"))))
      .withColumn("player_1_team_figures_count", getTeamFiguresCountUdf(col("player_1_cards"), col("player_3_cards")))
      .withColumn("player_2_team_figures_count", getTeamFiguresCountUdf(col("player_2_cards"), col("player_4_cards")))
      .withColumn("player_3_team_figures_count", getTeamFiguresCountUdf(col("player_3_cards"), col("player_1_cards")))
      .withColumn("player_4_team_figures_count", getTeamFiguresCountUdf(col("player_4_cards"), col("player_2_cards")))
      .withColumn("player_1_has_over_3_partner_tarnib_count",
                  hasOverThreePartnersTarnibCount(col("player_1_cards"), col("tarnib"), col("bids_data"), lit("1_3")))
      .withColumn("player_2_has_over_3_partner_tarnib_count",
                  hasOverThreePartnersTarnibCount(col("player_2_cards"), col("tarnib"), col("bids_data"), lit("2_4")))
      .withColumn("player_3_has_over_3_partner_tarnib_count",
                  hasOverThreePartnersTarnibCount(col("player_3_cards"), col("tarnib"), col("bids_data"), lit("3_1")))
      .withColumn("player_4_has_over_3_partner_tarnib_count",
                  hasOverThreePartnersTarnibCount(col("player_4_cards"), col("tarnib"), col("bids_data"), lit("4_2")))
      .withColumn("player_1_partner_picked_tarnib_count", hasPartnerPickedTarnibCount(col("bids_data"), lit("1_3")))
      .withColumn("player_2_partner_picked_tarnib_count", hasPartnerPickedTarnibCount(col("bids_data"), lit("2_4")))
      .withColumn("player_3_partner_picked_tarnib_count", hasPartnerPickedTarnibCount(col("bids_data"), lit("3_1")))
      .withColumn("player_4_partner_picked_tarnib_count", hasPartnerPickedTarnibCount(col("bids_data"), lit("4_2")))
      .withColumn("player_1_has_over_3_enemy_tarnib_count",
                  hasOverThreeEnemyTarnibCount(col("player_1_cards"), col("tarnib"), col("bids_data"), lit("2_4")))
      .withColumn("player_2_has_over_3_enemy_tarnib_count",
                  hasOverThreeEnemyTarnibCount(col("player_2_cards"), col("tarnib"), col("bids_data"), lit("1_3")))
      .withColumn("player_3_has_over_3_enemy_tarnib_count",
                  hasOverThreeEnemyTarnibCount(col("player_3_cards"), col("tarnib"), col("bids_data"), lit("2_4")))
      .withColumn("player_4_has_over_3_enemy_tarnib_count",
                  hasOverThreeEnemyTarnibCount(col("player_4_cards"), col("tarnib"), col("bids_data"), lit("1_3")))
      .withColumn("player_1_enemy_picked_tarnib_count", hasEnemyPickedTarnibCount(col("bids_data"), lit("2_4")))
      .withColumn("player_2_enemy_picked_tarnib_count", hasEnemyPickedTarnibCount(col("bids_data"), lit("1_3")))
      .withColumn("player_3_enemy_picked_tarnib_count", hasEnemyPickedTarnibCount(col("bids_data"), lit("2_4")))
      .withColumn("player_4_enemy_picked_tarnib_count", hasEnemyPickedTarnibCount(col("bids_data"), lit("1_3")))
      .withColumn(
        "rayan_figures_count",
        when(array_contains(lit(rayanNames), lower(col("player_1"))), getFiguresCountUdf(col("player_1_cards")))
          .when(array_contains(lit(rayanNames), lower(col("player_2"))), getFiguresCountUdf(col("player_2_cards")))
          .when(array_contains(lit(rayanNames), lower(col("player_3"))), getFiguresCountUdf(col("player_3_cards")))
          .when(array_contains(lit(rayanNames), lower(col("player_4"))), getFiguresCountUdf(col("player_4_cards")))
      )
      .withColumn(
        "fares_figures_count",
        when(array_contains(lit(faresNames), lower(col("player_1"))), getFiguresCountUdf(col("player_1_cards")))
          .when(array_contains(lit(faresNames), lower(col("player_2"))), getFiguresCountUdf(col("player_2_cards")))
          .when(array_contains(lit(faresNames), lower(col("player_3"))), getFiguresCountUdf(col("player_3_cards")))
          .when(array_contains(lit(faresNames), lower(col("player_4"))), getFiguresCountUdf(col("player_4_cards")))
      )
      .withColumn(
        "jack_figures_count",
        when(array_contains(lit(jackNames), lower(col("player_1"))), getFiguresCountUdf(col("player_1_cards")))
          .when(array_contains(lit(jackNames), lower(col("player_2"))), getFiguresCountUdf(col("player_2_cards")))
          .when(array_contains(lit(jackNames), lower(col("player_3"))), getFiguresCountUdf(col("player_3_cards")))
          .when(array_contains(lit(jackNames), lower(col("player_4"))), getFiguresCountUdf(col("player_4_cards")))
      )
      .withColumn(
        "jad_figures_count",
        when(array_contains(lit(jadNames), lower(col("player_1"))), getFiguresCountUdf(col("player_1_cards")))
          .when(array_contains(lit(jadNames), lower(col("player_2"))), getFiguresCountUdf(col("player_2_cards")))
          .when(array_contains(lit(jadNames), lower(col("player_3"))), getFiguresCountUdf(col("player_3_cards")))
          .when(array_contains(lit(jadNames), lower(col("player_4"))), getFiguresCountUdf(col("player_4_cards")))
      )
      .withColumn(
        "rayan_figures_score",
        when(array_contains(lit(rayanNames), lower(col("player_1"))), getFigureScoreUdf(col("player_1_cards")))
          .when(array_contains(lit(rayanNames), lower(col("player_2"))), getFigureScoreUdf(col("player_2_cards")))
          .when(array_contains(lit(rayanNames), lower(col("player_3"))), getFigureScoreUdf(col("player_3_cards")))
          .when(array_contains(lit(rayanNames), lower(col("player_4"))), getFigureScoreUdf(col("player_4_cards")))
      )
      .withColumn(
        "fares_figures_score",
        when(array_contains(lit(faresNames), lower(col("player_1"))), getFigureScoreUdf(col("player_1_cards")))
          .when(array_contains(lit(faresNames), lower(col("player_2"))), getFigureScoreUdf(col("player_2_cards")))
          .when(array_contains(lit(faresNames), lower(col("player_3"))), getFigureScoreUdf(col("player_3_cards")))
          .when(array_contains(lit(faresNames), lower(col("player_4"))), getFigureScoreUdf(col("player_4_cards")))
      )
      .withColumn(
        "jack_figures_score",
        when(array_contains(lit(jackNames), lower(col("player_1"))), getFigureScoreUdf(col("player_1_cards")))
          .when(array_contains(lit(jackNames), lower(col("player_2"))), getFigureScoreUdf(col("player_2_cards")))
          .when(array_contains(lit(jackNames), lower(col("player_3"))), getFigureScoreUdf(col("player_3_cards")))
          .when(array_contains(lit(jackNames), lower(col("player_4"))), getFigureScoreUdf(col("player_4_cards")))
      )
      .withColumn(
        "jad_figures_score",
        when(array_contains(lit(jadNames), lower(col("player_1"))), getFigureScoreUdf(col("player_1_cards")))
          .when(array_contains(lit(jadNames), lower(col("player_2"))), getFigureScoreUdf(col("player_2_cards")))
          .when(array_contains(lit(jadNames), lower(col("player_3"))), getFigureScoreUdf(col("player_3_cards")))
          .when(array_contains(lit(jadNames), lower(col("player_4"))), getFigureScoreUdf(col("player_4_cards")))
      )
      .withColumn(
        "rayan_tarnib_figures_count",
        when(array_contains(lit(rayanNames), lower(col("player_1"))),
             getTarnibFiguresCountUdf(col("player_1_cards"), col("tarnib")))
          .when(array_contains(lit(rayanNames), lower(col("player_2"))),
                getTarnibFiguresCountUdf(col("player_2_cards"), col("tarnib")))
          .when(array_contains(lit(rayanNames), lower(col("player_3"))),
                getTarnibFiguresCountUdf(col("player_3_cards"), col("tarnib")))
          .when(array_contains(lit(rayanNames), lower(col("player_4"))),
                getTarnibFiguresCountUdf(col("player_4_cards"), col("tarnib")))
      )
      .withColumn(
        "fares_tarnib_figures_count",
        when(array_contains(lit(faresNames), lower(col("player_1"))),
             getTarnibFiguresCountUdf(col("player_1_cards"), col("tarnib")))
          .when(array_contains(lit(faresNames), lower(col("player_2"))),
                getTarnibFiguresCountUdf(col("player_2_cards"), col("tarnib")))
          .when(array_contains(lit(faresNames), lower(col("player_3"))),
                getTarnibFiguresCountUdf(col("player_3_cards"), col("tarnib")))
          .when(array_contains(lit(faresNames), lower(col("player_4"))),
                getTarnibFiguresCountUdf(col("player_4_cards"), col("tarnib")))
      )
      .withColumn(
        "jack_tarnib_figures_count",
        when(array_contains(lit(jackNames), lower(col("player_1"))),
             getTarnibFiguresCountUdf(col("player_1_cards"), col("tarnib")))
          .when(array_contains(lit(jackNames), lower(col("player_2"))),
                getTarnibFiguresCountUdf(col("player_2_cards"), col("tarnib")))
          .when(array_contains(lit(jackNames), lower(col("player_3"))),
                getTarnibFiguresCountUdf(col("player_3_cards"), col("tarnib")))
          .when(array_contains(lit(jackNames), lower(col("player_4"))),
                getTarnibFiguresCountUdf(col("player_4_cards"), col("tarnib")))
      )
      .withColumn(
        "jad_tarnib_figures_count",
        when(array_contains(lit(jadNames), lower(col("player_1"))),
             getTarnibFiguresCountUdf(col("player_1_cards"), col("tarnib")))
          .when(array_contains(lit(jadNames), lower(col("player_2"))),
                getTarnibFiguresCountUdf(col("player_2_cards"), col("tarnib")))
          .when(array_contains(lit(jadNames), lower(col("player_3"))),
                getTarnibFiguresCountUdf(col("player_3_cards"), col("tarnib")))
          .when(array_contains(lit(jadNames), lower(col("player_4"))),
                getTarnibFiguresCountUdf(col("player_4_cards"), col("tarnib")))
      )
      .withColumn(
        "rayan_team_figures_count",
        when(array_contains(lit(rayanNames), lower(col("player_1"))), col("player_1_team_figures_count"))
          .when(array_contains(lit(rayanNames), lower(col("player_2"))), col("player_2_team_figures_count"))
          .when(array_contains(lit(rayanNames), lower(col("player_3"))), col("player_3_team_figures_count"))
          .when(array_contains(lit(rayanNames), lower(col("player_4"))), col("player_4_team_figures_count"))
      )
      .withColumn(
        "fares_team_figures_count",
        when(array_contains(lit(faresNames), lower(col("player_1"))), col("player_1_team_figures_count"))
          .when(array_contains(lit(faresNames), lower(col("player_2"))), col("player_2_team_figures_count"))
          .when(array_contains(lit(faresNames), lower(col("player_3"))), col("player_3_team_figures_count"))
          .when(array_contains(lit(faresNames), lower(col("player_4"))), col("player_4_team_figures_count"))
      )
      .withColumn(
        "jack_team_figures_count",
        when(array_contains(lit(jackNames), lower(col("player_1"))), col("player_1_team_figures_count"))
          .when(array_contains(lit(jackNames), lower(col("player_2"))), col("player_2_team_figures_count"))
          .when(array_contains(lit(jackNames), lower(col("player_3"))), col("player_3_team_figures_count"))
          .when(array_contains(lit(jackNames), lower(col("player_4"))), col("player_4_team_figures_count"))
      )
      .withColumn(
        "jad_team_figures_count",
        when(array_contains(lit(jadNames), lower(col("player_1"))), col("player_1_team_figures_count"))
          .when(array_contains(lit(jadNames), lower(col("player_2"))), col("player_2_team_figures_count"))
          .when(array_contains(lit(jadNames), lower(col("player_3"))), col("player_3_team_figures_count"))
          .when(array_contains(lit(jadNames), lower(col("player_4"))), col("player_4_team_figures_count"))
      )
      .withColumn(
        "rayan_over_3_partner_tarnib_count",
        when(array_contains(lit(rayanNames), lower(col("player_1"))), col("player_1_has_over_3_partner_tarnib_count"))
          .when(array_contains(lit(rayanNames), lower(col("player_2"))),
                col("player_2_has_over_3_partner_tarnib_count"))
          .when(array_contains(lit(rayanNames), lower(col("player_3"))),
                col("player_3_has_over_3_partner_tarnib_count"))
          .when(array_contains(lit(rayanNames), lower(col("player_4"))),
                col("player_4_has_over_3_partner_tarnib_count"))
      )
      .withColumn(
        "fares_over_3_partner_tarnib_count",
        when(array_contains(lit(faresNames), lower(col("player_1"))), col("player_1_has_over_3_partner_tarnib_count"))
          .when(array_contains(lit(faresNames), lower(col("player_2"))),
                col("player_2_has_over_3_partner_tarnib_count"))
          .when(array_contains(lit(faresNames), lower(col("player_3"))),
                col("player_3_has_over_3_partner_tarnib_count"))
          .when(array_contains(lit(faresNames), lower(col("player_4"))),
                col("player_4_has_over_3_partner_tarnib_count"))
      )
      .withColumn(
        "jack_over_3_partner_tarnib_count",
        when(array_contains(lit(jackNames), lower(col("player_1"))), col("player_1_has_over_3_partner_tarnib_count"))
          .when(array_contains(lit(jackNames), lower(col("player_2"))), col("player_2_has_over_3_partner_tarnib_count"))
          .when(array_contains(lit(jackNames), lower(col("player_3"))), col("player_3_has_over_3_partner_tarnib_count"))
          .when(array_contains(lit(jackNames), lower(col("player_4"))), col("player_4_has_over_3_partner_tarnib_count"))
      )
      .withColumn(
        "jad_over_3_partner_tarnib_count",
        when(array_contains(lit(jadNames), lower(col("player_1"))), col("player_1_has_over_3_partner_tarnib_count"))
          .when(array_contains(lit(jadNames), lower(col("player_2"))), col("player_2_has_over_3_partner_tarnib_count"))
          .when(array_contains(lit(jadNames), lower(col("player_3"))), col("player_3_has_over_3_partner_tarnib_count"))
          .when(array_contains(lit(jadNames), lower(col("player_4"))), col("player_4_has_over_3_partner_tarnib_count"))
      )
      .withColumn(
        "rayan_over_two_aces_count",
        when(array_contains(lit(rayanNames), lower(col("player_1"))), getOverTwoAcesCount(col("player_1_cards")))
          .when(array_contains(lit(rayanNames), lower(col("player_2"))), getOverTwoAcesCount(col("player_2_cards")))
          .when(array_contains(lit(rayanNames), lower(col("player_3"))), getOverTwoAcesCount(col("player_3_cards")))
          .when(array_contains(lit(rayanNames), lower(col("player_4"))), getOverTwoAcesCount(col("player_4_cards")))
      )
      .withColumn(
        "fares_over_two_aces_count",
        when(array_contains(lit(faresNames), lower(col("player_1"))), getOverTwoAcesCount(col("player_1_cards")))
          .when(array_contains(lit(faresNames), lower(col("player_2"))), getOverTwoAcesCount(col("player_2_cards")))
          .when(array_contains(lit(faresNames), lower(col("player_3"))), getOverTwoAcesCount(col("player_3_cards")))
          .when(array_contains(lit(faresNames), lower(col("player_4"))), getOverTwoAcesCount(col("player_4_cards")))
      )
      .withColumn(
        "jack_over_two_aces_count",
        when(array_contains(lit(jackNames), lower(col("player_1"))), getOverTwoAcesCount(col("player_1_cards")))
          .when(array_contains(lit(jackNames), lower(col("player_2"))), getOverTwoAcesCount(col("player_2_cards")))
          .when(array_contains(lit(jackNames), lower(col("player_3"))), getOverTwoAcesCount(col("player_3_cards")))
          .when(array_contains(lit(jackNames), lower(col("player_4"))), getOverTwoAcesCount(col("player_4_cards")))
      )
      .withColumn(
        "jad_over_two_aces_count",
        when(array_contains(lit(jadNames), lower(col("player_1"))), getOverTwoAcesCount(col("player_1_cards")))
          .when(array_contains(lit(jadNames), lower(col("player_2"))), getOverTwoAcesCount(col("player_2_cards")))
          .when(array_contains(lit(jadNames), lower(col("player_3"))), getOverTwoAcesCount(col("player_3_cards")))
          .when(array_contains(lit(jadNames), lower(col("player_4"))), getOverTwoAcesCount(col("player_4_cards")))
      )
      .withColumn(
        "rayan_over_3_enemy_tarnib_count",
        when(array_contains(lit(rayanNames), lower(col("player_1"))), col("player_1_has_over_3_enemy_tarnib_count"))
          .when(array_contains(lit(rayanNames), lower(col("player_2"))), col("player_2_has_over_3_enemy_tarnib_count"))
          .when(array_contains(lit(rayanNames), lower(col("player_3"))), col("player_3_has_over_3_enemy_tarnib_count"))
          .when(array_contains(lit(rayanNames), lower(col("player_4"))), col("player_4_has_over_3_enemy_tarnib_count"))
      )
      .withColumn(
        "fares_over_3_enemy_tarnib_count",
        when(array_contains(lit(faresNames), lower(col("player_1"))), col("player_1_has_over_3_enemy_tarnib_count"))
          .when(array_contains(lit(faresNames), lower(col("player_2"))), col("player_2_has_over_3_enemy_tarnib_count"))
          .when(array_contains(lit(faresNames), lower(col("player_3"))), col("player_3_has_over_3_enemy_tarnib_count"))
          .when(array_contains(lit(faresNames), lower(col("player_4"))), col("player_4_has_over_3_enemy_tarnib_count"))
      )
      .withColumn(
        "jack_over_3_enemy_tarnib_count",
        when(array_contains(lit(jackNames), lower(col("player_1"))), col("player_1_has_over_3_enemy_tarnib_count"))
          .when(array_contains(lit(jackNames), lower(col("player_2"))), col("player_2_has_over_3_enemy_tarnib_count"))
          .when(array_contains(lit(jackNames), lower(col("player_3"))), col("player_3_has_over_3_enemy_tarnib_count"))
          .when(array_contains(lit(jackNames), lower(col("player_4"))), col("player_4_has_over_3_enemy_tarnib_count"))
      )
      .withColumn(
        "jad_over_3_enemy_tarnib_count",
        when(array_contains(lit(jadNames), lower(col("player_1"))), col("player_1_has_over_3_enemy_tarnib_count"))
          .when(array_contains(lit(jadNames), lower(col("player_2"))), col("player_2_has_over_3_enemy_tarnib_count"))
          .when(array_contains(lit(jadNames), lower(col("player_3"))), col("player_3_has_over_3_enemy_tarnib_count"))
          .when(array_contains(lit(jadNames), lower(col("player_4"))), col("player_4_has_over_3_enemy_tarnib_count"))
      )
      .withColumn(
        "rayan_rounds_with_less_than_two_figures",
        when(array_contains(lit(rayanNames), lower(col("player_1"))), getLessThanTwoFiguresCount(col("player_1_cards")))
          .when(array_contains(lit(rayanNames), lower(col("player_2"))),
                getLessThanTwoFiguresCount(col("player_2_cards")))
          .when(array_contains(lit(rayanNames), lower(col("player_3"))),
                getLessThanTwoFiguresCount(col("player_3_cards")))
          .when(array_contains(lit(rayanNames), lower(col("player_4"))),
                getLessThanTwoFiguresCount(col("player_4_cards")))
      )
      .withColumn(
        "fares_rounds_with_less_than_two_figures",
        when(array_contains(lit(faresNames), lower(col("player_1"))), getLessThanTwoFiguresCount(col("player_1_cards")))
          .when(array_contains(lit(faresNames), lower(col("player_2"))),
                getLessThanTwoFiguresCount(col("player_2_cards")))
          .when(array_contains(lit(faresNames), lower(col("player_3"))),
                getLessThanTwoFiguresCount(col("player_3_cards")))
          .when(array_contains(lit(faresNames), lower(col("player_4"))),
                getLessThanTwoFiguresCount(col("player_4_cards")))
      )
      .withColumn(
        "jack_rounds_with_less_than_two_figures",
        when(array_contains(lit(jackNames), lower(col("player_1"))), getLessThanTwoFiguresCount(col("player_1_cards")))
          .when(array_contains(lit(jackNames), lower(col("player_2"))),
                getLessThanTwoFiguresCount(col("player_2_cards")))
          .when(array_contains(lit(jackNames), lower(col("player_3"))),
                getLessThanTwoFiguresCount(col("player_3_cards")))
          .when(array_contains(lit(jackNames), lower(col("player_4"))),
                getLessThanTwoFiguresCount(col("player_4_cards")))
      )
      .withColumn(
        "jad_rounds_with_less_than_two_figures",
        when(array_contains(lit(jadNames), lower(col("player_1"))), getLessThanTwoFiguresCount(col("player_1_cards")))
          .when(array_contains(lit(jadNames), lower(col("player_2"))),
                getLessThanTwoFiguresCount(col("player_2_cards")))
          .when(array_contains(lit(jadNames), lower(col("player_3"))),
                getLessThanTwoFiguresCount(col("player_3_cards")))
          .when(array_contains(lit(jadNames), lower(col("player_4"))),
                getLessThanTwoFiguresCount(col("player_4_cards")))
      )
      .withColumn(
        "rayan_suits_over_five_cards",
        when(array_contains(lit(rayanNames), lower(col("player_1"))),
             getSuitsWithOverFiveCardsCountUdf(col("player_1_cards")))
          .when(array_contains(lit(rayanNames), lower(col("player_2"))),
                getSuitsWithOverFiveCardsCountUdf(col("player_2_cards")))
          .when(array_contains(lit(rayanNames), lower(col("player_3"))),
                getSuitsWithOverFiveCardsCountUdf(col("player_3_cards")))
          .when(array_contains(lit(rayanNames), lower(col("player_4"))),
                getSuitsWithOverFiveCardsCountUdf(col("player_4_cards")))
      )
      .withColumn(
        "fares_suits_over_five_cards",
        when(array_contains(lit(faresNames), lower(col("player_1"))),
             getSuitsWithOverFiveCardsCountUdf(col("player_1_cards")))
          .when(array_contains(lit(faresNames), lower(col("player_2"))),
                getSuitsWithOverFiveCardsCountUdf(col("player_2_cards")))
          .when(array_contains(lit(faresNames), lower(col("player_3"))),
                getSuitsWithOverFiveCardsCountUdf(col("player_3_cards")))
          .when(array_contains(lit(faresNames), lower(col("player_4"))),
                getSuitsWithOverFiveCardsCountUdf(col("player_4_cards")))
      )
      .withColumn(
        "jack_suits_over_five_cards",
        when(array_contains(lit(jackNames), lower(col("player_1"))),
             getSuitsWithOverFiveCardsCountUdf(col("player_1_cards")))
          .when(array_contains(lit(jackNames), lower(col("player_2"))),
                getSuitsWithOverFiveCardsCountUdf(col("player_2_cards")))
          .when(array_contains(lit(jackNames), lower(col("player_3"))),
                getSuitsWithOverFiveCardsCountUdf(col("player_3_cards")))
          .when(array_contains(lit(jackNames), lower(col("player_4"))),
                getSuitsWithOverFiveCardsCountUdf(col("player_4_cards")))
      )
      .withColumn(
        "jad_suits_over_five_cards",
        when(array_contains(lit(jadNames), lower(col("player_1"))),
             getSuitsWithOverFiveCardsCountUdf(col("player_1_cards")))
          .when(array_contains(lit(jadNames), lower(col("player_2"))),
                getSuitsWithOverFiveCardsCountUdf(col("player_2_cards")))
          .when(array_contains(lit(jadNames), lower(col("player_3"))),
                getSuitsWithOverFiveCardsCountUdf(col("player_3_cards")))
          .when(array_contains(lit(jadNames), lower(col("player_4"))),
                getSuitsWithOverFiveCardsCountUdf(col("player_4_cards")))
      )
      .withColumn(
        "rayan_has_partner_picked_tarnib_count",
        when(array_contains(lit(rayanNames), lower(col("player_1"))), col("player_1_partner_picked_tarnib_count"))
          .when(array_contains(lit(rayanNames), lower(col("player_2"))), col("player_2_partner_picked_tarnib_count"))
          .when(array_contains(lit(rayanNames), lower(col("player_3"))), col("player_3_partner_picked_tarnib_count"))
          .when(array_contains(lit(rayanNames), lower(col("player_4"))), col("player_4_partner_picked_tarnib_count"))
      )
      .withColumn(
        "fares_has_partner_picked_tarnib_count",
        when(array_contains(lit(faresNames), lower(col("player_1"))), col("player_1_partner_picked_tarnib_count"))
          .when(array_contains(lit(faresNames), lower(col("player_2"))), col("player_2_partner_picked_tarnib_count"))
          .when(array_contains(lit(faresNames), lower(col("player_3"))), col("player_3_partner_picked_tarnib_count"))
          .when(array_contains(lit(faresNames), lower(col("player_4"))), col("player_4_partner_picked_tarnib_count"))
      )
      .withColumn(
        "jack_has_partner_picked_tarnib_count",
        when(array_contains(lit(jackNames), lower(col("player_1"))), col("player_1_partner_picked_tarnib_count"))
          .when(array_contains(lit(jackNames), lower(col("player_2"))), col("player_2_partner_picked_tarnib_count"))
          .when(array_contains(lit(jackNames), lower(col("player_3"))), col("player_3_partner_picked_tarnib_count"))
          .when(array_contains(lit(jackNames), lower(col("player_4"))), col("player_4_partner_picked_tarnib_count"))
      )
      .withColumn(
        "jad_has_partner_picked_tarnib_count",
        when(array_contains(lit(jadNames), lower(col("player_1"))), col("player_1_partner_picked_tarnib_count"))
          .when(array_contains(lit(jadNames), lower(col("player_2"))), col("player_2_partner_picked_tarnib_count"))
          .when(array_contains(lit(jadNames), lower(col("player_3"))), col("player_3_partner_picked_tarnib_count"))
          .when(array_contains(lit(jadNames), lower(col("player_4"))), col("player_4_partner_picked_tarnib_count"))
      )
      .withColumn(
        "rayan_suits_with_over_one_figures",
        when(array_contains(lit(rayanNames), lower(col("player_1"))),
             getSuitsWithOverOneFigureUdf(col("player_1_cards")))
          .when(array_contains(lit(rayanNames), lower(col("player_2"))),
                getSuitsWithOverOneFigureUdf(col("player_2_cards")))
          .when(array_contains(lit(rayanNames), lower(col("player_3"))),
                getSuitsWithOverOneFigureUdf(col("player_3_cards")))
          .when(array_contains(lit(rayanNames), lower(col("player_4"))),
                getSuitsWithOverOneFigureUdf(col("player_4_cards")))
      )
      .withColumn(
        "fares_suits_with_over_one_figures",
        when(array_contains(lit(faresNames), lower(col("player_1"))),
             getSuitsWithOverOneFigureUdf(col("player_1_cards")))
          .when(array_contains(lit(faresNames), lower(col("player_2"))),
                getSuitsWithOverOneFigureUdf(col("player_2_cards")))
          .when(array_contains(lit(faresNames), lower(col("player_3"))),
                getSuitsWithOverOneFigureUdf(col("player_3_cards")))
          .when(array_contains(lit(faresNames), lower(col("player_4"))),
                getSuitsWithOverOneFigureUdf(col("player_4_cards")))
      )
      .withColumn(
        "jack_suits_with_over_one_figures",
        when(array_contains(lit(jackNames), lower(col("player_1"))),
             getSuitsWithOverOneFigureUdf(col("player_1_cards")))
          .when(array_contains(lit(jackNames), lower(col("player_2"))),
                getSuitsWithOverOneFigureUdf(col("player_2_cards")))
          .when(array_contains(lit(jackNames), lower(col("player_3"))),
                getSuitsWithOverOneFigureUdf(col("player_3_cards")))
          .when(array_contains(lit(jackNames), lower(col("player_4"))),
                getSuitsWithOverOneFigureUdf(col("player_4_cards")))
      )
      .withColumn(
        "jad_suits_with_over_one_figures",
        when(array_contains(lit(jadNames), lower(col("player_1"))), getSuitsWithOverOneFigureUdf(col("player_1_cards")))
          .when(array_contains(lit(jadNames), lower(col("player_2"))),
                getSuitsWithOverOneFigureUdf(col("player_2_cards")))
          .when(array_contains(lit(jadNames), lower(col("player_3"))),
                getSuitsWithOverOneFigureUdf(col("player_3_cards")))
          .when(array_contains(lit(jadNames), lower(col("player_4"))),
                getSuitsWithOverOneFigureUdf(col("player_4_cards")))
      )
      .withColumn(
        "rayan_has_enemy_picked_tarnib_count",
        when(array_contains(lit(rayanNames), lower(col("player_1"))), col("player_1_enemy_picked_tarnib_count"))
          .when(array_contains(lit(rayanNames), lower(col("player_2"))), col("player_2_enemy_picked_tarnib_count"))
          .when(array_contains(lit(rayanNames), lower(col("player_3"))), col("player_3_enemy_picked_tarnib_count"))
          .when(array_contains(lit(rayanNames), lower(col("player_4"))), col("player_4_enemy_picked_tarnib_count"))
      )
      .withColumn(
        "fares_has_enemy_picked_tarnib_count",
        when(array_contains(lit(faresNames), lower(col("player_1"))), col("player_1_enemy_picked_tarnib_count"))
          .when(array_contains(lit(faresNames), lower(col("player_2"))), col("player_2_enemy_picked_tarnib_count"))
          .when(array_contains(lit(faresNames), lower(col("player_3"))), col("player_3_enemy_picked_tarnib_count"))
          .when(array_contains(lit(faresNames), lower(col("player_4"))), col("player_4_enemy_picked_tarnib_count"))
      )
      .withColumn(
        "jack_has_enemy_picked_tarnib_count",
        when(array_contains(lit(jackNames), lower(col("player_1"))), col("player_1_enemy_picked_tarnib_count"))
          .when(array_contains(lit(jackNames), lower(col("player_2"))), col("player_2_enemy_picked_tarnib_count"))
          .when(array_contains(lit(jackNames), lower(col("player_3"))), col("player_3_enemy_picked_tarnib_count"))
          .when(array_contains(lit(jackNames), lower(col("player_4"))), col("player_4_enemy_picked_tarnib_count"))
      )
      .withColumn(
        "jad_has_enemy_picked_tarnib_count",
        when(array_contains(lit(jadNames), lower(col("player_1"))), col("player_1_enemy_picked_tarnib_count"))
          .when(array_contains(lit(jadNames), lower(col("player_2"))), col("player_2_enemy_picked_tarnib_count"))
          .when(array_contains(lit(jadNames), lower(col("player_3"))), col("player_3_enemy_picked_tarnib_count"))
          .when(array_contains(lit(jadNames), lower(col("player_4"))), col("player_4_enemy_picked_tarnib_count"))
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
        "rayan_figures_score",
        "fares_figures_score",
        "jack_figures_score",
        "jad_figures_score",
        "rayan_tarnib_figures_count",
        "fares_tarnib_figures_count",
        "jack_tarnib_figures_count",
        "jad_tarnib_figures_count",
        "rayan_over_3_enemy_tarnib_count",
        "fares_over_3_enemy_tarnib_count",
        "jack_over_3_enemy_tarnib_count",
        "jad_over_3_enemy_tarnib_count",
        "rayan_has_enemy_picked_tarnib_count",
        "fares_has_enemy_picked_tarnib_count",
        "jack_has_enemy_picked_tarnib_count",
        "jad_has_enemy_picked_tarnib_count",
        "rayan_over_3_partner_tarnib_count",
        "fares_over_3_partner_tarnib_count",
        "jack_over_3_partner_tarnib_count",
        "jad_over_3_partner_tarnib_count",
        "rayan_has_partner_picked_tarnib_count",
        "fares_has_partner_picked_tarnib_count",
        "jack_has_partner_picked_tarnib_count",
        "jad_has_partner_picked_tarnib_count",
        "rayan_team_figures_count",
        "fares_team_figures_count",
        "jad_team_figures_count",
        "jack_team_figures_count",
        "rayan_rounds_with_less_than_two_figures",
        "fares_rounds_with_less_than_two_figures",
        "jack_rounds_with_less_than_two_figures",
        "jad_rounds_with_less_than_two_figures",
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

    // Save the dataframe in S3 as a json file
    aggregatedDF
      .coalesce(1)
      .write
      .format("json")
      .option("header", "true") // Optional: Write column names as the first row
      .mode(SaveMode.Overwrite)
      .save("s3://tarnib-analytics-bucket/analyzed-json-data")

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
    val figures = Seq("14", "13", "12", "11")
    figures.map(card => card.r.findAllIn(cards).length).sum
  }

  /**
    * Count occurrence of suits with more than 5 cards
    * Ex: if you had 6 spades, 6 hearts, 1 diamond, the function will return 2
    * @param cards - cards array string
    */
  def getSuitsWithOverFiveCardsCount(cards: String): Int = {
    val figures = Seq("of_spades", "of_hearts", "of_clubs", "of_diamonds")
    figures.map(card => if (card.r.findAllIn(cards).length > 5) 1 else 0).sum
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

  /**
    * Get score of a players figures
    * J = 1, Q = 2, K = 3, A = 4
    * @param cards
    */
  def getFiguresScore(cards: String): Int = {
    val figures = Seq("11", "12", "13", "14")
    figures.map(card => card.r.findAllIn(cards).length * (figures.indexOf(card) + 1)).sum
  }

  /**
    * Returns true if player had less than 2 figures, false otherwise
    * @param cards
    */
  def hadLessThanTwoFigures(cards: String): Boolean = {
    getFiguresCount(cards) < 2
  }

  /**
    * Count figures of the chosen tarnib
    * @param cards
    * @param tarnib
    */
  def getTarnibFiguresCount(cards: String, tarnib: String): Int = {
    val figures = Seq(s"11_of_$tarnib", s"12_of_$tarnib", s"13_of_$tarnib", s"14_of_$tarnib")
    figures.map(card => card.r.findAllIn(cards).length).sum
  }

  /**
    * Return true if the player has over 3 of his partners chosen tarnib, otherwise return false
    * If the tarnib was not chosen by the partner, returns false
    * @param cards
    * @param tarnib
    * @param bids_data
    * @param player_pair - represents the current team player pair, ex: 1_3 (player 1 and player 3)
    */
  def hasOverThreePartnersTarnib(cards: String, tarnib: String, bids_data: String, player_pair: String): Boolean = {
    // Get the player who picked the tarnib with +1 offset (if its 1, then its player_2 who picked)
    val tarnib_picker = bids_data(bids_data.length - 2).toString.toInt + 1
    val player = player_pair(0).toString.toInt // Get the current player

    if (player == 1 && tarnib_picker == 3 || player == 2 && tarnib_picker == 4 || player == 3 && tarnib_picker == 1 || player == 4 && tarnib_picker == 1) {
      if (tarnib.r.findAllIn(cards).length > 3) return true
    }
    false
  }

  /**
    * Return true if the player has over 3 of his enemies chosen tarnib, otherwise return false
    * If the tarnib was not chosen by an enemy player, returns false
    * @param cards
    * @param tarnib
    * @param bids_data
    * @param player_pair - represents the enemy team player pair, ex: 1_3 (player 1 and player 3)
    */
  def hasOverThreeEnemyTarnib(cards: String, tarnib: String, bids_data: String, enemy_player_pair: String): Boolean = {
    // Get the player who picked the tarnib with +1 offset (if its 1, then its player_2 who picked)
    val tarnib_picker = (bids_data(bids_data.length - 2).toString.toInt + 1).toString

    if (enemy_player_pair.contains(tarnib_picker)) {
      if (tarnib.r.findAllIn(cards).length > 3) return true
    }
    false
  }

  /**
    * Return true if the partner has picked the tarnib, false otherwise
    * @param bids_data
    * @param player_pair - represents the enemy team player pair, ex: 1_3 (player 1 and player 3)
    */
  def hasPartnerPickedTarnib(bids_data: String, player_pair: String): Boolean = {
    // Get the player who picked the tarnib with +1 offset (if its 1, then its player_2 who picked)
    val tarnib_picker = bids_data(bids_data.length - 2).toString.toInt + 1
    val player = player_pair(0).toString.toInt // Get the current player

    if (player == 1 && tarnib_picker == 3 || player == 2 && tarnib_picker == 4 || player == 3 && tarnib_picker == 1 || player == 4 && tarnib_picker == 1)
      return true
    else return false
  }

  /**
    * Return true if any enemy player has picked the tarnib, false otherwise
    * @param bids_data
    * @param enemy_player_pair - represents the enemy team player pair, ex: 1_3 (player 1 and player 3)
    */
  def hasEnemyPickedTarnib(bids_data: String, enemy_player_pair: String): Boolean = {
    // Get the player who picked the tarnib with +1 offset (if its 1, then its player_2 who picked)
    val tarnib_picker = (bids_data(bids_data.length - 2).toString.toInt + 1).toString
    if (enemy_player_pair.contains(tarnib_picker)) return true else return false
  }

}
