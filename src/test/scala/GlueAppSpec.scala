import com.fares.AWSGlue.GlueApp
import org.scalatest.flatspec.AnyFlatSpec

/**
  * A class that runs a local execution of an AWS Glue job within a scalatest
  * Instead of running our local executions, it is preferred to call them from
  * a test framework, where we are able to add assertions for verification.
  */
class GlueAppSpec extends AnyFlatSpec {

  "GlueApp" should "run successfully" in {

    println("Executing test case!")
    // Trigger the execution by directly calling the main class and supplying
    // arguments. AWS Glue job arguments always begin with "--"
    GlueApp.main(
      Array(
        "--JOB_NAME",
        "job",
        "--stage",
        "dev"
      ))

    assert(true)
  }


  it should "count the correct amount of figures" in {
    val player_cards =
      "[\"10_of_hearts\",\"2_of_hearts\",\"14_of_spades\",\"13_of_spades\",\"7_of_spades\",\"2_of_spades\"," +
        "\"14_of_diamonds\",\"10_of_diamonds\",\"8_of_diamonds\",\"3_of_diamonds\",\"14_of_clubs\",\"12_of_clubs\"," +
        "\"8_of_clubs\"]"
    val figures_count = GlueApp.getFiguresCount(player_cards)
    assert(figures_count == 5)
  }

  it should "count the correct amount of suits with over 5 cards" in {
    val player_cards =
      "[\"10_of_hearts\",\"2_of_hearts\",\"14_of_spades\",\"13_of_spades\",\"14_of_diamonds\",\"13_of_diamonds\"" +
        "\"12_of_diamonds\",\"10_of_diamonds\",\"8_of_diamonds\",\"3_of_diamonds\",\"14_of_clubs\",\"12_of_clubs\"," +
        "\"8_of_clubs\"]"
    val suits_with_over_five_cards_count = GlueApp.getSuitsWithOverFiveCardsCount(player_cards)
    assert(suits_with_over_five_cards_count == 1)
  }
}
