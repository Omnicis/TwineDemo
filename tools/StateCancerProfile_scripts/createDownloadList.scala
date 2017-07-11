#!/bin/sh
exec scala "$0" "$@"
!#

object StateCancerProfileDownload {
  val ageGrps = Seq(
    "001" -> "all",
    "006" -> "lt65",
    "009" -> "lt50",
    "136" -> "50plus",
    "157" -> "65plus"
  ).toMap

  val raceGrps = Seq(
    "00" -> "All",
    "01" -> "White",
    "02" -> "Black",
    "03" -> "AIAN",
    "04" -> "API",
    "05" -> "HispAll",
    "06" -> "WhiteHisp",
    "07" -> "WhiteNonHisp"
  ).toMap

  val sex = Seq(
    "0" -> "B",
    "1" -> "M",
    "2" -> "F"
  ).toMap

  val stat = Seq(
    "01" -> "Inc",
    "02" -> "Mor"
  ).toMap

  def main(args: Array[String]) {
    val list = for {
      v <- stat.keys
      age <- ageGrps.keys
      race <- raceGrps.keys
      s <- sex.keys
    } yield {
      val q = s"99&${age}&047&${race}&${s}&${v}&0&1&5&0"
      val file = s"${stat(v)}_${raceGrps(race)}_${sex(s)}_${ageGrps(age)}.csv"
      "http://statecancerprofiles.cancer.gov/map/mapData.php?" + q + "&export " + file
    }

    list.foreach(println)
  }

}

StateCancerProfileDownload.main(args)
