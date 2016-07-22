package Ben.com.Util

object caseUtils extends Serializable {

  case class movieLens(user: Int, movie: Int, rating: Double, timstamp: Long)
  case class movie(id: Int, title: String, category: String)

}