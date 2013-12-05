package movielens

import com.twitter.scalding._
import scala.util.Try

class Ratings(args: Args) extends Job(args) {

  def intOpt(key: String, default: Int) =
    args.optional(key).flatMap(i => Try(i.toInt).toOption).getOrElse(default)

  val minRatings = intOpt("min", 100)
  val topRated = intOpt("top", 10)
  val topDiverse = intOpt("div", 5)


  val ratingsSchema = ('user, 'movie, 'rating, 'timestamp)
  val userSchema = ('user, 'gender, 'age, 'occupation, 'zip)
  val movieSchema = ('movie, 'title, 'genres)

  val movies = Csv("data/movielens/movies.dat", separator = "::", fields = movieSchema).read

  val ratings = Csv("data/movielens/ratings.dat", separator = "::", fields = ratingsSchema).read

  val users = Csv("data/movielens/users.dat", separator = "::", fields = userSchema).read


  val merged = ratings
    .joinWithSmaller('user -> 'user, users)
    .joinWithSmaller('movie -> 'movie, movies)

  val withRatings = merged
    .groupBy('movie) { _.size('numRatings) }
    .joinWithLarger('movie -> 'movie, merged)

  val activelyRated = withRatings
    .filter('numRatings) { num: Int => num >= minRatings }

  val ratingsByGender = activelyRated
    .groupBy('title, 'gender) { _.sizeAveStdev('rating -> ('numRatings, 'rating, 'devRating)) }
    .groupBy('title) { _.pivot(('gender, 'rating) -> ('M, 'F)) }

  val topFemale = ratingsByGender
    .groupAll { _.sortWithTake(('title, 'F) -> 'topFemaleRatings, topRated) {
      (t1: (String, Double), t2: (String, Double)) => t1._2 > t2._2 } }
    .flatMapTo('topFemaleRatings -> ('title, 'rating)) { sorted: List[(String, Double)] => sorted.toIterable }

  val genderDisagreements = ratingsByGender
    .map(('F, 'M) -> 'diff){ r: (Double, Double) => r._1 - r._2 }
    .groupAll { _.sortBy('diff) }

  val ratingDiversityByGenre = activelyRated
    .flatMap('genres -> 'genre) { genres: String => genres.split('|') }.discard('genres)
    .groupBy('genre, 'title) { _.sizeAveStdev('rating -> ('numRatings, 'rating, 'devRating)) }


  ratingsByGender.write(Csv("output/movielens/ratingsByGender.csv", writeHeader = true))
  topFemale.write(Csv("output/movielens/topFemale.csv", writeHeader = true))
  genderDisagreements.write(Csv("output/movielens/genderDisagreements.csv", writeHeader = true))

  ratingDiversityByGenre
    .groupBy('genre) { _.sortBy('rating).reverse.take(topRated) }
    .write(Csv("output/movielens/topRatedByGenre.csv", writeHeader = true))

  ratingDiversityByGenre
    .groupBy('genre) { _.sortBy('devRating).reverse.take(topDiverse) }
    .write(Csv("output/movielens/mostDiverseByGenre.csv", writeHeader = true))
}
