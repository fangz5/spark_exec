class Fraction(val n: Int, val d: Int) {
  def *(other: Fraction): Fraction = new Fraction(n * other.n, d * other.d)

  override def toString(): String = n + "/" + d
}

implicit def toFraction(i: Int): Fraction = new Fraction(i, 1)

val f1: Fraction = new Fraction(2, 3)

println(f1 * 2)


// implicit object
object Test {
  def echo: Unit = println("This is a test!")
}

implicit def toTest(i: Int) = Test
