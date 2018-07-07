

import org.scalatest.FlatSpec

class SetTest extends FlatSpec{


  "A union" should " the union of two sets" in {
    val low:Set[Int] = (1 to 5).toSet
    println(low)
    val medium = (3 to 7).toSet
    println(medium)

    val unionSet=low.union(medium)
    val unionSet2=(1 to 7).toSet

    println(unionSet)
    println(unionSet2)

    assert( unionSet.toArray.sortWith((a,b)=>a<b) sameElements  unionSet2.toArray.sortWith((a,b)=>a<b))
  }

  "A intersect funtion" should " return the intersection of he two test" in {
    val low:Set[Int] = (1 to 5).toSet
    println(low)
    val medium = (3 to 7).toSet
    println(medium)

    val intersectSet=low.intersect(medium)
    val intersectSet2=(3 to 5).toSet

    println("intersectSet: "+intersectSet)
    println("intersectSet2: "+intersectSet2)

    assert( intersectSet.toArray.sortWith((a,b)=>a<b) sameElements intersectSet2.toArray.sortWith((a,b)=>a<b))
  }

}
