import org.apache.spark.mllib.linalg.Vector

def vectorsToMaps(vocabulary: Array[String]) = {
  (v: Vector) => {
    val sv = v.toSparse
    sv.indices.map(i => (vocabulary(i) -> sv(i))).toMap
  }
}