package es.weso.wdsub.spark.pschema

/**
 * Information about a label that depends on some triple
 *
 * @param srcLabel label
 * @param dependTriple triple on which this label depends
 */
case class DependInfo[P,L](
                            srcLabel: L,
                            dependTriple: DependTriple[P,L]
                          ) {
  override def toString: String =
    s"depend(srcLabel=$srcLabel, triple: $dependTriple)"

}