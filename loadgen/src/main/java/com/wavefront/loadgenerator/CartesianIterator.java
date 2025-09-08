package com.wavefront.loadgenerator;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Contains static methods to evenly generate load in a cartesian space
 *
 * @author Mori Bellamy (mori@wavefront.com)
 */
public class CartesianIterator {

  private CartesianIterator() {}

  /**
   * A contrived way to evenly generate load in a cartesian space given that the cardinality of each
   * dimension should have certain proportions with each other.
   *
   * @param weights a vector W
   * @param target  T
   * @return a vector V such that, (1) for all i,j, V[i] / V[j] = W[i] / W[j] and (2) PI(V) is
   * approximately equal to T.
   */
  public static List<Integer> weightedDimensionsForLoad(List<Integer> weights, int target) {
    double denominator =
        Math.pow(weights.stream().reduce(1, (x, y) -> x * y), 1.0 / weights.size());
    double nthRoot = Math.pow(target, 1.0 / weights.size());
    return weights.stream().map(x -> Math.max(1, Math.round(x * nthRoot / denominator)))
        .map(Math::toIntExact).collect(Collectors.toList());
  }

  /**
   * A contrived way to evenly generate load in a cartesian space given that certain dimensions are
   * pegged to a given value.
   *
   * @param pegs   A vector P
   * @param target T
   * @return A vector V such that (1) for all i, if P[i] != 0 then V[i] = P[i] and (2) PI(V) is
   * approximately T, if the initial values for P allow it and (3) all zeros of P are replaced by
   * approximately the same number in V.
   */
  public static List<Integer> peggedDimensions(List<Integer> pegs, int target) {
    long product = pegs.stream().filter(p -> p != 0L).reduce(1, (x, y) -> x * y);
    long numZeros = pegs.stream().filter(p -> p == 0L).count();
    if (numZeros == 0) {
      return ImmutableList.copyOf(pegs);
    }
    long otherCardinalities = Math.round(Math.pow((1.0 * target / product), 1.0 / numZeros));
    otherCardinalities = Math.max(1, otherCardinalities);
    ImmutableList.Builder<Integer> builder = new ImmutableList.Builder<>();
    for (Integer peg : pegs) {
      if (peg == 0) {
        builder.add(Math.toIntExact(otherCardinalities));
      } else {
        builder.add(peg);
      }
    }
    return builder.build();
  }
}
