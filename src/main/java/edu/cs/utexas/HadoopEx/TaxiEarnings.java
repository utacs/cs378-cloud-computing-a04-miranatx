package edu.cs.utexas.HadoopEx;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;

public class TaxiEarnings implements Comparable<TaxiEarnings> {

  private final Text taxi;
  private final FloatWritable earningsRate;

  // Constructor
  public TaxiEarnings(Text taxi, FloatWritable earningsRate) {
    this.taxi = taxi;
    this.earningsRate = earningsRate;
  }

  // Getters
  public Text getTaxi() {
    return taxi;
  }

  public FloatWritable getEarningsRate() {
    return earningsRate;
  }

  // Comparison based on earnings rate
  @Override
  public int compareTo(TaxiEarnings other) {
    float diff = earningsRate.get() - other.earningsRate.get();
    if (diff > 0) {
      return 1;
    } else if (diff < 0) {
      return -1;
    }
    return 0;
  }

  // toString method
  @Override
  public String toString() {
    return "(" + taxi.toString() + " , Earnings Rate: " + earningsRate.toString() + ")";
  }

  // Override equals to compare only the taxi (key)
  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }

    TaxiEarnings that = (TaxiEarnings) obj;

    // Check if the taxi (key) fields are equal
    return this.taxi.equals(that.taxi);
  }
}