package edu.cs.utexas.HadoopEx;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;


public class TaxiError implements Comparable<TaxiError> {

        private final IntWritable hour;
        private final IntWritable error;

        public TaxiError( IntWritable hour, IntWritable error) {
            this.hour = hour;
            this.error = error;
        }

        public IntWritable getHour() {
            return hour;
        }

        public IntWritable getError() {
            return error;
        }
    /**
     * Compares two sort data objects by their value.
     * @param other
     * @return 0 if equal, negative if this < other, positive if this > other
     */
        @Override
        public int compareTo(TaxiError other) {

            float diff = error.get() - other.error.get();
            if (diff > 0) {
                return 1;
            } else if (diff < 0) {
                return -1;
            }
            return 0;
        }


        public String toString(){

            return "("+ hour.toString() +" , "+ error.toString() +")";
        }
    }

