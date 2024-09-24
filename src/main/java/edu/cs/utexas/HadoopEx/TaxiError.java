package edu.cs.utexas.HadoopEx;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;


public class TaxiError implements Comparable<TaxiError> {

        private final IntWritable hour;
        private final IntWritable error;

        public TaxiError( IntWritable hour, IntWritable count) {
            this.hour = hour;
            this.count = count;
        }

        public IntWritable getHour() {
            return hour;
        }

        public IntWritable getCount() {
            return count;
        }
    /**
     * Compares two sort data objects by their value.
     * @param other
     * @return 0 if equal, negative if this < other, positive if this > other
     */
        @Override
        public int compareTo(WordAndCount other) {

            float diff = error.get() - other.error.get();
            if (diff > 0) {
                return 1;
            } else if (diff < 0) {
                return -1;
            }
            return 0;
        }


        public String toString(){

            return "("+hour.toString() +" , "+ count.toString()+")";
        }
    }

