package com.chinaunicom.datalabs.mdm.util;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * construct keys
 * Created by zhangxr103 on 2015/3/30.
 */
public class PairWritable<F extends WritableComparable<F>, S extends WritableComparable<S>> implements WritableComparable<PairWritable> {
    private F firstKey;
    private S secondKey;

    public F getFirstKey() {
        return firstKey;
    }

    public void setFirstKey(F firstKey) {
        this.firstKey = firstKey;
    }

    public S getSecondKey() {
        return secondKey;
    }

    public void setSecondKey(S secondKey) {
        this.secondKey = secondKey;
    }

    public void setPair(F firstKey,S secondKey){
        this.firstKey=firstKey;
        this.secondKey=secondKey;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        firstKey.write(out);
        secondKey.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        firstKey.readFields(in);
        secondKey.readFields(in);
    }

    @Override
    public int compareTo(PairWritable o) {
        if (firstKey != null && o.firstKey != null) {
            return firstKey.compareTo((F) o.firstKey) == 0 ? secondKey != null && o.secondKey != null ? secondKey.compareTo((S) o.secondKey) : firstKey.compareTo((F) o.firstKey) : -1;
        } else {
            return -1;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PairWritable pairKey = (PairWritable) o;

        if (!firstKey.equals(pairKey.firstKey)) return false;
        if (!secondKey.equals(pairKey.secondKey)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = firstKey.hashCode();
        result = 31 * result + secondKey.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return firstKey +"\t" + secondKey;
    }
}

