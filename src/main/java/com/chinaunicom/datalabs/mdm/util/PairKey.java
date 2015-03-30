package com.chinaunicom.datalabs.mdm.util;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * construct keys
 * Created by zhangxr103 on 2015/3/30.
 */
public class PairKey<F extends WritableComparable<F>, S extends WritableComparable<S>> implements WritableComparable<PairKey<F,S>> {
    private F firstKey;
    private S secondKey;

    public PairKey(F firstKey, S secondKey) {
        this.firstKey = firstKey;
        this.secondKey = secondKey;
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
    public int compareTo(PairKey<F,S> o) {
        if (firstKey != null && o.firstKey != null) {
            return firstKey.compareTo( o.firstKey) == 0 ? secondKey != null && o.secondKey != null ? secondKey.compareTo( o.secondKey) : firstKey.compareTo((F) o.firstKey) : -1;
        } else {
            return -1;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PairKey pairKey = (PairKey) o;

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

