/**
 * Copyright 2011 NexR
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.nexr.rhive.util;

import java.util.Comparator;
import java.util.TreeMap;

import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class RangeTreeFactory {
    
    @SuppressWarnings("unchecked")
    public static RangeTree createStringTree(String name, boolean minExclusive,
            boolean maxExclusive, Object defaultValue) {
        StringRangeComparator scomp = new StringRangeComparator(minExclusive, maxExclusive);
        return new StringRangeTree(name, scomp, defaultValue);
    }

    @SuppressWarnings("unchecked")
    public static RangeTree createDoubleTree(String name, boolean minExclusive,
            boolean maxExclusive, Object defaultValue) {
        DoubleRangeComparator dcomp = new DoubleRangeComparator(minExclusive, maxExclusive);
        return new DoubleRangeTree(name, dcomp, defaultValue);
    }
    
    @SuppressWarnings("unchecked")
    public static RangeTree createFloatTree(String name, boolean minExclusive,
            boolean maxExclusive, Object defaultValue) {
        FloatRangeComparator dcomp = new FloatRangeComparator(minExclusive, maxExclusive);
        return new FloatRangeTree(name, dcomp, defaultValue);
    }

    @SuppressWarnings("unchecked")
    public static RangeTree createLongTree(String name, boolean minExclusive,
            boolean maxExclusive, Object defaultValue) {
        LongRangeComparator lcomp = new LongRangeComparator(minExclusive, maxExclusive);
        return new LongRangeTree(name, lcomp, defaultValue);
    }

    @SuppressWarnings("unchecked")
    public static RangeTree createIntTree(String name, boolean minExclusive, boolean maxExclusive,
            Object defaultValue) {
        IntRangeComparator icomp = new IntRangeComparator(minExclusive, maxExclusive);
        return new IntRangeTree(name, icomp, defaultValue);
    }

    @SuppressWarnings("unchecked")
    public static abstract class RangeTree<T, V> extends TreeMap<T, V> {

        private final String name;
        private final V defaultValue;

        RangeTree(String name, Comparator<T> comparator, V defaultValue) {
            super(comparator);
            this.name = name;
            this.defaultValue = defaultValue;
        }

        public V get(Object key) {
            V result = super.get(key);
            return result == null ? defaultValue : result;
        }

        public String getName() {
            return name;
        }

        public Writable convertWritable(Object obj) {
            if(obj==null) return null;
            Writable w = null;
            if (obj instanceof Integer) {
                w = new IntWritable(((Integer) obj).intValue());
            } else if (obj instanceof Long) {
                w = new LongWritable(((Long) obj).longValue());
            } else if (obj instanceof Double) {
                w = new DoubleWritable(((Double) obj).doubleValue());
            } else {
                w = new Text(obj.toString());
            }
            return w;
        }

    }

    @SuppressWarnings("unchecked")
    public static class StringRangeTree<V> extends RangeTree<String[], V> {

        StringRangeTree(String name, Comparator<String[]> comparator, V defaultValue) {
            super(name, comparator, defaultValue);
        }

        public V search(String value) {
            return get(new String[] { value, value });
        }

        public Writable searchWritable(String value) {
            return convertWritable(search(value));
        }
    }

    @SuppressWarnings("unchecked")
    public static class IntRangeTree<V> extends RangeTree<int[], V> {

        IntRangeTree(String name, Comparator<int[]> comparator, V defaultValue) {
            super(name, comparator, defaultValue);
        }

        public V search(int value) {
            return get(new int[] { value, value });
        }

        public Writable searchWritable(int value) {
            return convertWritable(search(value));
        }
    }

    @SuppressWarnings("unchecked")
    public static class LongRangeTree<V> extends RangeTree<long[], V> {

        LongRangeTree(String name, Comparator<long[]> comparator, V defaultValue) {
            super(name, comparator, defaultValue);
        }

        public V search(long value) {
            return get(new long[] { value, value });
        }

        public Writable searchWritable(long value) {
            return convertWritable(search(value));
        }
    }

    @SuppressWarnings("unchecked")
    public static class DoubleRangeTree<V> extends RangeTree<double[], V> {

        DoubleRangeTree(String name, Comparator<double[]> comparator, V defaultValue) {
            super(name, comparator, defaultValue);
        }

        public V search(double value) {
            return get(new double[] { value, value });
        }

        public Writable searchWritable(double value) {
            return convertWritable(search(value));
        }
    }
    
    @SuppressWarnings("unchecked")
    public static class FloatRangeTree<V> extends RangeTree<float[], V> {

        FloatRangeTree(String name, Comparator<float[]> comparator, V defaultValue) {
            super(name, comparator, defaultValue);
        }

        public V search(float value) {
            return get(new float[] { value, value });
        }

        public Writable searchWritable(float value) {
            return convertWritable(search(value));
        }
    }

    @SuppressWarnings("unchecked")
    private static class StringRangeComparator implements Comparator<String[]> {

        final boolean minExclusive;
        final boolean maxExclusive;

        public StringRangeComparator(boolean minExclusive, boolean maxExclusive) {
            this.minExclusive = minExclusive;
            this.maxExclusive = maxExclusive;
        }

        public int compare(String[] o1, String[] o2) {
            int minCompared = o1[0].compareTo(o2[0]);
            if (minCompared <= 0) {
                return minCompared == 0 && minExclusive ? -1 : minCompared;
            }
            int maxCompared = o1[1].compareTo(o2[1]);
            if (maxCompared <= 0) {
                return maxCompared == 0 && maxExclusive ? 1 : 0;
            }
            return maxCompared;
        }
    }

    private static class IntRangeComparator implements Comparator<int[]> {

        final boolean minExclusive;
        final boolean maxExclusive;

        public IntRangeComparator(boolean minExclusive, boolean maxExclusive) {
            this.minExclusive = minExclusive;
            this.maxExclusive = maxExclusive;
        }

        public int compare(int[] o1, int[] o2) {
            int minCompared = o1[0] < o2[0] ? -1 : o1[0] == o2[0] ? 0 : 1;
            if (minCompared <= 0) {
                return minCompared == 0 && minExclusive ? -1 : minCompared;
            }
            int maxCompared = o1[1] < o2[1] ? -1 : o1[1] == o2[1] ? 0 : 1;
            if (maxCompared <= 0) {
                return maxCompared == 0 && maxExclusive ? 1 : 0;
            }
            return maxCompared;
        }
    }

    private static class LongRangeComparator implements Comparator<long[]> {

        final boolean minExclusive;
        final boolean maxExclusive;

        public LongRangeComparator(boolean minExclusive, boolean maxExclusive) {
            this.minExclusive = minExclusive;
            this.maxExclusive = maxExclusive;
        }

        public int compare(long[] o1, long[] o2) {
            int minCompared = o1[0] < o2[0] ? -1 : o1[0] == o2[0] ? 0 : 1;
            if (minCompared <= 0) {
                return minCompared == 0 && minExclusive ? -1 : minCompared;
            }
            int maxCompared = o1[1] < o2[1] ? -1 : o1[1] == o2[1] ? 0 : 1;
            if (maxCompared <= 0) {
                return maxCompared == 0 && maxExclusive ? 1 : 0;
            }
            return maxCompared;
        }
    }

    private static class DoubleRangeComparator implements Comparator<double[]> {

        final boolean minExclusive;
        final boolean maxExclusive;

        public DoubleRangeComparator(boolean minExclusive, boolean maxExclusive) {
            this.minExclusive = minExclusive;
            this.maxExclusive = maxExclusive;
        }

        public int compare(double[] o1, double[] o2) {
            int minCompared = o1[0] < o2[0] ? -1 : o1[0] == o2[0] ? 0 : 1;
            if (minCompared <= 0) {
                return minCompared == 0 && minExclusive ? -1 : minCompared;
            }
            int maxCompared = o1[1] < o2[1] ? -1 : o1[1] == o2[1] ? 0 : 1;
            if (maxCompared <= 0) {
                return maxCompared == 0 && maxExclusive ? 1 : 0;
            }
            return maxCompared;
        }
    }
    
    private static class FloatRangeComparator implements Comparator<float[]> {

        final boolean minExclusive;
        final boolean maxExclusive;

        public FloatRangeComparator(boolean minExclusive, boolean maxExclusive) {
            this.minExclusive = minExclusive;
            this.maxExclusive = maxExclusive;
        }

        public int compare(float[] o1, float[] o2) {
            int minCompared = o1[0] < o2[0] ? -1 : o1[0] == o2[0] ? 0 : 1;
            if (minCompared <= 0) {
                return minCompared == 0 && minExclusive ? -1 : minCompared;
            }
            int maxCompared = o1[1] < o2[1] ? -1 : o1[1] == o2[1] ? 0 : 1;
            if (maxCompared <= 0) {
                return maxCompared == 0 && maxExclusive ? 1 : 0;
            }
            return maxCompared;
        }
    }
    
}
