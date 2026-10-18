package common;

import java.util.Iterator;

import org.apache.hadoop.io.Text;


public final class Stat {

    private Stat() {
    }

    public static Text of(double x) {
        return new Text("1," + x + "," + (x * x) + "," + x + "," + x);
    }

    public static double[] merge(Iterator<Text> values) {
        double n = 0, sum = 0, sumsq = 0;
        double min = Double.POSITIVE_INFINITY, max = Double.NEGATIVE_INFINITY;
        while (values.hasNext()) {
            String[] p = values.next().toString().split(",");
            n += Double.parseDouble(p[0]);
            sum += Double.parseDouble(p[1]);
            sumsq += Double.parseDouble(p[2]);
            min = Math.min(min, Double.parseDouble(p[3]));
            max = Math.max(max, Double.parseDouble(p[4]));
        }
        return new double[] {n, sum, sumsq, min, max};
    }

    public static Text format(double[] a) {
        return new Text(a[0] + "," + a[1] + "," + a[2] + "," + a[3] + "," + a[4]);
    }
}
