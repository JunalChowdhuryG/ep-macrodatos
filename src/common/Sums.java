package common;

import java.util.Iterator;

import org.apache.hadoop.io.Text;

/**
 * Vector de sumas "a,b,c,..." como texto. Sumar vectores es asociativo y
 * conmutativo, asi que el mismo codigo sirve de Combiner y de Reducer.
 * (Double.toString no depende del locale, a diferencia de String.format sin Locale.)
 */
public final class Sums {

    private Sums() {
    }

    public static Text of(double... xs) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < xs.length; i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append(xs[i]);
        }
        return new Text(sb.toString());
    }

    public static double[] parse(String s) {
        String[] p = s.split(",");
        double[] a = new double[p.length];
        for (int i = 0; i < p.length; i++) {
            a[i] = Double.parseDouble(p[i]);
        }
        return a;
    }

    /** Suma elemento a elemento todos los vectores. */
    public static double[] merge(Iterator<Text> values) {
        double[] acc = null;
        while (values.hasNext()) {
            double[] a = parse(values.next().toString());
            if (acc == null) {
                acc = a;
            } else {
                if (a.length != acc.length) {
                    throw new IllegalStateException("Vectores de distinta longitud: " + a.length + " vs " + acc.length);
                }
                for (int i = 0; i < a.length; i++) {
                    acc[i] += a[i];
                }
            }
        }
        return acc;
    }
}
