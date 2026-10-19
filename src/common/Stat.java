package common;

import java.util.Iterator;

import org.apache.hadoop.io.Text;

/**
 * Estadistico "mergeable": n, suma, suma de cuadrados, minimo y maximo.
 * Se puede combinar en cualquier orden y agrupacion (asociativo y conmutativo),
 * por eso sirve como Combiner y como Reducer a la vez.
 *
 * Formato de texto: "n,suma,sumaCuadrados,min,max" (Double.toString es
 * independiente del locale, a diferencia de String.format sin Locale).
 */
public final class Stat {

    private Stat() {
    }

    /** Estadistico de una sola observacion. */
    public static Text of(double x) {
        return new Text("1," + x + "," + (x * x) + "," + x + "," + x);
    }

    /** Combina todos los valores en [n, suma, sumaCuadrados, min, max]. */
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
