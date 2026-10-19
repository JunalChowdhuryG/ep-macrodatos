package c1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import common.Sums;

/**
 * C1 - Job 3: para cada carretera calcula, por sitio, la RAZON entre el % de
 * exceso del sitio y el % de exceso medio de la carretera, y emite los top-K.
 * Descarta sitios con menos de min.n detecciones (promedios de pocas filas son ruido).
 * Salida: carretera TAB rank,lat,lon,n,exceso_medio,pct_sitio,pct_carretera,razon
 */
public class Job3Reducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

    private int minN;
    private int topK;

    @Override
    public void configure(JobConf job) {
        minN = job.getInt("min.n", 30);
        topK = job.getInt("top.k", 20);
    }

    public void reduce(Text carretera, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        double totalN = -1, totalPct = 0;
        List<double[]> sitios = new ArrayList<double[]>();
        while (values.hasNext()) {
            String v = values.next().toString();
            if (v.startsWith("TOTAL:")) {
                double[] t = Sums.parse(v.substring(6));
                totalN = t[0];
                totalPct = t[1];
            } else if (v.startsWith("SITIO:")) {
                sitios.add(Sums.parse(v.substring(6)));
            }
        }
        if (totalN <= 0) {
            reporter.incrCounter("calidad", "carretera_sin_total", 1);
            return;
        }
        double pctCarretera = totalPct / totalN;

        List<double[]> filas = new ArrayList<double[]>(); // n, pctSitio, lat, lon, excesoMedio, razon
        for (double[] s : sitios) {
            if (s[0] < minN) {
                reporter.incrCounter("calidad", "sitios_descartados_n_bajo", 1);
                continue;
            }
            double pct = s[1] / s[0];
            filas.add(new double[] {s[0], pct, s[2] / s[0], s[3] / s[0], s[4] / s[0], pct / pctCarretera});
        }
        Collections.sort(filas, new Comparator<double[]>() {
            public int compare(double[] a, double[] b) {
                return Double.compare(b[5], a[5]); // razon descendente
            }
        });

        int k = Math.min(topK, filas.size());
        for (int i = 0; i < k; i++) {
            double[] f = filas.get(i);
            String linea = String.format(Locale.US, "%d,%.6f,%.6f,%d,%.3f,%.5f,%.5f,%.4f",
                    i + 1, f[2], f[3], (long) f[0], f[4], f[1], pctCarretera, f[5]);
            output.collect(carretera, new Text(linea));
        }
    }
}
