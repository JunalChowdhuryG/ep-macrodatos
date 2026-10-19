package c3;

import common.Grid;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

/**
 * C3 - Job E (reducer): Davies-Bouldin y silueta simplificada global.
 *   S_i = dispersion media del cluster i (km), M_ij = distancia entre centroides
 *   DB = (1/k) * sum_i max_{j!=i} (S_i + S_j) / M_ij        (menor = mejor)
 * Opera sobre los k resumenes de cluster (pocos), no sobre los puntos.
 * Salida: lineas "cluster TAB label,lat,lon,n,dispersion_km,silueta_media" y 4 lineas globales.
 */
public class EvalReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

    private Map<Long, double[]> cent;

    @Override
    public void configure(JobConf job) {
        cent = HdfsLines.centroids(job, job.get("centroids.dir"));
    }

    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        List<double[]> cl = new ArrayList<double[]>(); // lat, lon, n, S_i, sumS
        while (values.hasNext()) {
            String[] p = values.next().toString().split(",");
            double[] c = cent.get(Long.parseLong(p[0]));
            double n = Double.parseDouble(p[1]);
            cl.add(new double[] {c[0], c[1], n, Double.parseDouble(p[2]) / n, Double.parseDouble(p[3])});
            output.collect(new Text("cluster"), new Text(String.format(Locale.US, "%s,%.6f,%.6f,%d,%.4f,%.4f",
                    p[0], c[0], c[1], (long) n, Double.parseDouble(p[2]) / n, Double.parseDouble(p[3]) / n)));
        }
        int k = cl.size();
        double totalN = 0, totalS = 0;
        for (double[] c : cl) {
            totalN += c[2];
            totalS += c[4];
        }
        String db = "NA";
        if (k >= 2) {
            double suma = 0;
            for (int i = 0; i < k; i++) {
                double max = 0;
                for (int j = 0; j < k; j++) {
                    if (i == j) {
                        continue;
                    }
                    double m = Grid.distKm(cl.get(i)[0], cl.get(i)[1], cl.get(j)[0], cl.get(j)[1]);
                    if (m > 0) {
                        max = Math.max(max, (cl.get(i)[3] + cl.get(j)[3]) / m);
                    }
                }
                suma += max;
            }
            db = String.format(Locale.US, "%.6f", suma / k);
        }
        output.collect(new Text("k"), new Text(Integer.toString(k)));
        output.collect(new Text("davies_bouldin"), new Text(db));
        output.collect(new Text("silueta_simplificada"), new Text(String.format(Locale.US, "%.6f", totalN > 0 ? totalS / totalN : 0.0)));
        output.collect(new Text("puntos_en_clusters"), new Text(Long.toString((long) totalN)));
    }
}
