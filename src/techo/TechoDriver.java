package techo;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import common.HdfsText;
import common.Sums;
import common.SumsReducer;

/**
 * TECHO de lo que se puede explicar conociendo SOLO el sitio (cluster de la C3).
 * Si cada punto se predijera con la media real de su sitio:
 *   R2_techo = varianza entre sitios / varianza total   (correlacion al cuadrado, "eta^2")
 *   AUC_techo = AUC de puntuar cada punto con la tasa real de graves de su sitio
 * Se calcula con TODAS las filas (en muestra): es un techo ligeramente optimista, no un resultado de prueba.
 * Uso: hadoop jar dist/techo.jar techo.TechoDriver <entrada> <base> <dirCeldasC3> <cellSize>
 */
public class TechoDriver {

    public static void main(String[] a) throws Exception {
        if (a.length < 4) {
            System.err.println("Uso: TechoDriver <entrada> <base> <dirCeldasC3> <cellSize>");
            System.exit(1);
        }
        JobConf c = new JobConf(TechoDriver.class);
        c.setJobName("Techo_por_sitio");
        c.setOutputKeyClass(Text.class);
        c.setOutputValueClass(Text.class);
        c.setMapperClass(TechoMapper.class);
        c.setReducerClass(SumsReducer.class);
        c.setCombinerClass(SumsReducer.class);
        c.setNumReduceTasks(1);
        c.setInputFormat(TextInputFormat.class);
        c.setOutputFormat(TextOutputFormat.class);
        c.set("cell.size", a[3]);
        c.set("cells.dir", a[2]);
        FileInputFormat.setInputPaths(c, new Path(a[0]));
        FileOutputFormat.setOutputPath(c, new Path(a[1]));
        RunningJob rj = JobClient.runJob(c);
        long sinSitio = rj.getCounters().getGroup("cluster").getCounter("puntos_sin_sitio");

        final List<double[]> sit = new ArrayList<double[]>(); // n, sumE, sumE2, graves
        for (String l : HdfsText.read(new Configuration(), a[1])) {
            sit.add(Sums.parse(l.split("\t")[1]));
        }
        double n = 0, se = 0, se2 = 0, g = 0;
        for (double[] s : sit) {
            n += s[0];
            se += s[1];
            se2 += s[2];
            g += s[3];
        }
        double media = se / n, sst = se2 - n * media * media, ssb = 0;
        double p = g / n, ssbG = 0;
        for (double[] s : sit) {
            ssb += s[0] * Math.pow(s[1] / s[0] - media, 2);
            ssbG += s[0] * Math.pow(s[3] / s[0] - p, 2);
        }
        double eta2 = ssb / sst;

        // AUC con la tasa de graves de cada sitio como puntuacion (empates: 1/2)
        Collections.sort(sit, new Comparator<double[]>() {
            public int compare(double[] x, double[] y) {
                return Double.compare(x[3] / x[0], y[3] / y[0]);
            }
        });
        double pos = g, neg = n - g, num = 0, negDebajo = 0;
        int i = 0;
        while (i < sit.size()) {
            int j = i;
            double gp = 0, gn = 0, r = sit.get(i)[3] / sit.get(i)[0];
            while (j < sit.size() && sit.get(j)[3] / sit.get(j)[0] == r) {
                gp += sit.get(j)[3];
                gn += sit.get(j)[0] - sit.get(j)[3];
                j++;
            }
            num += gp * (negDebajo + 0.5 * gn);
            negDebajo += gn;
            i = j;
        }

        System.out.println("==== TECHO por sitio (solo conociendo el cluster) ====");
        System.out.println(String.format(Locale.US, "sitios=%d  filas_con_sitio=%d  filas_sin_sitio(ruido)=%d", sit.size(), (long) n, sinSitio));
        System.out.println(String.format(Locale.US, "REGRESION: R2_techo=%.4f  (varianza entre sitios = %.1f%% de la total)", eta2, 100 * eta2));
        System.out.println(String.format(Locale.US, "           RMSE_techo=%.3f km/h  vs  RMSE de la media=%.3f km/h", Math.sqrt(sst / n * (1 - eta2)), Math.sqrt(sst / n)));
        System.out.println(String.format(Locale.US, "CLASIFICACION (grave, prevalencia %.2f%%): AUC_techo=%.4f   varianza de la tasa entre sitios=%.1f%% de la total",
                100 * p, num / (pos * neg), 100 * ssbG / (n * p * (1 - p))));
    }
}
