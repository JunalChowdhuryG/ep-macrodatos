package c2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

/**
 * C2 - Job 2 (reducer): serie mensual de UNA carretera y su variacion mes a mes.
 * Necesita ORDEN (comparar cada mes con el anterior), por eso se buferan los meses
 * de la carretera (~25) y se ordenan en memoria.
 *
 * Un mes cuenta solo si n >= max(min.n, min.frac * mediana_de_n_de_la_carretera).
 * El umbral RELATIVO evita comparar contra meses de arranque o parciales (en este
 * dataset: un mes con 45 detecciones frente a ~6000 en regimen normal).
 *
 * La variacion solo se calcula contra el mes calendario INMEDIATO anterior; tras un
 * hueco (mes ausente o descartado) vale "NA".
 * Salida: carretera TAB mes,n,exceso_medio,variacion_abs,variacion_pct   (NA si no aplica)
 */
public class Job2Reducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

    private int minN;
    private double minFrac;

    @Override
    public void configure(JobConf job) {
        minN = job.getInt("min.n", 30);
        minFrac = job.getFloat("min.frac", 0.25f);
    }

    public void reduce(Text carretera, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        List<String[]> todos = new ArrayList<String[]>(); // [yyyy-MM, n, sumExc]
        while (values.hasNext()) {
            todos.add(values.next().toString().split(","));
        }

        // Mediana de n sobre TODOS los meses con datos de esta carretera
        double[] ns = new double[todos.size()];
        for (int i = 0; i < ns.length; i++) {
            ns[i] = Double.parseDouble(todos.get(i)[1]);
        }
        Arrays.sort(ns);
        double mediana = ns.length % 2 == 1 ? ns[ns.length / 2] : (ns[ns.length / 2 - 1] + ns[ns.length / 2]) / 2.0;
        double umbral = Math.max(minN, minFrac * mediana);

        List<String[]> meses = new ArrayList<String[]>();
        for (String[] m : todos) {
            if (Double.parseDouble(m[1]) < umbral) {
                reporter.incrCounter("calidad", "meses_descartados_n_bajo", 1);
            } else {
                meses.add(m);
            }
        }
        Collections.sort(meses, new Comparator<String[]>() {
            public int compare(String[] a, String[] b) {
                return a[0].compareTo(b[0]); // yyyy-MM ordena bien como texto
            }
        });

        double mediaAnt = Double.NaN;
        int idxAnt = Integer.MIN_VALUE;
        for (String[] m : meses) {
            long n = (long) Double.parseDouble(m[1]);
            double media = Double.parseDouble(m[2]) / n;
            int idx = Integer.parseInt(m[0].substring(0, 4)) * 12 + Integer.parseInt(m[0].substring(5, 7)) - 1;

            String varAbs = "NA", varPct = "NA";
            if (idx - idxAnt == 1 && mediaAnt > 0) {
                varAbs = String.format(Locale.US, "%.4f", media - mediaAnt);
                varPct = String.format(Locale.US, "%.4f", (media - mediaAnt) / mediaAnt * 100.0);
            }
            output.collect(carretera, new Text(String.format(Locale.US, "%s,%d,%.4f,%s,%s", m[0], n, media, varAbs, varPct)));
            mediaAnt = media;
            idxAnt = idx;
        }
    }
}
