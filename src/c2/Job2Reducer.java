package c2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

/**
 * C2 - Job 2 (reducer): serie mensual de UNA carretera y su variacion mes a mes.
 * Este paso necesita ORDEN (comparar cada mes con el anterior), por eso se
 * buffera la lista de meses de la carretera (son ~25) y se ordena en memoria.
 *
 * Reglas:
 *  - se descartan meses con menos de min.n detecciones (un promedio de pocas filas es ruido)
 *  - la variacion solo se calcula si el mes ANTERIOR existe en el calendario y no fue
 *    descartado; si hubo hueco, la variacion es "NA" (no se compara contra 2 meses atras)
 * Salida: carretera TAB mes,n,exceso_medio,variacion_abs,variacion_pct   (NA si no aplica)
 */
public class Job2Reducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

    private int minN;

    @Override
    public void configure(JobConf job) {
        minN = job.getInt("min.n", 30);
    }

    public void reduce(Text carretera, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        List<String[]> meses = new ArrayList<String[]>(); // [yyyy-MM, n, sumExc]
        while (values.hasNext()) {
            String[] p = values.next().toString().split(",");
            if (Double.parseDouble(p[1]) < minN) {
                reporter.incrCounter("calidad", "meses_descartados_n_bajo", 1);
                continue;
            }
            meses.add(p);
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
