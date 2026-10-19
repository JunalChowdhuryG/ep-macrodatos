package c1;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import common.Sums;

/**
 * C1 - Job 2: lee la SALIDA del Job 1 (carretera|lat|lon TAB n,sumPct,...) y la
 * agrega por carretera. Suma n y sumPct de todos sus sitios, de modo que el
 * promedio de la carretera es el promedio real de detecciones (no un promedio
 * de promedios de sitios). El Reducer es common.SumsReducer.
 */
public class Job2Mapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        String[] kv = value.toString().split("\t", 2);
        if (kv.length != 2) {
            reporter.incrCounter("calidad", "job2_lineas_raras", 1);
            return;
        }
        String carretera = kv[0].split("\\|", 2)[0];
        double[] s = Sums.parse(kv[1]); // n, sumPct, sumLat, sumLon, sumExc
        output.collect(new Text(carretera), Sums.of(s[0], s[1]));
    }
}
