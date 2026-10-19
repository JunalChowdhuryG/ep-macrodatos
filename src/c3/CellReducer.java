package c3;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import common.Sums;

/**
 * C3 - Job A (reducer): solo las celdas DENSAS (>= min.pts puntos) sobreviven; las demas
 * son ruido. Etiqueta inicial = id de la propia celda.
 * Salida: celdaId TAB label,n,sumLat,sumLon
 */
public class CellReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

    private int minPts;

    @Override
    public void configure(JobConf job) {
        minPts = job.getInt("min.pts", 20);
    }

    public void reduce(Text cell, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        double[] s = Sums.merge(values); // n, sumLat, sumLon
        if (s[0] < minPts) {
            reporter.incrCounter("cluster", "celdas_ruido", 1);
            return;
        }
        reporter.incrCounter("cluster", "celdas_densas", 1);
        output.collect(cell, new Text(cell.toString() + "," + s[0] + "," + s[1] + "," + s[2]));
    }
}
