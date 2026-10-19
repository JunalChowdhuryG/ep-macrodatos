package c1;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import common.Detection;
import common.Sums;

/**
 * C1 - Job 1: estadisticos por (carretera, sitio).
 * Sitio = coordenadas redondeadas a 3 decimales (~110 m): aproxima un cinemometro.
 * Valor: [n, sumPctExceso, sumLat, sumLon, sumExceso]
 */
public class Job1Mapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        Detection d = Detection.parse(value.toString());
        if (d == null) {
            reporter.incrCounter("calidad", "filas_invalidas", 1);
            return;
        }
        String llave = d.carretera + "|" + d.sitio();
        output.collect(new Text(llave), Sums.of(1, d.pctExceso, d.lat, d.lon, d.exceso));
    }
}
