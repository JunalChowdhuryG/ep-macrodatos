package job0;

import java.io.IOException;
import java.util.Iterator;
import java.util.Locale;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import common.Stat;

/** Reducer final. Salida por linea: nombre TAB n,media,desviacion,min,max */
public class Job0Reducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        double[] a = Stat.merge(values);
        double n = a[0];
        double media = a[1] / n;
        double varianza = Math.max(0.0, a[2] / n - media * media); // poblacional
        double desv = Math.sqrt(varianza);

        // Locale.US: punto decimal siempre (en el lab anterior salio "44,60" y complica Power BI)
        String linea = String.format(Locale.US, "%d,%.6f,%.6f,%.6f,%.6f", (long) n, media, desv, a[3], a[4]);
        output.collect(key, new Text(linea));
    }
}
