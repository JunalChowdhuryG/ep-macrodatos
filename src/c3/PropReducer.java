package c3;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

/**
 * C3 - propagacion de etiquetas (reducer): nueva etiqueta = minimo entre la propia y las de
 * las vecinas densas. Si la celda no es densa (no llego su "S:") se ignora. Cuenta en el
 * contador cluster/cambios cuantas celdas cambiaron; el Driver itera hasta que sea 0.
 */
public class PropReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

    public void reduce(Text cell, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        String self = null;
        long minVecina = Long.MAX_VALUE;
        while (values.hasNext()) {
            String v = values.next().toString();
            if (v.startsWith("S:")) {
                self = v.substring(2);
            } else {
                minVecina = Math.min(minVecina, Long.parseLong(v.substring(2)));
            }
        }
        if (self == null) {
            return; // vecina no densa
        }
        String[] p = self.split(",");
        long propia = Long.parseLong(p[0]);
        long nueva = Math.min(propia, minVecina);
        if (nueva != propia) {
            reporter.incrCounter("cluster", "cambios", 1);
        }
        output.collect(cell, new Text(nueva + "," + p[1] + "," + p[2] + "," + p[3]));
    }
}
