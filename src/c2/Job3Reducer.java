package c2;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

/**
 * C2 - Job 3: patron argmax/argmin sobre la variacion porcentual de cada carretera.
 * Salida (2 lineas por carretera):  carretera TAB TIPO,mes,n,exceso_medio,variacion_abs,variacion_pct
 */
public class Job3Reducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

    public void reduce(Text carretera, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        String mayorSubida = null, mayorBajada = null;
        double maxV = Double.NEGATIVE_INFINITY, minV = Double.POSITIVE_INFINITY;
        while (values.hasNext()) {
            String linea = values.next().toString();
            double v = Double.parseDouble(linea.split(",")[4]);
            if (v > maxV) {
                maxV = v;
                mayorSubida = linea;
            }
            if (v < minV) {
                minV = v;
                mayorBajada = linea;
            }
        }
        if (mayorSubida != null) {
            output.collect(carretera, new Text("MAYOR_SUBIDA," + mayorSubida));
            output.collect(carretera, new Text("MAYOR_BAJADA," + mayorBajada));
        }
    }
}
