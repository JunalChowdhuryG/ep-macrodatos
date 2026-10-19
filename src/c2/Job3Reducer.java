package c2;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

/**
 * C2 - Job 3: argmax/argmin de la variacion porcentual de cada carretera.
 * MAYOR_SUBIDA solo se emite si hubo una subida real (>0); MAYOR_BAJADA solo si hubo una
 * bajada real (<0). Asi nunca se llama "subida" a una caida (caso de una unica transicion).
 * Salida: carretera TAB TIPO,mes,n,exceso_medio,variacion_abs,variacion_pct,transiciones
 */
public class Job3Reducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

    public void reduce(Text carretera, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        String mayorSubida = null, mayorBajada = null;
        double maxV = Double.NEGATIVE_INFINITY, minV = Double.POSITIVE_INFINITY;
        int transiciones = 0;
        while (values.hasNext()) {
            String linea = values.next().toString();
            double v = Double.parseDouble(linea.split(",")[4]);
            transiciones++;
            if (v > maxV) {
                maxV = v;
                mayorSubida = linea;
            }
            if (v < minV) {
                minV = v;
                mayorBajada = linea;
            }
        }
        if (mayorSubida != null && maxV > 0) {
            output.collect(carretera, new Text("MAYOR_SUBIDA," + mayorSubida + "," + transiciones));
        }
        if (mayorBajada != null && minV < 0) {
            output.collect(carretera, new Text("MAYOR_BAJADA," + mayorBajada + "," + transiciones));
        }
    }
}
