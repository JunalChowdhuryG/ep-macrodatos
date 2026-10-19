package job0;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import common.Detection;
import common.Stat;

/**
 * Job 0 - estadisticos globales para normalizar y para decidir el umbral de severidad.
 * Por cada fila valida emite varios pares (nombre_estadistico -> observacion).
 */
public class Job0Mapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        Detection d = Detection.parse(value.toString());
        if (d == null) {
            // Se espera 1 por el encabezado; cualquier otra cosa merece revision
            reporter.incrCounter("calidad", "filas_invalidas", 1);
            return;
        }
        reporter.incrCounter("calidad", "filas_validas", 1);
        if (d.exceso <= 0) {
            reporter.incrCounter("calidad", "exceso_no_positivo", 1);
        }

        emit(output, "lat", d.lat);
        emit(output, "lon", d.lon);
        emit(output, "limite", d.limite);
        emit(output, "exceso", d.exceso);
        emit(output, "pct_exceso", d.pctExceso);

        // Para un 0/1 el promedio es la PROPORCION de filas que cumplen la condicion
        emit(output, "p_exceso_gt20", d.exceso > 20 ? 1 : 0);
        emit(output, "p_exceso_gt30", d.exceso > 30 ? 1 : 0);
        emit(output, "p_exceso_gt40", d.exceso > 40 ? 1 : 0);
        emit(output, "p_prueba", d.esPrueba() ? 1 : 0);
    }

    private static void emit(OutputCollector<Text, Text> out, String nombre, double x) throws IOException {
        out.collect(new Text(nombre), Stat.of(x));
    }
}
