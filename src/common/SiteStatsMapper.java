package common;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

/**
 * Estadisticas por sitio (cluster de la C3) calculadas SOLO con las filas de codificacion
 * (NRO_DETECCION % 5 == 1). Valor: [1, es_grave, exceso]. Reducer/Combiner: SumsReducer.
 * Esas filas no entrenan ni prueban, asi que el modelo nunca ve la etiqueta de las filas con
 * las que se evalua (ni de las que se entrena) a traves de esta variable.
 */
public class SiteStatsMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

    private double size;
    private double umbral;
    private Map<Long, Long> cells;

    @Override
    public void configure(JobConf job) {
        size = Double.parseDouble(job.get("cell.size", "0.002"));
        umbral = Double.parseDouble(job.get("umbral.exceso", "30"));
        cells = Sites.loadCells(job, job.get("cells.dir"));
    }

    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        Detection d = Detection.parse(value.toString());
        if (d == null || !d.esCodificacion()) {
            return;
        }
        Long label = cells.get(Grid.cellId(d.lat, d.lon, size));
        if (label == null) {
            reporter.incrCounter("cluster", "codificacion_sin_sitio", 1);
            return;
        }
        output.collect(new Text(label.toString()), Sums.of(1, d.exceso > umbral ? 1 : 0, d.exceso));
    }
}
