package techo;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import common.Detection;
import common.Grid;
import common.Sites;
import common.Sums;

/** Por sitio (cluster de la C3), sobre TODAS las filas: [n, sumExceso, sumExceso^2, graves]. Reducer: SumsReducer. */
public class TechoMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

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
        if (d == null) {
            return;
        }
        Long label = cells.get(Grid.cellId(d.lat, d.lon, size));
        if (label == null) {
            reporter.incrCounter("cluster", "puntos_sin_sitio", 1);
            return;
        }
        reporter.incrCounter("cluster", "puntos_con_sitio", 1);
        output.collect(new Text(label.toString()), Sums.of(1, d.exceso, (double) d.exceso * d.exceso, d.exceso > umbral ? 1 : 0));
    }
}
