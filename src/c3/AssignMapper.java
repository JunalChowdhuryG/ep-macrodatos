package c3;

import common.Grid;
import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import common.Detection;
import common.Sums;

/**
 * C3 - Job C: asigna cada punto al cluster de su celda (join broadcast con la tabla de
 * celdas finales) y suma [n, lat, lon] por cluster -> centroides. Puntos en celdas no
 * densas = ruido. Reducer/Combiner: common.SumsReducer.
 */
public class AssignMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

    private double size;
    private Map<Long, Long> labels;

    @Override
    public void configure(JobConf job) {
        size = Double.parseDouble(job.get("cell.size", "0.002"));
        labels = HdfsLines.cellLabels(job, job.get("cells.dir"));
    }

    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        Detection d = Detection.parse(value.toString());
        if (d == null) {
            reporter.incrCounter("calidad", "filas_invalidas", 1);
            return;
        }
        Long label = labels.get(Grid.cellId(d.lat, d.lon, size));
        if (label == null) {
            reporter.incrCounter("cluster", "puntos_ruido", 1);
            return;
        }
        reporter.incrCounter("cluster", "puntos_en_cluster", 1);
        output.collect(new Text(label.toString()), Sums.of(1, d.lat, d.lon));
    }
}
