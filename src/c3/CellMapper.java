package c3;

import common.Grid;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import common.Detection;
import common.Sums;

/** C3 - Job A: cada punto cae en una celda. Valor: [1, lat, lon] (se suma luego). */
public class CellMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

    private double size;

    @Override
    public void configure(JobConf job) {
        size = Double.parseDouble(job.get("cell.size", "0.002"));
    }

    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        Detection d = Detection.parse(value.toString());
        if (d == null) {
            reporter.incrCounter("calidad", "filas_invalidas", 1);
            return;
        }
        output.collect(new Text(Long.toString(Grid.cellId(d.lat, d.lon, size))), Sums.of(1, d.lat, d.lon));
    }
}
