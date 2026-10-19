package c3;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import common.Detection;

/** C3 - Job F (solo map): cada punto con su cluster (-1 = ruido), para el mapa de Power BI. TSV. */
public class PointsMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

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
            return;
        }
        Long label = labels.get(Grid.cellId(d.lat, d.lon, size));
        output.collect(new Text(label == null ? "-1" : label.toString()),
                new Text(String.format(Locale.US, "%.8f\t%.8f\t%d\t%s", d.lat, d.lon, d.exceso, d.carretera)));
    }
}
