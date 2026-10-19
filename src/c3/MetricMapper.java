package c3;

import common.Grid;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import common.Detection;
import common.Sums;

/**
 * C3 - Job D: por punto calcula a = distancia a su centroide y b = distancia al centroide
 * mas cercano de OTRO cluster, y la silueta simplificada s = (b - a) / max(a, b).
 * (La silueta exacta es O(n^2) y no es practica con 160 mil puntos; esta es la version
 * "simplificada con centroides"; hay que reportarla como aproximacion.)
 * Valor: [1, a, s] por cluster. Reducer/Combiner: common.SumsReducer.
 */
public class MetricMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

    private double size;
    private Map<Long, Long> labels;
    private long[] ids;
    private double[] clat, clon;
    private Map<Long, Integer> idx = new HashMap<Long, Integer>();

    @Override
    public void configure(JobConf job) {
        size = Double.parseDouble(job.get("cell.size", "0.002"));
        labels = HdfsLines.cellLabels(job, job.get("cells.dir"));
        Map<Long, double[]> cs = HdfsLines.centroids(job, job.get("centroids.dir"));
        int k = cs.size();
        ids = new long[k];
        clat = new double[k];
        clon = new double[k];
        int i = 0;
        for (Map.Entry<Long, double[]> e : cs.entrySet()) {
            ids[i] = e.getKey();
            clat[i] = e.getValue()[0];
            clon[i] = e.getValue()[1];
            idx.put(e.getKey(), i);
            i++;
        }
    }

    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        Detection d = Detection.parse(value.toString());
        if (d == null) {
            return;
        }
        Long label = labels.get(Grid.cellId(d.lat, d.lon, size));
        if (label == null) {
            return; // ruido
        }
        int own = idx.get(label);
        double a = Grid.distKm(d.lat, d.lon, clat[own], clon[own]);
        double b = Double.POSITIVE_INFINITY;
        for (int j = 0; j < ids.length; j++) {
            if (j != own) {
                b = Math.min(b, Grid.distKm(d.lat, d.lon, clat[j], clon[j]));
            }
        }
        double m = Math.max(a, b);
        double s = (ids.length < 2 || m == 0.0) ? 0.0 : (b - a) / m;
        output.collect(new Text(label.toString()), Sums.of(1, a, s));
    }
}
