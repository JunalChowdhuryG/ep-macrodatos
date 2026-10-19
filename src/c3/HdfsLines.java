package c3;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;

/**
 * Lee la salida (pequena) de un job anterior para usarla en memoria dentro de otro
 * (join "broadcast"): cada tarea carga la tabla de celdas/centroides en configure().
 */
public final class HdfsLines {

    private HdfsLines() {
    }

    public static List<String> read(JobConf job, String dir) {
        try {
            Path p = new Path(dir);
            FileSystem fs = p.getFileSystem(job);
            List<String> out = new ArrayList<String>();
            for (FileStatus st : fs.listStatus(p)) {
                String name = st.getPath().getName();
                if (st.isDirectory() || name.startsWith("_") || name.startsWith(".")) {
                    continue;
                }
                BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(st.getPath()), "UTF-8"));
                try {
                    String l;
                    while ((l = br.readLine()) != null) {
                        out.add(l);
                    }
                } finally {
                    br.close();
                }
            }
            return out;
        } catch (IOException e) {
            throw new RuntimeException("No se pudo leer " + dir, e);
        }
    }

    /** celda -> etiqueta de cluster. Lineas: "celdaId TAB label,n,sumLat,sumLon". */
    public static Map<Long, Long> cellLabels(JobConf job, String dir) {
        Map<Long, Long> m = new HashMap<Long, Long>();
        for (String l : read(job, dir)) {
            String[] kv = l.split("\t");
            m.put(Long.parseLong(kv[0]), Long.parseLong(kv[1].split(",")[0]));
        }
        return m;
    }

    /** etiqueta -> {lat, lon, n}. Lineas: "label TAB n,sumLat,sumLon". */
    public static Map<Long, double[]> centroids(JobConf job, String dir) {
        Map<Long, double[]> m = new HashMap<Long, double[]>();
        for (String l : read(job, dir)) {
            String[] kv = l.split("\t");
            String[] v = kv[1].split(",");
            double n = Double.parseDouble(v[0]);
            m.put(Long.parseLong(kv[0]), new double[] {Double.parseDouble(v[1]) / n, Double.parseDouble(v[2]) / n, n});
        }
        return m;
    }
}
