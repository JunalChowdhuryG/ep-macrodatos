package common;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;

/** Carga las tablas (pequenas) que unen cada punto con su sitio (cluster de la C3). */
public final class Sites {

    private Sites() {
    }

    /** celda -> cluster. Lineas de la C3: "celdaId TAB label,n,sumLat,sumLon". */
    public static Map<Long, Long> loadCells(Configuration c, String dir) {
        Map<Long, Long> m = new HashMap<Long, Long>();
        try {
            for (String l : HdfsText.read(c, dir)) {
                String[] kv = l.split("\t");
                m.put(Long.parseLong(kv[0]), Long.parseLong(kv[1].split(",")[0]));
            }
        } catch (IOException e) {
            throw new RuntimeException("No se pudo leer " + dir, e);
        }
        return m;
    }

    /** cluster -> {n, graves, sumExceso}. Lineas: "label TAB n,graves,sumExceso". */
    public static Map<Long, double[]> loadTable(Configuration c, String dir) {
        Map<Long, double[]> m = new HashMap<Long, double[]>();
        try {
            for (String l : HdfsText.read(c, dir)) {
                String[] kv = l.split("\t");
                m.put(Long.parseLong(kv[0]), Sums.parse(kv[1]));
            }
        } catch (IOException e) {
            throw new RuntimeException("No se pudo leer " + dir, e);
        }
        return m;
    }
}
