package common;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;

/**
 * Estadisticos del Job 0 (lineas "nombre TAB n,media,desv,min,max"). El driver los lee de
 * HDFS y los pasa a las tareas por la configuracion del job (st.<nombre>.mean / .std).
 */
public final class Stats {

    private final Map<String, double[]> m = new HashMap<String, double[]>();

    public static Stats load(Configuration conf, String dirJob0) throws IOException {
        Stats s = new Stats();
        for (String l : HdfsText.read(conf, dirJob0)) {
            String[] kv = l.split("\t");
            if (kv.length != 2) {
                continue;
            }
            String[] v = kv[1].split(",");
            double[] a = new double[v.length];
            for (int i = 0; i < v.length; i++) {
                a[i] = Double.parseDouble(v[i]);
            }
            s.m.put(kv[0], a);
        }
        return s;
    }

    private double[] get(String nombre) {
        double[] a = m.get(nombre);
        if (a == null) {
            throw new IllegalStateException("Falta el estadistico '" + nombre + "' en la salida del Job 0");
        }
        return a;
    }

    public double mean(String nombre) {
        return get(nombre)[1];
    }

    public double std(String nombre) {
        return get(nombre)[2];
    }

    public void putInto(Configuration conf) {
        for (Map.Entry<String, double[]> e : m.entrySet()) {
            conf.set("st." + e.getKey() + ".mean", Double.toString(e.getValue()[1]));
            conf.set("st." + e.getKey() + ".std", Double.toString(e.getValue()[2]));
        }
    }
}
