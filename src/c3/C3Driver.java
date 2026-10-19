package c3;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import common.SumsReducer;

/**
 * C3 - Agrupamiento por DENSIDAD sobre una cuadricula (aproximacion tipo DBSCAN, NO es
 * DBSCAN exacto), 100% MapReduce. Distinto de K-means.
 *
 *   Job A     : puntos -> celdas; se conservan las celdas con >= minPts puntos (el resto es ruido)
 *   Jobs B1..n: propagacion de etiquetas entre celdas vecinas (8 vecinas) hasta que ninguna cambie
 *   Job C     : cada punto toma el cluster de su celda; suma -> centroides y tamanos
 *   Job D     : silueta simplificada por punto (distancia al centroide propio vs al otro mas cercano)
 *   Job E     : Davies-Bouldin y silueta global
 *   Job F     : (solo map) puntos etiquetados para Power BI
 *
 * Uso: hadoop jar dist/c3.jar c3.C3Driver <entrada> <base> [cellSize=0.002] [minPts=20] [maxIter=60]
 */
public class C3Driver {

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Uso: C3Driver <entrada> <base> [cellSize=0.002] [minPts=20] [maxIter=60]");
            System.exit(1);
        }
        String in = args[0];
        String base = args[1];
        String cellSize = args.length > 2 ? args[2] : "0.002";
        int minPts = args.length > 3 ? Integer.parseInt(args[3]) : 20;
        int maxIter = args.length > 4 ? Integer.parseInt(args[4]) : 60;

        long t0 = System.nanoTime();
        int jobs = 0;

        // ---- Job A: celdas densas ----
        JobConf a = conf("C3_A_celdas", CellMapper.class, CellReducer.class);
        a.setCombinerClass(SumsReducer.class);
        a.setNumReduceTasks(2);
        a.set("cell.size", cellSize);
        a.setInt("min.pts", minPts);
        FileInputFormat.setInputPaths(a, new Path(in));
        FileOutputFormat.setOutputPath(a, new Path(base + "/cells"));
        RunningJob ra = run(a);
        jobs++;
        long densas = ra.getCounters().getGroup("cluster").getCounter("celdas_densas");
        long ruido = ra.getCounters().getGroup("cluster").getCounter("celdas_ruido");

        // ---- Jobs B: propagacion hasta converger ----
        String prev = base + "/cells";
        int iter = 0;
        long cambios = -1;
        while (iter < maxIter && cambios != 0) {
            iter++;
            String out = base + "/prop_" + iter;
            JobConf b = conf("C3_B_prop_" + iter, PropMapper.class, PropReducer.class);
            b.setNumReduceTasks(1);
            FileInputFormat.setInputPaths(b, new Path(prev));
            FileOutputFormat.setOutputPath(b, new Path(out));
            cambios = run(b).getCounters().getGroup("cluster").getCounter("cambios");
            jobs++;
            System.out.println(">>> iteracion " + iter + ": cambios=" + cambios);
            prev = out;
        }
        if (cambios != 0) {
            System.err.println("AVISO: no convergio en " + maxIter + " iteraciones (cluster 'encadenado'); "
                    + "usa celdas mas chicas o aumenta maxIter.");
        }
        String finalCells = prev;

        // ---- Job C: centroides ----
        JobConf c = conf("C3_C_centroides", AssignMapper.class, SumsReducer.class);
        c.setCombinerClass(SumsReducer.class);
        c.setNumReduceTasks(1);
        c.set("cell.size", cellSize);
        c.set("cells.dir", finalCells);
        FileInputFormat.setInputPaths(c, new Path(in));
        FileOutputFormat.setOutputPath(c, new Path(base + "/centroids"));
        RunningJob rc = run(c);
        jobs++;
        long enCluster = rc.getCounters().getGroup("cluster").getCounter("puntos_en_cluster");
        long puntosRuido = rc.getCounters().getGroup("cluster").getCounter("puntos_ruido");

        // ---- Job D: silueta simplificada ----
        JobConf d = conf("C3_D_silueta", MetricMapper.class, SumsReducer.class);
        d.setCombinerClass(SumsReducer.class);
        d.setNumReduceTasks(1);
        d.set("cell.size", cellSize);
        d.set("cells.dir", finalCells);
        d.set("centroids.dir", base + "/centroids");
        FileInputFormat.setInputPaths(d, new Path(in));
        FileOutputFormat.setOutputPath(d, new Path(base + "/silueta"));
        run(d);
        jobs++;

        // ---- Job E: Davies-Bouldin y silueta global ----
        JobConf e = conf("C3_E_metricas", EvalMapper.class, EvalReducer.class);
        e.setNumReduceTasks(1);
        e.set("centroids.dir", base + "/centroids");
        FileInputFormat.setInputPaths(e, new Path(base + "/silueta"));
        FileOutputFormat.setOutputPath(e, new Path(base + "/metricas"));
        run(e);
        jobs++;

        // ---- Job F: puntos etiquetados (solo map) ----
        JobConf f = conf("C3_F_puntos", PointsMapper.class, org.apache.hadoop.mapred.lib.IdentityReducer.class);
        f.setNumReduceTasks(0);
        f.set("cell.size", cellSize);
        f.set("cells.dir", finalCells);
        FileInputFormat.setInputPaths(f, new Path(in));
        FileOutputFormat.setOutputPath(f, new Path(base + "/puntos"));
        run(f);
        jobs++;

        double total = (System.nanoTime() - t0) / 1e9;
        System.out.println("==== Resumen C3 ====");
        System.out.println("cellSize=" + cellSize + " minPts=" + minPts);
        System.out.println("celdas_densas=" + densas + " celdas_ruido=" + ruido);
        System.out.println("iteraciones_propagacion=" + iter + " convergio=" + (cambios == 0));
        System.out.println("puntos_en_cluster=" + enCluster + " puntos_ruido=" + puntosRuido);
        System.out.println(String.format(java.util.Locale.US, "jobs=%d total_s=%.1f", jobs, total));
        System.out.println("Metricas : " + base + "/metricas/part-00000");
        System.out.println("Clusters : " + base + "/metricas/part-00000 (lineas 'cluster')");
        System.out.println("Puntos   : " + base + "/puntos  (TSV: cluster, lat, lon, exceso, carretera)");
    }

    private static JobConf conf(String nombre, Class<? extends Mapper> m, Class<? extends Reducer> r) {
        JobConf c = new JobConf(C3Driver.class);
        c.setJobName(nombre);
        c.setOutputKeyClass(Text.class);
        c.setOutputValueClass(Text.class);
        c.setMapperClass(m);
        c.setReducerClass(r);
        c.setInputFormat(TextInputFormat.class);
        c.setOutputFormat(TextOutputFormat.class);
        return c;
    }

    private static RunningJob run(JobConf c) throws java.io.IOException {
        return JobClient.runJob(c); // si falla lanza excepcion: NO se atrapa
    }
}
