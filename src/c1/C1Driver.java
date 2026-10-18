package c1;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import common.SumsReducer;


public class C1Driver {

    private static final int REDUCERS_JOB1 = 2;  

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Uso: C1Driver <entrada> <base_salida> [minN=30] [topK=20]");
            System.exit(1);
        }
        String in = args[0];
        String o1 = args[1] + "/job1";
        String o2 = args[1] + "/job2";
        String o3 = args[1] + "/job3";
        int minN = args.length > 2 ? Integer.parseInt(args[2]) : 30;
        int topK = args.length > 3 ? Integer.parseInt(args[3]) : 20;

        long t0 = System.nanoTime();

        //  Job 1 
        JobConf j1 = base("C1_Job1_sitios", Job1Mapper.class, SumsReducer.class);
        j1.setCombinerClass(SumsReducer.class);
        j1.setNumReduceTasks(REDUCERS_JOB1);
        FileInputFormat.setInputPaths(j1, new Path(in));
        FileOutputFormat.setOutputPath(j1, new Path(o1));
        double s1 = run(j1);

        //  Job 2  (entrada = salida del Job 1)
        JobConf j2 = base("C1_Job2_carretera", Job2Mapper.class, SumsReducer.class);
        j2.setCombinerClass(SumsReducer.class);
        j2.setNumReduceTasks(1);
        FileInputFormat.setInputPaths(j2, new Path(o1));
        FileOutputFormat.setOutputPath(j2, new Path(o2));
        double s2 = run(j2);

        //  Job 3  (join: salida del Job 1 + salida del Job 2)
        JobConf j3 = base("C1_Job3_razon", Job3Mapper.class, Job3Reducer.class);
        j3.setNumReduceTasks(1);
        j3.setInt("min.n", minN);
        j3.setInt("top.k", topK);
        FileInputFormat.setInputPaths(j3, new Path(o1), new Path(o2));
        FileOutputFormat.setOutputPath(j3, new Path(o3));
        double s3 = run(j3);

        double total = (System.nanoTime() - t0) / 1e9;
        System.out.println("==== Resumen C1 ====");
        System.out.println(String.format(java.util.Locale.US, "job1_s=%.1f job2_s=%.1f job3_s=%.1f total_s=%.1f", s1, s2, s3, total));
        System.out.println("Resultado: " + o3 + "/part-00000");
    }

    private static JobConf base(String nombre, Class<? extends Mapper> mapper, Class<? extends Reducer> reducer) {
        JobConf c = new JobConf(C1Driver.class);
        c.setJobName(nombre);
        c.setOutputKeyClass(Text.class);
        c.setOutputValueClass(Text.class);
        c.setMapperClass(mapper);
        c.setReducerClass(reducer);
        c.setInputFormat(TextInputFormat.class);
        c.setOutputFormat(TextOutputFormat.class);
        return c;
    }

    private static double run(JobConf c) throws java.io.IOException {
        long t = System.nanoTime();
        JobClient.runJob(c); // si falla lanza excepcion: NO se atrapa
        return (System.nanoTime() - t) / 1e9;
    }
}
