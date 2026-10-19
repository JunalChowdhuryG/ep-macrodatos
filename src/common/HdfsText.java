package common;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/** Lee/escribe texto pequeno en HDFS (salida de un job anterior, pesos del modelo). */
public final class HdfsText {

    private HdfsText() {
    }

    /** Lee un archivo, o todos los part-* de un directorio (ignora _SUCCESS y ocultos). */
    public static List<String> read(Configuration conf, String ruta) throws IOException {
        Path p = new Path(ruta);
        FileSystem fs = p.getFileSystem(conf);
        List<String> out = new ArrayList<String>();
        FileStatus[] archivos = fs.isDirectory(p) ? fs.listStatus(p) : new FileStatus[] {fs.getFileStatus(p)};
        for (FileStatus st : archivos) {
            String nombre = st.getPath().getName();
            if (st.isDirectory() || nombre.startsWith("_") || nombre.startsWith(".")) {
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
    }

    public static void write(Configuration conf, String ruta, String contenido) throws IOException {
        Path p = new Path(ruta);
        FileSystem fs = p.getFileSystem(conf);
        Writer w = new OutputStreamWriter(fs.create(p, true), "UTF-8");
        try {
            w.write(contenido);
        } finally {
            w.close();
        }
    }
}
