package common;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;

/**
 * Vector de entrada del MLP (7 variables). La velocidad detectada NUNCA entra: el objetivo se
 * deriva de ella y seria fuga de etiqueta.
 *   x0 latitud, x1 longitud, x2 limite   (z-score con los estadisticos del Job 0)
 *   x3 dia de la semana -> (d-4)/2        x4 mes -> (m-6.5)/3.452
 *   x5 tasa de graves del SITIO  (suavizada, en z)   x6 exceso medio del SITIO (suavizado, en z)
 * El sitio es el cluster de la C3 al que pertenece la celda del punto. Las estadisticas del sitio
 * salen de las filas de codificacion (id%5==1), con suavizado hacia el promedio global:
 *   tasa = (graves + m*p0) / (n + m)     media = (suma + m*mu) / (n + m)      m = site.smooth (10)
 * Un punto sin sitio (ruido) recibe 0,0 (= el promedio global).
 */
public final class Features {

    public static final int N = 7;
    private final double mLat, sLat, mLon, sLon, mLim, sLim, size;
    private final Map<Long, Long> cells;
    private final Map<Long, double[]> sitio = new HashMap<Long, double[]>();

    public Features(Configuration c) {
        mLat = d(c, "st.lat.mean");
        sLat = d(c, "st.lat.std");
        mLon = d(c, "st.lon.mean");
        sLon = d(c, "st.lon.std");
        mLim = d(c, "st.limite.mean");
        sLim = d(c, "st.limite.std");
        size = d(c, "cell.size");
        cells = Sites.loadCells(c, req(c, "cells.dir"));

        double p0 = d(c, "st.p_exceso_gt30.mean");
        double mu = d(c, "st.exceso.mean");
        double sg = d(c, "st.exceso.std");
        double m = Double.parseDouble(c.get("site.smooth", "10"));
        double sdp = Math.sqrt(p0 * (1 - p0));
        for (Map.Entry<Long, double[]> e : Sites.loadTable(c, req(c, "sites.dir")).entrySet()) {
            double n = e.getValue()[0], g = e.getValue()[1], s = e.getValue()[2];
            double tasa = (g + m * p0) / (n + m);
            double media = (s + m * mu) / (n + m);
            sitio.put(e.getKey(), new double[] {(tasa - p0) / sdp, (media - mu) / sg});
        }
    }

    private static String req(Configuration c, String k) {
        String v = c.get(k);
        if (v == null) {
            throw new IllegalStateException("Falta " + k + " en la configuracion del job");
        }
        return v;
    }

    private static double d(Configuration c, String k) {
        return Double.parseDouble(req(c, k));
    }

    public double[] of(Detection x) {
        double[] s = null;
        Long label = cells.get(Grid.cellId(x.lat, x.lon, size));
        if (label != null) {
            s = sitio.get(label);
        }
        return new double[] {
            (x.lat - mLat) / sLat,
            (x.lon - mLon) / sLon,
            (x.limite - mLim) / sLim,
            (x.diaSemana - 4.0) / 2.0,
            (x.mes - 6.5) / 3.452,
            s == null ? 0.0 : s[0],
            s == null ? 0.0 : s[1]
        };
    }
}
