package common;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;


public final class Detection {
    public static final char SEP = ';';
    public static final int N_CAMPOS = 9;

    
    private static final double LAT_MIN = -19.0, LAT_MAX = 0.0;
    private static final double LON_MIN = -82.0, LON_MAX = -68.0;
    private static final DateTimeFormatter FECHA = DateTimeFormatter.BASIC_ISO_DATE;

    public final long id;
    public final String region;
    public final String carretera;
    public final double lat;
    public final double lon;
    public final int limite;
    public final int velocidad;
    public final int anio;
    public final int mes;
    public final int diaSemana;     
    public final int exceso;       
    public final double pctExceso;  

    private Detection(long id, String region, String carretera, double lat, double lon,
                      int limite, int velocidad, LocalDate fecha) {
        this.id = id;
        this.region = region;
        this.carretera = carretera;
        this.lat = lat;
        this.lon = lon;
        this.limite = limite;
        this.velocidad = velocidad;
        this.anio = fecha.getYear();
        this.mes = fecha.getMonthValue();
        this.diaSemana = fecha.getDayOfWeek().getValue();
        this.exceso = velocidad - limite;
        this.pctExceso = (double) (velocidad - limite) / limite;
    }

    public static Detection parse(String linea) {
        if (linea == null) {
            return null;
        }
        List<String> f = split(linea);
        if (f.size() != N_CAMPOS) {
            return null;
        }
        try {
            long id = Long.parseLong(f.get(0).trim());
            LocalDate fecha = LocalDate.parse(f.get(1).trim(), FECHA);
            String region = f.get(2).trim();
            String carretera = f.get(3).trim();
            double lat = Double.parseDouble(f.get(4).trim());
            double lon = Double.parseDouble(f.get(5).trim());
            int limite = Integer.parseInt(f.get(6).trim());
            int velocidad = Integer.parseInt(f.get(7).trim());

            if (lat < LAT_MIN || lat > LAT_MAX || lon < LON_MIN || lon > LON_MAX) {
                return null;
            }
            if (limite <= 0 || velocidad <= 0) {
                return null;
            }
            return new Detection(id, region, carretera, lat, lon, limite, velocidad, fecha);
        } catch (RuntimeException e) {
            return null;
        }
    }

    public boolean esPrueba() {
        return id % 5 == 0;
    }

    public String sitio() {
        return String.format(java.util.Locale.US, "%.3f|%.3f", lat, lon);
    }


    static List<String> split(String linea) {
        List<String> out = new ArrayList<String>();
        StringBuilder sb = new StringBuilder();
        boolean enComillas = false;
        for (int i = 0; i < linea.length(); i++) {
            char c = linea.charAt(i);
            if (c == '"') {
                if (enComillas && i + 1 < linea.length() && linea.charAt(i + 1) == '"') {
                    sb.append('"');
                    i++;
                } else {
                    enComillas = !enComillas;
                }
            } else if (c == SEP && !enComillas) {
                out.add(sb.toString());
                sb.setLength(0);
            } else {
                sb.append(c);
            }
        }
        out.add(sb.toString());
        return out;
    }
}
