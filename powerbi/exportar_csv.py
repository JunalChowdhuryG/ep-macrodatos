#!/usr/bin/env python3
"""Convierte las salidas de Hadoop (TSV / consolas / resumen.csv) en CSV limpios para Power BI.
Uso (en el master, desde la raiz del repo, despues de traer_salidas.sh):  python3 powerbi/exportar_csv.py
Lee   salidas/*.tsv, resultados/<modo>/c4_r1.consola.txt, c5_r1.consola.txt, resumen.csv
Escribe powerbi/datos/*.csv  (punto decimal, UTF-8, encabezados). Solo libreria estandar."""
import collections, csv, glob, os, random, re, sys

SAL, RES, OUT = "salidas", "resultados", "powerbi/datos"
BASE, PAR = "worker1", "cluster2"          # speedup = T(BASE) / T(PAR)
os.makedirs(OUT, exist_ok=True)
resumen = []

def lineas(ruta):
    if not os.path.exists(ruta):
        print("  (falta %s: se omite lo que dependa de el)" % ruta)
        return None
    return [l.rstrip("\n") for l in open(ruta, encoding="utf-8") if l.strip()]

def escribir(nombre, cab, filas):
    with open(os.path.join(OUT, nombre), "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(cab)
        w.writerows(filas)
    resumen.append((nombre, len(filas)))

def num(x):
    return "" if x in ("NA", "") else x

# ---------------- KPIs globales (Job 0) ----------------
l = lineas(os.path.join(SAL, "job0.tsv"))
if l:
    st = {}
    for x in l:
        k, v = x.split("\t")
        st[k] = [float(t) for t in v.split(",")]       # n, media, desv, min, max
    escribir("kpis.csv", ["kpi", "valor"], [
        ("filas_validas", int(st["exceso"][0])), ("exceso_medio_kmh", st["exceso"][1]),
        ("exceso_desv_kmh", st["exceso"][2]), ("exceso_max_kmh", st["exceso"][4]),
        ("limite_medio_kmh", st["limite"][1]), ("pct_exceso_mayor_20", 100 * st["p_exceso_gt20"][1]),
        ("pct_exceso_mayor_30", 100 * st["p_exceso_gt30"][1]), ("pct_exceso_mayor_40", 100 * st["p_exceso_gt40"][1])])

# ---------------- C3: clusters y puntos ----------------
met, pts = lineas(os.path.join(SAL, "c3_metricas.tsv")), lineas(os.path.join(SAL, "c3_puntos.tsv"))
if met and pts:
    cl, glob_m = {}, {}
    for x in met:
        k, v = x.split("\t")
        if k == "cluster":
            p = v.split(",")
            cl[p[0]] = dict(lat=float(p[1]), lon=float(p[2]), n=int(p[3]), disp=float(p[4]), sil=float(p[5]))
        else:
            glob_m[k] = v
    orden = sorted(cl, key=lambda L: -cl[L]["n"])
    cid = {L: i + 1 for i, L in enumerate(orden)}       # 1 = el sitio con mas detecciones
    cid["-1"] = 0                                       # 0 = ruido
    carr, graves, sumexc, cnt = collections.defaultdict(collections.Counter), collections.Counter(), collections.Counter(), collections.Counter()
    filas, por_cluster = [], collections.defaultdict(list)
    for x in pts:
        p = x.split("\t")
        if len(p) != 5:
            continue
        L, lat, lon, exc, car = p
        e = int(exc)
        filas.append((cid[L], lat, lon, e, 1 if e > 30 else 0, car))
        por_cluster[cid[L]].append(len(filas) - 1)
        carr[L][car] += 1
        cnt[L] += 1
        graves[L] += e > 30
        sumexc[L] += e
    escribir("puntos.csv", ["cluster_id", "lat", "lon", "exceso_kmh", "es_grave", "carretera"], filas)
    rnd = random.Random(1)                              # muestra reproducible para los mapas de puntos
    muestra = []
    for c, idx in por_cluster.items():
        muestra += [filas[i] for i in rnd.sample(idx, min(len(idx), 1000 if c == 0 else 40))]
    escribir("puntos_muestra.csv", ["cluster_id", "lat", "lon", "exceso_kmh", "es_grave", "carretera"], muestra)
    escribir("clusters.csv",
             ["cluster_id", "nombre", "lat", "lon", "n", "dispersion_km", "silueta_media", "carretera_dominante", "pct_graves", "exceso_medio_kmh"],
             [(cid[L], "Sitio %03d" % cid[L], cl[L]["lat"], cl[L]["lon"], cl[L]["n"], cl[L]["disp"], cl[L]["sil"],
               carr[L].most_common(1)[0][0] if carr[L] else "", round(100.0 * graves[L] / cnt[L], 3) if cnt[L] else "",
               round(sumexc[L] / cnt[L], 3) if cnt[L] else "") for L in orden])
    ruido = len(por_cluster.get(0, []))
    escribir("cluster_metricas.csv", ["metrica", "valor"],
             [("k", glob_m.get("k", "")), ("davies_bouldin", glob_m.get("davies_bouldin", "")),
              ("silueta_simplificada", glob_m.get("silueta_simplificada", "")),
              ("puntos_en_clusters", glob_m.get("puntos_en_clusters", "")), ("puntos_ruido", ruido)])

# ---------------- C1: sitios criticos ----------------
l = lineas(os.path.join(SAL, "c1_job3.tsv"))
if l:
    escribir("c1_sitios.csv", ["carretera", "rank", "lat", "lon", "n", "exceso_medio", "pct_sitio", "pct_carretera", "razon"],
             [[x.split("\t")[0]] + x.split("\t")[1].split(",") for x in l])

# ---------------- C2: serie mensual y extremos ----------------
l = lineas(os.path.join(SAL, "c2_job2.tsv"))
if l:
    f = []
    for x in l:
        c, v = x.split("\t")
        m, n, med, va, vp = v.split(",")
        f.append((c, m, m + "-01", n, med, num(va), num(vp)))
    escribir("c2_serie.csv", ["carretera", "mes", "fecha", "n", "exceso_medio", "variacion_abs", "variacion_pct"], f)
l = lineas(os.path.join(SAL, "c2_job3.tsv"))
if l:
    escribir("c2_extremos.csv", ["carretera", "tipo", "mes", "n", "exceso_medio", "variacion_abs", "variacion_pct", "transiciones"],
             [[x.split("\t")[0]] + x.split("\t")[1].split(",") for x in l])

# ---------------- C4 / C5: curvas por epoca y resumen final ----------------
def tabla_epocas(texto):
    cab, filas = None, []
    for x in texto:
        p = x.rstrip("\n").split("\t")
        if p[0] == "epoca":
            cab = [re.sub(r"\s*\(.*$", "", c).strip() for c in p]
        elif cab and len(p) == len(cab) and re.fullmatch(r"\d+", p[0]):
            filas.append(p)
    return cab, filas

def final(modelo, txt, modo):
    g = lambda pat: (re.search(pat, txt) or [None, ""])
    r = []
    if modelo == "c4":
        m = re.search(r"test_loss=([\d.]+) \(linea base, solo la prevalencia: ([\d.]+)\)\s+test_AUC=([\d.]+)", txt)
        if m:
            r += [("test_loss", m[1]), ("test_loss_linea_base", m[2]), ("test_auc", m[3])]
        for nom, pat in (("acc_linea_base", r"LINEA BASE \(siempre la clase mayoritaria\): accuracy=([\d.]+)"),
                         ("f1_linea_base", r"LINEA BASE F1 .*?f1=([\d.]+)")):
            m = re.search(pat, txt)
            if m:
                r.append((nom, m[1]))
        for nom, pat in (("umbral05", r"umbral 0\.5\s*: "), ("umbral_prevalencia", r"umbral=prevalencia: ")):
            m = re.search(pat + r"acc=([\d.]+) precision=([\d.]+) recall=([\d.]+) f1=([\d.]+)", txt)
            if m:
                r += [(nom + "_" + k, m[i + 1]) for i, k in enumerate(("acc", "precision", "recall", "f1"))]
    else:
        m = re.search(r"test_mse_z=([\d.]+)\s+test_rmse=([\d.]+) km/h\s+test_mae=([\d.]+) km/h\s+test_r2=(-?[\d.]+)", txt)
        if m:
            r += [("test_mse_z", m[1]), ("test_rmse_kmh", m[2]), ("test_mae_kmh", m[3]), ("test_r2", m[4])]
        m = re.search(r"LINEA BASE .*?RMSE=([\d.]+)", txt)
        if m:
            r.append(("rmse_linea_base_kmh", m[1]))
    m = re.search(r"tiempo_medio_por_epoca_s=([\d.]+)", txt)
    if m:
        r.append(("tiempo_medio_por_epoca_s", m[1]))
    return [(modelo, modo, k, v) for k, v in r]

ff = []
for modelo in ("c4", "c5"):
    todas = []
    for ruta in sorted(glob.glob(os.path.join(RES, "*", modelo + "_r1.consola.txt"))):
        modo = os.path.basename(os.path.dirname(ruta))
        txt = open(ruta, encoding="utf-8", errors="replace").read()
        cab, filas = tabla_epocas(txt.split("\n"))
        if cab:
            todas += [[modo] + f for f in filas]
            hdr = ["modo"] + cab
        ff += final(modelo, txt, modo)
    if todas:
        escribir("ml_%s_epocas.csv" % modelo, hdr, todas)
t = os.path.join(SAL, "techo_consola.txt")
if os.path.exists(t):
    txt = open(t, encoding="utf-8", errors="replace").read()
    for nom, pat in (("r2_techo", r"R2_techo=([\d.]+)"), ("auc_techo", r"AUC_techo=([\d.]+)")):
        m = re.search(pat, txt)
        if m:
            ff.append(("techo", "todos", nom, m[1]))
if ff:
    escribir("ml_final.csv", ["modelo", "modo", "metrica", "valor"], ff)

# ---------------- Benchmark cluster vs 1 worker ----------------
if os.path.exists("resumen.csv"):
    filas = list(csv.DictReader(open("resumen.csv", encoding="utf-8")))
    escribir("bench_resumen.csv", list(filas[0].keys()), [list(r.values()) for r in filas])
    rec = []
    for r in filas:
        for k in r:
            if k.startswith("cpu_"):
                nodo = k[4:]
                rec.append((r["job"], r["modo"], nodo, r[k], r.get("mem_mb_" + nodo, "")))
    escribir("bench_recursos.csv", ["job", "modo", "nodo", "cpu_pct", "mem_mb"], rec)
    T = {(r["job"], r["modo"]): float(r["tiempo_medio_s"]) for r in filas}
    sp = []
    for j in sorted({r["job"] for r in filas}):
        if (j, BASE) in T and (j, PAR) in T:
            s = T[(j, BASE)] / T[(j, PAR)]
            sp.append((j, T[(j, BASE)], T[(j, PAR)], round(s, 4), round(s / 2, 4)))
    escribir("bench_speedup.csv", ["job", "t_1worker_s", "t_2workers_s", "speedup", "eficiencia"], sp)
else:
    print("  (falta resumen.csv: corre antes medir/resumen.py)")

print("\nCSV escritos en %s/:" % OUT)
for n, c in resumen:
    print("  %-22s %7d filas" % (n, c))
