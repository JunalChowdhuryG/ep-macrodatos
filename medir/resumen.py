#!/usr/bin/env python3
"""Resume resultados/*/tiempos.csv y los logs de CPU/memoria por nodo.
Uso: python3 medir/resumen.py [--base worker1] [--par cluster2] [--ratio 2]
  speedup = T(base)/T(par);  eficiencia = speedup / ratio   (ratio = cuantas veces mas workers tiene 'par')
Excluye el trabajo 'warmup' y las corridas con rc != 0. Solo usa la libreria estandar."""
import csv, glob, os, statistics as st, sys

def arg(nombre, defecto):
    return sys.argv[sys.argv.index(nombre) + 1] if nombre in sys.argv else defecto

BASE, PAR, RATIO = arg("--base", "worker1"), arg("--par", "cluster2"), float(arg("--ratio", "2"))

def muestras(ruta):
    out = []
    for l in open(ruta):
        p = l.split()
        if len(p) >= 14 and p[1] == "cpu":
            try:
                out.append((float(p[0]), [float(x) for x in p[2:10]], float(p[12]), float(p[13])))
            except ValueError:
                pass
    return out

def uso(ruta, t0, t1):
    """(cpu% promedio, memoria usada MB promedio, memoria usada % promedio) dentro de [t0, t1]."""
    m = [x for x in muestras(ruta) if t0 <= x[0] <= t1]
    cpu, mem, pct = [], [], []
    for a, b in zip(m, m[1:]):
        tot = sum(b[1]) - sum(a[1])
        idle = (b[1][3] + b[1][4]) - (a[1][3] + a[1][4])          # idle + iowait
        if tot > 0:
            cpu.append(100.0 * (1 - idle / tot))
    for x in m:
        usada = (x[2] - x[3]) / 1024.0
        mem.append(usada)
        pct.append(100.0 * (x[2] - x[3]) / x[2])
    f = lambda v: st.mean(v) if v else float("nan")
    return f(cpu), f(mem), f(pct)

filas, aviso = [], []
for ruta in sorted(glob.glob("resultados/*/tiempos.csv")):
    d = os.path.dirname(ruta)
    for r in csv.DictReader(open(ruta)):
        if r["job"] == "warmup":
            continue
        if r["rc"] != "0":
            aviso.append("%s %s rep%s rc=%s" % (r["modo"], r["job"], r["rep"], r["rc"]))
            continue
        t0, t1 = float(r["t0"]), float(r["t1"])
        rid = "%s_r%s" % (r["job"], r["rep"])
        fila = {"job": r["job"], "modo": r["modo"], "rep": int(r["rep"]), "tiempo_s": float(r["tiempo_s"])}
        for lg in sorted(glob.glob("%s/%s.*.log" % (d, rid))):
            nodo = os.path.basename(lg)[len(rid) + 1:-4]
            if os.path.getsize(lg) > 0:
                c, m, p = uso(lg, t0, t1)
                fila["cpu_" + nodo], fila["mem_mb_" + nodo], fila["mem_pct_" + nodo] = c, m, p
        filas.append(fila)

if not filas:
    sys.exit("No hay corridas validas en resultados/*/tiempos.csv")

cols = sorted({k for f in filas for k in f})
with open("resumen_por_corrida.csv", "w", newline="") as fh:
    w = csv.DictWriter(fh, fieldnames=cols)
    w.writeheader()
    w.writerows(filas)

grupos = {}
for f in filas:
    grupos.setdefault((f["job"], f["modo"]), []).append(f)
nodos = sorted({k[4:] for f in filas for k in f if k.startswith("cpu_")})
agg = []
for (job, modo), fs in sorted(grupos.items()):
    t = [f["tiempo_s"] for f in fs]
    a = {"job": job, "modo": modo, "n": len(fs), "tiempo_medio_s": st.mean(t),
         "desv_s": st.stdev(t) if len(t) > 1 else 0.0, "min_s": min(t), "max_s": max(t)}
    for n in nodos:
        for pref in ("cpu_", "mem_mb_"):
            v = [f[pref + n] for f in fs if pref + n in f and f[pref + n] == f[pref + n]]
            a[pref + n] = st.mean(v) if v else float("nan")
    agg.append(a)
cols = ["job", "modo", "n", "tiempo_medio_s", "desv_s", "min_s", "max_s"] + ["%s%s" % (p, n) for n in nodos for p in ("cpu_", "mem_mb_")]
with open("resumen.csv", "w", newline="") as fh:
    w = csv.DictWriter(fh, fieldnames=cols)
    w.writeheader()
    w.writerows(agg)

print("%-8s %-10s %2s %10s %8s" % ("job", "modo", "n", "tiempo_s", "desv"))
for a in agg:
    print("%-8s %-10s %2d %10.1f %8.1f   " % (a["job"], a["modo"], a["n"], a["tiempo_medio_s"], a["desv_s"]) +
          "  ".join("%s: cpu %.0f%% mem %.0fMB" % (n, a["cpu_" + n], a["mem_mb_" + n]) for n in nodos
                    if a.get("cpu_" + n) == a.get("cpu_" + n)))
T = {(a["job"], a["modo"]): a for a in agg}
print("\nSPEEDUP = T(%s) / T(%s)   EFICIENCIA = speedup / %g" % (BASE, PAR, RATIO))
for job in sorted({a["job"] for a in agg}):
    if (job, BASE) in T and (job, PAR) in T:
        s = T[(job, BASE)]["tiempo_medio_s"] / T[(job, PAR)]["tiempo_medio_s"]
        print("%-8s speedup=%.2f  eficiencia=%.0f%%" % (job, s, 100 * s / RATIO))
for x in aviso:
    print("AVISO corrida excluida:", x)
print("\nescritos: resumen.csv, resumen_por_corrida.csv")
