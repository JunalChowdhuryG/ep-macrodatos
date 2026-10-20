#!/bin/bash
# Uso: sampler.sh <archivo_salida>  -> 1 muestra por segundo hasta que lo detengan.
# Solo usa /proc (no requiere instalar nada). Cada linea:
#   timestamp  cpu user nice system idle iowait irq softirq steal guest guest_nice  MemTotal_kB MemAvailable_kB
out="$1"
while true; do
  echo "$(date +%s.%N) $(head -1 /proc/stat) $(awk '/^MemTotal|^MemAvailable/{printf "%s ", $2}' /proc/meminfo)" >> "$out"
  sleep 1
done
