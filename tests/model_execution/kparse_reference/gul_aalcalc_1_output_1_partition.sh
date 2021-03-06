#!/bin/bash

rm -R -f output/*
rm -R -f fifo/*
rm -R -f work/*

mkdir work/kat
mkfifo fifo/gul_P1

mkfifo fifo/gul_S1_summary_P1

mkdir work/gul_S1_summaryaalcalc

# --- Do ground up loss computes ---


tee < fifo/gul_S1_summary_P1 work/gul_S1_summaryaalcalc/P1.bin > /dev/null & pid1=$!
summarycalc -g  -1 fifo/gul_S1_summary_P1 < fifo/gul_P1 &

eve 1 1 | getmodel | gulcalc -S100 -L100 -r -c - > fifo/gul_P1  &

wait $pid1


# --- Do ground up loss kats ---


aalcalc -Kgul_S1_summaryaalcalc > output/gul_S1_aalcalc.csv & lpid1=$!
wait $lpid1

rm fifo/gul_P1

rm fifo/gul_S1_summary_P1

rm -rf work/kat
rm -rf work/gul_S1_summaryaalcalc/*
rmdir work/gul_S1_summaryaalcalc

