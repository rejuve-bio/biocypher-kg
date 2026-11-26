#!/usr/bin/env python3
"""Build the fly gene-id map for lift_enhancer_atlas.py from the ids that a first lift pass held out.

Input : HELD   gene_id_held.tsv of a lift run with GENE_IDS and no map (first column = old id, second = pairs when present)
        TABLE  FlyBase fbgn_annotation_ID_*.tsv(.gz) (columns: symbol, organism, primary_FBgn#, secondary_FBgn#(s), annotation_ID, ...)
        IDS    one current gene id per line (the ids of the annotation the graph uses, e.g. the Ensembl dmel GTF)
Rule  : an old id is mapped when it is a secondary id of exactly one primary id and that primary is in IDS (a split gene with exactly one primary in IDS is also mapped).
        Everything else stays held, with the reason: not in the table, primary absent from IDS, several primaries in IDS.
Output: MAP_OUT   old_id, new_id, symbol, pairs, relation      (GENE_ID_MAP of the lift)
        NOTES_OUT old_id, reason, flybase_candidate_primaries, flybase_gene_report ('not checked'; edit by hand what you verify)   (GENE_ID_NOTES of the lift)
Env   : HELD, TABLE, IDS, MAP_OUT, NOTES_OUT.   Read-only apart from the two outputs.
"""
import csv, gzip, os, sys
from collections import Counter, defaultdict

HELD, TABLE, IDS = os.environ.get("HELD", ""), os.environ.get("TABLE", ""), os.environ.get("IDS", "")
MAP_OUT, NOTES_OUT = os.environ.get("MAP_OUT", ""), os.environ.get("NOTES_OUT", "")
if not (HELD and TABLE and IDS and MAP_OUT and NOTES_OUT):
    sys.exit("set HELD= TABLE= IDS= MAP_OUT= NOTES_OUT=")
current = {l.split()[0] for l in open(IDS) if l.strip() and not l.startswith("#")}
opener = gzip.open if TABLE.endswith(".gz") else open
primary, secondary = {}, defaultdict(list)
with opener(TABLE, "rt") as f:
    for line in f:
        if line.startswith("#") or not line.strip():
            continue
        p = line.rstrip("\n").split("\t")
        primary[p[2]] = p[0]
        for s in filter(None, (p[3] if len(p) > 3 else "").split(",")):
            secondary[s].append(p[2])
rows = []
with open(HELD) as f:
    r = csv.reader(f, delimiter="\t")
    next(r, None)
    for p in r:
        if p and p[0]:
            rows.append((p[0], int(p[1]) if len(p) > 1 and p[1].isdigit() else 0))
mapped, notes, why, pairs = [], [], Counter(), Counter()
for g, n in rows:
    if g in current:
        continue
    prims = secondary.get(g, [])
    new, reason = "", ""
    if not prims:
        reason = "primary id, absent from the ids" if g in primary else "not in the table"
    elif len(prims) == 1:
        if prims[0] in current: new, reason = prims[0], "1 primary, in the ids"
        else: reason = "1 primary, NOT in the ids"
    else:
        inn = [x for x in prims if x in current]
        if len(inn) == 1: new, reason = inn[0], "several primaries, exactly 1 in the ids"
        else: reason = f"several primaries, {len(inn)} in the ids"
    if new:
        mapped.append((g, new, primary.get(new, ""), n, reason))
    else:
        notes.append((g, reason, ",".join(prims), "not checked"))
    key = "mapped: " + reason if new else reason
    why[key] += 1; pairs[key] += n
with open(MAP_OUT, "w") as o:
    o.write("old_id\tnew_id\tsymbol\tpairs\trelation\n")
    for m in sorted(mapped, key=lambda x: -x[3]): o.write("\t".join(map(str, m)) + "\n")
with open(NOTES_OUT, "w") as o:
    o.write("old_id\treason\tflybase_candidate_primaries\tflybase_gene_report\n")
    for m in notes: o.write("\t".join(m) + "\n")
print(f"held ids read: {len(rows)} | mapped: {len(mapped)} | still held: {len(notes)}")
for k in sorted(why, key=lambda k: -pairs[k]): print(f"  {k:48s} ids {why[k]:4d} pairs {pairs[k]:>6,}")
