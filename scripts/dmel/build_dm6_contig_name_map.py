#!/usr/bin/env python3
"""Build an auditable map UCSC dm6 contig -> FlyBase sequence id for the contigs the chromAlias file cannot name.

Contigs "to resolve" = UCSC contigs in chromAlias none of whose aliases is a sequence id (feature or ##FASTA header) of the FlyBase GFF.
Candidates are the GFF/FASTA ids that match no alias ('orphans'). For each contig the script reports evidence from three sources:
  name      an orphan id starting with one of the contig's aliases + '_'
  length    an orphan whose ##FASTA length equals the contig length (from DM6_FASTA, else from the chain headers)
  sequence  an orphan whose uppercase sequence has the same md5 as the contig's sequence in DM6_FASTA (needs DM6_FASTA = UCSC dm6.fa.gz)
Status: verified (identical sequence, unique) | evidence_only (name and length agree, no sequence check) | mismatch (candidates disagree or
sequences differ) | unresolved (no candidate). Only 'verified' (or, if you allow it, 'evidence_only') entries should be used for renaming.
Env: CHROM_ALIAS, GFF (comma-separated), MAP_OUT (required); NODES (nodes_union.bed, only for node counts), DM6_FASTA, CHAIN.
Version 1.0 first release; 1.1 also reports the orphan ids that no contig claims (and, with DM6_FASTA, whether they equal ANY dm6 contig);
1.2 ignores a tar header at the start of the GFF stream and NUL padding at its end (a .gff.gz that really holds a tar archive).
"""
VERSION = "1.2"
import gzip, hashlib, os, sys


class AnySet:
    def __contains__(self, x):
        return True

from collections import Counter, defaultdict

CHROM_ALIAS, MAP_OUT = os.environ.get("CHROM_ALIAS", ""), os.environ.get("MAP_OUT", "")
NODES, DM6_FASTA, CHAIN = os.environ.get("NODES", ""), os.environ.get("DM6_FASTA", ""), os.environ.get("CHAIN", "")
GFF = [g for g in os.environ.get("GFF", "").split(",") if g]
if not (CHROM_ALIAS and GFF and MAP_OUT):
    sys.exit("set CHROM_ALIAS=<dm6.chromAlias.txt> GFF=<FlyBase gff(s)> MAP_OUT=<output tsv> [NODES=] [DM6_FASTA=<dm6.fa.gz>] [CHAIN=]")


def open_text(path):
    with open(path, "rb") as fh:
        magic = fh.read(2)
    return gzip.open(path, "rt") if magic == b"\x1f\x8b" else open(path, "rt", newline="")


def read_fasta(path, wanted=None):
    """yield (id, length, md5 or None); md5 (uppercase sequence) only for ids in `wanted` (None = no md5)"""
    cur, n, h = None, 0, None
    with open_text(path) as f:
        for line in f:
            if line.startswith(">"):
                if cur is not None:
                    yield cur, n, h.hexdigest() if h else None
                cur, n = line[1:].split()[0], 0
                h = hashlib.md5() if (wanted is not None and cur in wanted) else None
            elif cur is not None:
                seq = line.strip()
                n += len(seq)
                if h:
                    h.update(seq.upper().encode())
    if cur is not None:
        yield cur, n, h.hexdigest() if h else None


# ---- aliases
rows = []
with open_text(CHROM_ALIAS) as f:
    for line in f:
        if not line.startswith("#") and line.strip():
            rows.append(line.rstrip("\r\n").split("\t"))
ncols = max(len(r) for r in rows)
cnt = [sum(1 for r in rows if i < len(r) and r[i].startswith("chr")) for i in range(ncols)]
ucol = cnt.index(max(cnt))
aliases = {r[ucol]: [a for i, col in enumerate(r) if i != ucol for a in col.split(",") if a] for r in rows}
alias_all = {a for al in aliases.values() for a in al}

# ---- GFF: feature ids, FASTA ids and lengths
features, gff_len, in_fasta, cur = set(), {}, False, None
skipped_lines = 0
for path in GFF:
    with open_text(path) as f:
        in_fasta = False
        for line in f:
            if in_fasta:
                if line.startswith(">"):
                    cur = line[1:].split()[0]
                    gff_len.setdefault(cur, 0)
                elif cur is not None:
                    gff_len[cur] += len(line.strip().replace("\x00", ""))
                continue
            if line.startswith("##FASTA"):
                in_fasta = True
                continue
            if not line.startswith("#"):
                sid = line.split("\t", 1)[0]
                if sid and " " not in sid and "\x00" not in sid and len(sid) < 200:
                    features.add(sid)
                else:
                    skipped_lines += 1
gff_ids = features | set(gff_len)
todo = sorted(c for c, al in aliases.items() if not any(a in gff_ids for a in al))
orphans = sorted(gff_ids - alias_all)
print(f"aliases for {len(aliases)} UCSC contigs | GFF ids {len(gff_ids)} | contigs to resolve {len(todo)} | orphan GFF ids {len(orphans)}"
      + (f" | non-GFF lines ignored (tar header?): {skipped_lines}" if skipped_lines else ""))

# ---- contig lengths and sequence md5 for the contigs to resolve
clen, cmd5 = {}, {}
if DM6_FASTA:
    for cid, n, h in read_fasta(DM6_FASTA, wanted=set(todo)):
        if cid in set(todo):
            clen[cid], cmd5[cid] = n, h
elif CHAIN:
    with open_text(CHAIN) as f:
        for line in f:
            if line.startswith("chain "):
                p = line.split()
                clen[p[7]] = int(p[8])
else:
    print("neither DM6_FASTA nor CHAIN given: no length/sequence evidence")

omd5 = {}
if DM6_FASTA:
    # md5 of the orphan sequences inside the GFF ##FASTA section
    orphan_set = set(orphans)
    for path in GFF:
        with open_text(path) as f:
            seen_fasta, cur, h = False, None, None
            for line in f:
                if not seen_fasta:
                    seen_fasta = line.startswith("##FASTA")
                    continue
                if line.startswith(">"):
                    if cur in orphan_set and h:
                        omd5[cur] = h.hexdigest()
                    cur = line[1:].split()[0]
                    h = hashlib.md5() if cur in orphan_set else None
                elif h:
                    h.update(line.strip().replace("\x00", "").upper().encode())
            if cur in orphan_set and h:
                omd5[cur] = h.hexdigest()

nodes = Counter()
if NODES:
    with open(NODES) as f:
        for line in f:
            nodes[line.split("\t", 1)[0]] += 1

by_len = defaultdict(list)
for o in orphans:
    if o in gff_len:
        by_len[gff_len[o]].append(o)
by_md5 = defaultdict(list)
for o, h in omd5.items():
    by_md5[h].append(o)

out, stat = [], Counter()
for c in todo:
    al = [a for a in aliases[c] if "." not in a]
    name_c = sorted(o for o in orphans if any(o.startswith(a + "_") for a in al))
    len_c = sorted(by_len.get(clen.get(c), [])) if c in clen else []
    seq_c = sorted(by_md5.get(cmd5.get(c), [])) if cmd5.get(c) else []
    chosen, status = "", "unresolved"
    if DM6_FASTA:
        if len(seq_c) == 1:
            chosen, status = seq_c[0], "verified"
            if (name_c and chosen not in name_c) or (len_c and chosen not in len_c):
                status = "mismatch"
        elif len(seq_c) > 1:
            status = "mismatch"
        elif name_c or len_c:
            status = "mismatch"        # name/length candidates exist but the sequences differ
    else:
        if len(name_c) == 1 and name_c == len_c:
            chosen, status = name_c[0], "evidence_only"
        elif len(len_c) == 1 and not name_c:
            chosen, status = len_c[0], "evidence_only"
        elif name_c or len_c:
            status = "mismatch"
    stat[status] += 1
    out.append((c, nodes.get(c, 0), chosen if status in ("verified", "evidence_only") else "", status,
                ",".join(name_c), ",".join(len_c), ",".join(seq_c) if DM6_FASTA else "NA"))
with open(MAP_OUT, "w") as o:
    o.write("ucsc\tnodes\tflybase_id\tstatus\tname_candidates\tlength_candidates\tsequence_matches\n")
    for r in sorted(out, key=lambda r: -r[1]):
        o.write("\t".join(map(str, r)) + "\n")
print("status:", dict(stat), "| nodes by status:", {k: sum(r[1] for r in out if r[3] == k) for k in stat})
for r in sorted(out, key=lambda r: -r[1])[:10]:
    print(f"  {r[0]:26s} nodes={r[1]:>4}  {r[3]:13s} {r[2]}")
claimed = {r[2] for r in out if r[2]}
unclaimed = [o for o in orphans if o not in claimed]
print(f"\nversion {VERSION} | orphan GFF ids not claimed by any contig to resolve: {len(unclaimed)}")
if unclaimed:
    all_md5 = {}
    if DM6_FASTA:
        for cid, n, h in read_fasta(DM6_FASTA, wanted=AnySet()):
            all_md5.setdefault(h, []).append(cid)
    for o in unclaimed:
        eq = all_md5.get(omd5.get(o), []) if DM6_FASTA else None
        print(f"  {o}  length {gff_len.get(o, '?')}" + (f"  identical to dm6 contig(s): {eq or 'none'}" if DM6_FASTA else ""))
print(f"map written to {MAP_OUT}")
