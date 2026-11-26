#!/usr/bin/env python3
"""Offline liftover of EnhancerAtlas 2.0 data for the BioAS adapter (SPECIES=hsa: hg19->hg38, SPECIES=dmel: dm3->dm6).

Version history (the version is printed on the first line of summary.txt; summaries without a version line come from 1.0-1.2):
  1.0  per-tissue BED + EP lift, EP-only nodes, FlyBase naming via chromAlias            (output folders tissue_bed/ and ep/)
  1.1  output folders renamed to bed/ and enhancer_gene/; merged BED lift added (hs.bed.gz / dm.bed.gz)
  1.2  optional GFF_FASTA_IDS=1 (default off): accept ##FASTA header ids as FlyBase names. Output is identical to 1.1 unless it is set.
  1.3  version line in summary.txt (no change in the outputs)
  1.4  optional NAME_MAP=<tsv from build_dm6_contig_name_map.py> (flybase mode): extra UCSC->FlyBase contig names; by default only
       entries with status "verified" (identical sequence) are used. Output is identical to 1.3 unless NAME_MAP is set.
  1.5  also writes ep_only_nodes.bed (the rows of nodes_union.bed whose source is ep_only; empty for dmel). Nothing else changes.
  1.6  optional gene-id step for the EP files (GENE_IDS, GENE_ID_MAP, GENE_ID_NOTES): ids that are not current are remapped through the map,
       or their lines are held out in enhancer_gene_held/ (nothing is deleted). Output is identical to 1.5 unless GENE_IDS is set.
  1.7  GENE_ID_COLLISION (keep_all, the default, = 1.6; or dedupe_identical): where a remap makes several lines share tissue, enhancer and gene,
       the lines with an identical score are collapsed to one (the line that already had the current id is kept) and the others are moved to
       enhancer_gene_duplicates/ (nothing is deleted); lines with different scores are all kept.

Inputs (read only): the per-tissue BEDs (*.bed; '.bed.old' leftovers ignored) and the *_EP.txt files of one species.
Decisions applied (agreed): per-tissue BEDs are the node source, contigs included; human EP-only enhancer intervals are added as nodes;
UCSC liftOver with -minMatch (default 0.90), no -multiple; enhancers that fail to lift are dropped (nodes and their edges) and logged;
the TSS inside the EP keys is lifted, and when only the TSS fails the edge is kept with the TSS left blank; the same chain/parameters
are used for BEDs and EP files, so an interval identical in both before the lift stays identical after it.

Also lifts the species-level merged BED (hs.bed.gz / dm.bed.gz): columns 1-3 are lifted, column 4 (length:signal:tissue entries, source-build
lengths) is copied unchanged and the original interval is appended; regions that fail to lift are dropped and logged.

Reads <input_dir>/enhancer_atlas/non_lifted/ (the downloader output) and writes into OUT, by default <input_dir>/enhancer_atlas/
(the folder the adapter records point at):
  bed/<tissue>.bed               chr start end signal orig_chr orig_start orig_end            (lifted, tab separated)
  enhancer_gene/<tissue>_EP.txt  EP files in the original format with lifted enhancer interval and TSS (LF line endings)
  hs.bed.gz | dm.bed.gz          lifted merged BED (gzip): chr start end annotation orig_chr orig_start orig_end
  nodes_union.bed                unique lifted intervals: chr start end source(bed|ep_only) orig_chr orig_start orig_end
  ep_only_nodes.bed              the ep_only rows of nodes_union.bed (EP enhancers found in no per-tissue BED)
  unmapped_bed_intervals.tsv     tissue, original interval, liftOver reason
  unmapped_merged_regions.tsv    merged row number, original interval, liftOver reason
  ep_dropped_pairs.tsv           tissue, original EP key, reason
  enhancer_gene_held/<tissue>_EP.txt  (only with GENE_IDS) lifted EP lines whose gene id is not current and has no usable mapping; the adapter does not read them
  enhancer_gene_duplicates/<tissue>_EP.txt  (only with GENE_ID_COLLISION=dedupe_identical) the lines collapsed because they repeated another line's score
  gene_id_report/                (only with GENE_IDS) gene_id_remapped.tsv (old, new, pairs, tissues), gene_id_held.tsv (old, pairs, tissues, symbol, note),
                                 gene_id_collisions.tsv (same tissue, enhancer and new gene on more than one line after a remap; all lines are kept)
  summary.txt                    counts, parameters, chain md5, naming report
Chromosome naming (NAME_MODE): ucsc (default; keep the target-build names), strip (remove 'chr'), flybase (dmel only: use the alias of each
dm6 contig, from UCSC chromAlias.txt, that appears as a sequence id in the FlyBase GFF; otherwise main arms lose 'chr' and other contigs keep
the UCSC name and are reported).
Gene ids (optional, EP files only): GENE_IDS=<file, one current gene id per line, e.g. the ids of the annotation the graph uses>, GENE_ID_MAP=<tsv old_id<TAB>new_id,
header 'old_id' allowed; only mappings whose new id is in GENE_IDS are applied; needs GENE_IDS> and GENE_ID_NOTES=<tsv, first column an id, the other columns
are copied as a note into gene_id_held.tsv>. A line whose gene is current is kept; a mapped one is rewritten with the new id; any other line goes to
enhancer_gene_held/. Env: SPECIES (required), INPUT_DIR (default: input_dir of config/<species>/<species>_adapters_config.yaml), BED_DIR, EP_DIR, MERGED_BED (SKIP_MERGED=1 to skip it), CHAIN, OUT, LIFTOVER, MINMATCH, NAME_MODE, CHROM_ALIAS, GFF,
     GFF_FASTA_IDS=1 (flybase mode: also accept the ids in the GFF ##FASTA headers, i.e. sequences that carry no feature),
     NAME_MAP=<contig map tsv> (flybase mode) and NAME_MAP_MIN=verified|evidence (default verified).
"""
import datetime, glob, gzip, hashlib, os, re, shutil, subprocess, sys
from collections import Counter, defaultdict

VERSION = "1.7"
SPECIES = os.environ.get("SPECIES", "")
# Default paths follow the repo layout: <input_dir> is the species' input_dir from
# config/<species>/<species>_adapters_config.yaml (override with INPUT_DIR). The downloader puts the
# published files in <input_dir>/enhancer_atlas/non_lifted/ and the chain of the dbSUPER liftover in
# <input_dir>/liftover_chains/; the lift writes to <input_dir>/enhancer_atlas/, where the adapter reads.
PROFILES = {
    "hsa": dict(code="hs", chain="hg19ToHg38.over.chain.gz", src="hg19", dst="hg38", strand=True),
    "dmel": dict(code="dm", chain="dm3ToDm6.over.chain.gz", src="dm3", dst="dm6", strand=False),
}
if SPECIES not in PROFILES:
    sys.exit(f"SPECIES must be one of {sorted(PROFILES)}")
P = PROFILES[SPECIES]


def _default_input_dir():
    cfg = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "config", SPECIES, f"{SPECIES}_adapters_config.yaml")
    try:
        with open(cfg) as f:
            for line in f:
                if line.startswith("input_dir:"):
                    return line.split(":", 1)[1].split("#", 1)[0].strip()
    except OSError:
        pass
    return ""


INPUT_DIR = os.environ.get("INPUT_DIR") or _default_input_dir()
EA_DIR = os.path.join(INPUT_DIR, "enhancer_atlas")
RAW_DIR = os.path.join(EA_DIR, "non_lifted")
P.update(bed=os.path.join(RAW_DIR, "bed"), ep=os.path.join(RAW_DIR, "enhancer_gene"),
         merged=os.path.join(RAW_DIR, f"{P['code']}.bed.gz"), merged_out=f"{P['code']}.bed.gz",
         chain=os.path.join(INPUT_DIR, "liftover_chains", P["chain"]))
BED_DIR = os.environ.get("BED_DIR", P["bed"])
EP_DIR = os.environ.get("EP_DIR", P["ep"])
MERGED_BED = os.environ.get("MERGED_BED", P["merged"])
SKIP_MERGED = os.environ.get("SKIP_MERGED") == "1"
CHAIN = os.environ.get("CHAIN", P["chain"])
OUT = os.environ.get("OUT", EA_DIR)
LIFTOVER = os.environ.get("LIFTOVER", "liftOver")
MINMATCH = os.environ.get("MINMATCH", "0.90")
NAME_MODE = os.environ.get("NAME_MODE", "ucsc")
CHROM_ALIAS = os.environ.get("CHROM_ALIAS", "")
GFF = [g for g in os.environ.get("GFF", "").split(",") if g]
NAME_MAP = os.environ.get("NAME_MAP", "")
NAME_MAP_MIN = os.environ.get("NAME_MAP_MIN", "verified")
GENE_IDS = os.environ.get("GENE_IDS", "")
GENE_ID_MAP = os.environ.get("GENE_ID_MAP", "")
GENE_ID_NOTES = os.environ.get("GENE_ID_NOTES", "")
GENE_ID_COLLISION = os.environ.get("GENE_ID_COLLISION", "keep_all")
if GENE_ID_COLLISION not in ("keep_all", "dedupe_identical"):
    sys.exit("GENE_ID_COLLISION must be keep_all or dedupe_identical")
if GENE_ID_COLLISION != "keep_all" and not GENE_IDS:
    sys.exit("GENE_ID_COLLISION needs GENE_IDS")
if GENE_ID_MAP and not GENE_IDS:
    sys.exit("GENE_ID_MAP needs GENE_IDS (the current ids), so that only mappings to existing ids are applied")
FASTA_IDS = os.environ.get("GFF_FASTA_IDS") == "1"      # also accept the ids of the ##FASTA headers (sequences without features)
EP_RE = re.compile(r"^([^:]+):(\d+)-(\d+)_(.*)$")
FLY_ARMS = {"2L", "2R", "3L", "3R", "4", "X", "Y"}


def open_text(path):
    with open(path, "rb") as fh:
        magic = fh.read(2)
    return gzip.open(path, "rt") if magic == b"\x1f\x8b" else open(path, "rt", newline="")


def md5(path):
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def build_namer():
    """returns (function old->new, description)"""
    if NAME_MODE == "ucsc":
        return (lambda c: c), "UCSC target names kept"
    if NAME_MODE == "strip":
        return (lambda c: c[3:] if c.startswith("chr") else c), "'chr' prefix removed"
    if NAME_MODE != "flybase" or SPECIES != "dmel":
        sys.exit("NAME_MODE must be ucsc, strip, or flybase (dmel only)")
    if NAME_MAP and not os.path.exists(NAME_MAP):
        sys.exit(f"NAME_MAP not found: {NAME_MAP}")
    if not CHROM_ALIAS or not GFF:
        sys.exit("NAME_MODE=flybase needs CHROM_ALIAS=<dm6.chromAlias.txt> and GFF=<FlyBase gff(s)>")
    seqids = set()
    for path in GFF:
        with open_text(path) as f:
            in_fasta = False
            for line in f:
                if in_fasta:
                    if line.startswith(">"):
                        seqids.add(line[1:].split()[0])
                    continue
                if line.startswith("##FASTA"):
                    if not FASTA_IDS:
                        break
                    in_fasta = True
                    continue
                if line.startswith("#"):
                    continue
                seqids.add(line.split("\t", 1)[0])
    rows = []
    with open_text(CHROM_ALIAS) as f:
        for line in f:
            if line.startswith("#") or not line.strip():
                continue
            rows.append(line.rstrip("\r\n").split("\t"))
    ncols = max(len(r) for r in rows)
    chr_cols = [sum(1 for r in rows if i < len(r) and r[i].startswith("chr")) for i in range(ncols)]
    ucol = chr_cols.index(max(chr_cols))                 # the column holding the UCSC chr* names
    mapping = {}
    for r in rows:
        aliases = [a for i, col in enumerate(r) if i != ucol for a in col.split(",") if a]
        hit = [a for a in aliases if a in seqids]
        if hit:
            mapping[r[ucol]] = hit[0]

    extra = 0
    if NAME_MAP:
        ok = {"verified"} if NAME_MAP_MIN == "verified" else {"verified", "evidence_only"}
        with open(NAME_MAP) as f:
            ci = {h: i for i, h in enumerate(f.readline().rstrip("\n").split("\t"))}
            for line in f:
                p = line.rstrip("\n").split("\t")
                u, fb, st = p[ci["ucsc"]], p[ci["flybase_id"]], p[ci["status"]]
                if st in ok and fb and u not in mapping:
                    mapping[u] = fb
                    extra += 1

    def namer(c):
        if c in mapping:
            return mapping[c]
        return c[3:] if c.startswith("chr") and c[3:] in FLY_ARMS else c
    return namer, (f"FlyBase GFF sequence ids{' and ##FASTA header ids' if FASTA_IDS else ''} via chromAlias ({len(mapping) - extra} UCSC contigs matched, UCSC column = {ucol + 1})"
                   + (f" + {extra} contigs from NAME_MAP (min status {NAME_MAP_MIN})" if NAME_MAP else ""))


def split_tss(rest):
    """-> (tokens, index of gene-chr token, index of TSS token) or (tokens, None, None) when the key holds no TSS"""
    toks = rest.split("$")
    try:
        if P["strand"]:
            int(toks[-2])
            return toks, len(toks) - 3, len(toks) - 2
        int(toks[-1])
        return toks, len(toks) - 2, len(toks) - 1
    except (ValueError, IndexError):
        return toks, None, None


def run_liftover(entries, name):
    """entries: list of (chr, start, end). Returns (mapped{id:(c,s,e)}, reason{id:str})"""
    work = f"{OUT}/work"
    bed, out, unm = f"{work}/{name}.in.bed", f"{work}/{name}.out.bed", f"{work}/{name}.unmapped"
    with open(bed, "w") as f:
        for i, (c, s, e) in enumerate(entries):
            f.write(f"{c}\t{s}\t{e}\t{i}\n")
    r = subprocess.run([LIFTOVER, f"-minMatch={MINMATCH}", bed, CHAIN, out, unm], capture_output=True, text=True)
    if r.returncode != 0:
        sys.exit(f"liftOver failed for {name}: {r.stderr.strip()}")
    mapped = {}
    with open(out) as f:
        for line in f:
            p = line.rstrip("\n").split("\t")
            mapped[int(p[3])] = (p[0], int(p[1]), int(p[2]))
    reason, last = {}, "unknown"
    with open(unm) as f:
        for line in f:
            line = line.rstrip("\n")
            if line.startswith("#"):
                last = line[1:].strip()
            elif line:
                reason[int(line.split("\t")[3])] = last
    return mapped, reason


def main():
    for p in (BED_DIR, EP_DIR, CHAIN) + (() if SKIP_MERGED else (MERGED_BED,)):
        if not os.path.exists(p):
            sys.exit(f"missing input: {p}" + ("  (set MERGED_BED=<path> or SKIP_MERGED=1)" if p == MERGED_BED else ""))
    if shutil.which(LIFTOVER) is None:
        sys.exit(f"liftOver binary not found: {LIFTOVER}")
    bed_files = sorted(p for p in glob.glob(f"{BED_DIR}/*.bed"))
    ep_files = sorted(glob.glob(f"{EP_DIR}/*_EP.txt"))
    if not bed_files or not ep_files:
        sys.exit(f"need *.bed in {BED_DIR} and *_EP.txt in {EP_DIR}")
    for d in ("work", "bed", "enhancer_gene"):
        os.makedirs(f"{OUT}/{d}", exist_ok=True)
    namer, name_desc = build_namer()

    # ---- pass 1: unique intervals from the per-tissue BEDs, then EP-only intervals and TSS points
    iv_id, ep_only = {}, set()
    n_bed_records = 0
    for path in bed_files:
        with open(path, newline="") as f:
            for line in f:
                if not line.strip() or line.startswith(("#", "track", "browser")):
                    continue
                p = line.rstrip("\r\n").split("\t")
                iv_id.setdefault((p[0], int(p[1]), int(p[2])), len(iv_id))
                n_bed_records += 1
    n_bed_unique = len(iv_id)
    tss_id, n_pairs = {}, 0
    for path in ep_files:
        with open(path, newline="") as f:
            for line in f:
                key = line.split("\t", 1)[0]
                m = EP_RE.match(key)
                if not m:
                    continue
                n_pairs += 1
                iv = (m.group(1), int(m.group(2)), int(m.group(3)))
                if iv not in iv_id:
                    iv_id[iv] = len(iv_id)
                    ep_only.add(iv)
                toks, ci, ti = split_tss(m.group(4))
                if ti is not None:
                    tss_id.setdefault((toks[ci], int(toks[ti])), len(tss_id))
    ivs = sorted(iv_id, key=iv_id.get)
    tss_list = sorted(tss_id, key=tss_id.get)
    print(f"per-tissue BED records {n_bed_records:,} | unique intervals {n_bed_unique:,} | EP-only intervals {len(ep_only):,} | EP pairs {n_pairs:,} | TSS points {len(tss_list):,}")

    # ---- lift
    m_iv, r_iv = run_liftover(ivs, "intervals")
    m_ts, r_ts = run_liftover([(c, p, p + 1) for c, p in tss_list], "tss")
    print(f"intervals lifted {len(m_iv):,} / unmapped {len(r_iv):,} | TSS lifted {len(m_ts):,} / unmapped {len(r_ts):,}")

    # ---- pass 2: per-tissue BEDs
    unm_log = open(f"{OUT}/unmapped_bed_intervals.tsv", "w")
    unm_log.write("tissue\tchr\tstart\tend\treason\n")
    bed_written = bed_dropped = 0
    per_tissue = {}
    for path in bed_files:
        t = os.path.basename(path)[:-4]
        w = d = 0
        with open(path, newline="") as f, open(f"{OUT}/bed/{t}.bed", "w") as o:
            for line in f:
                if not line.strip() or line.startswith(("#", "track", "browser")):
                    continue
                p = line.rstrip("\r\n").split("\t")
                src = (p[0], int(p[1]), int(p[2]))
                i = iv_id[src]
                if i in m_iv:
                    c, s, e = m_iv[i]
                    sig = p[3] if len(p) > 3 else ""
                    o.write(f"{namer(c)}\t{s}\t{e}\t{sig}\t{src[0]}\t{src[1]}\t{src[2]}\n")
                    w += 1
                else:
                    unm_log.write(f"{t}\t{src[0]}\t{src[1]}\t{src[2]}\t{r_iv.get(i, 'unknown')}\n")
                    d += 1
        per_tissue[t] = (w, d)
        bed_written += w
        bed_dropped += d
    unm_log.close()

    # ---- gene id inputs (optional)
    current, idmap, notes = None, {}, {}
    n_cur = n_remap = n_held = n_coll = n_moved = n_multi = 0
    remapped, held = Counter(), {}
    if GENE_IDS:
        current = {l.split()[0] for l in open_text(GENE_IDS) if l.strip() and not l.startswith("#")}
        if GENE_ID_MAP:
            for l in open_text(GENE_ID_MAP):
                p = l.rstrip("\r\n").split("\t")
                if len(p) >= 2 and p[0] and p[0] != "old_id" and not l.startswith("#"):
                    idmap[p[0]] = p[1]
        if GENE_ID_NOTES:
            for l in open_text(GENE_ID_NOTES):
                p = l.rstrip("\r\n").split("\t")
                if p and p[0] and p[0] != "old_id":
                    notes[p[0]] = " | ".join(x for x in p[1:] if x)
        for d_ in ("enhancer_gene_held", "gene_id_report") + (("enhancer_gene_duplicates",) if GENE_ID_COLLISION != "keep_all" else ()):
            shutil.rmtree(f"{OUT}/{d_}", ignore_errors=True)
            os.makedirs(f"{OUT}/{d_}")
        coll_log = open(f"{OUT}/gene_id_report/gene_id_collisions.tsv", "w")
        coll_log.write("tissue\tenhancer\tnew_gene\tlines" + ("\tdistinct_scores\tmoved_lines" if GENE_ID_COLLISION != "keep_all" else "") + "\n")
        print(f"gene ids: {len(current):,} current | {len(idmap):,} mappings | {len(notes):,} notes")

    # ---- pass 3: EP files
    drop_log = open(f"{OUT}/ep_dropped_pairs.tsv", "w")
    drop_log.write("tissue\tkey\treason\n")
    ep_written = ep_dropped = tss_blank = tss_ok = 0
    ep_per = {}
    for path in ep_files:
        t = os.path.basename(path)[:-len("_EP.txt")]
        w = d = 0
        held_f, pair_count, remap_keys, buf, coll_info = None, Counter(), set(), [], {}
        with open(path, newline="") as f, open(f"{OUT}/enhancer_gene/{t}_EP.txt", "w") as o:
            for line in f:
                line = line.rstrip("\r\n")
                if not line:
                    continue
                key, _, rest_of_line = line.partition("\t")
                m = EP_RE.match(key)
                if not m:
                    continue
                iv = (m.group(1), int(m.group(2)), int(m.group(3)))
                i = iv_id[iv]
                if i not in m_iv:
                    drop_log.write(f"{t}\t{key}\tenhancer not lifted: {r_iv.get(i, 'unknown')}\n")
                    d += 1
                    continue
                c, s, e = m_iv[i]
                toks, ci, ti = split_tss(m.group(4))
                gid, status = toks[0], "current"
                if current is not None and gid not in current:
                    new_id = idmap.get(gid)
                    status = "remapped" if (new_id and new_id in current) else "held"
                if ti is not None:
                    tid = tss_id[(toks[ci], int(toks[ti]))]
                    if tid in m_ts:
                        tc, tp, _ = m_ts[tid]
                        toks[ci], toks[ti] = namer(tc), str(tp)
                        tss_ok += status != "held"
                    else:
                        toks[ti] = ""                      # TSS could not be lifted: keep the edge, leave the TSS blank
                        toks[ci] = namer(toks[ci])
                        tss_blank += status != "held"
                elif not P["strand"] and toks[-1].startswith("chr"):
                    toks[-1] = namer(toks[-1])             # fly key without a TSS: the last field is the gene chromosome
                out_key = f"{namer(c)}:{s}-{e}"
                if status == "held":                       # not a current id and no usable mapping: keep the lifted line aside
                    if held_f is None:
                        held_f = open(f"{OUT}/enhancer_gene_held/{t}_EP.txt", "w")
                    held_f.write(f"{out_key}_{'$'.join(toks)}\t{rest_of_line}\n")
                    h = held.setdefault(gid, dict(pairs=0, tissues=set(), symbol=toks[1] if len(toks) > 1 else ""))
                    h["pairs"] += 1
                    h["tissues"].add(t)
                    n_held += 1
                    continue
                if status == "remapped":
                    toks[0] = idmap[gid]
                    remapped[(gid, toks[0], t)] += 1
                    n_remap += 1
                    remap_keys.add((out_key, toks[0]))
                elif current is not None:
                    n_cur += 1
                if current is not None:
                    pair_count[(out_key, toks[0])] += 1
                line_out = f"{out_key}_{'$'.join(toks)}\t{rest_of_line}\n"
                if GENE_ID_COLLISION == "keep_all":
                    o.write(line_out)
                    w += 1
                else:                                      # buffered: the collision groups are only known at the end of the file
                    buf.append((out_key, toks[0], rest_of_line.strip(), line_out, status == "remapped"))
            if GENE_ID_COLLISION != "keep_all":
                groups = defaultdict(list)
                for idx, b in enumerate(buf):
                    if (b[0], b[1]) in remap_keys and pair_count[(b[0], b[1])] > 1:
                        groups[(b[0], b[1])].append(idx)
                drop = set()
                for k, idxs in groups.items():
                    by_score = defaultdict(list)
                    for idx in idxs:
                        by_score[buf[idx][2]].append(idx)
                    moved = 0
                    for ids_ in by_score.values():
                        keep = next((x for x in ids_ if not buf[x][4]), ids_[0])   # prefer the line that already had the current id
                        for x in ids_:
                            if x != keep:
                                drop.add(x)
                                moved += 1
                    coll_info[k] = (len(by_score), moved)
                dup_f = None
                for idx, b in enumerate(buf):
                    if idx in drop:
                        if dup_f is None:
                            dup_f = open(f"{OUT}/enhancer_gene_duplicates/{t}_EP.txt", "w")
                        dup_f.write(b[3])
                    else:
                        o.write(b[3])
                        w += 1
                if dup_f is not None:
                    dup_f.close()
                n_moved += len(drop)
        if held_f is not None:
            held_f.close()
        for k in sorted(remap_keys):
            if pair_count[k] > 1:
                if GENE_ID_COLLISION == "keep_all":
                    coll_log.write(f"{t}\t{k[0]}\t{k[1]}\t{pair_count[k]}\n")
                else:
                    coll_log.write(f"{t}\t{k[0]}\t{k[1]}\t{pair_count[k]}\t{coll_info[k][0]}\t{coll_info[k][1]}\n")
                    n_multi += (pair_count[k] - coll_info[k][1]) > 1
                n_coll += 1
        ep_per[t] = (w, d)
        ep_written += w
        ep_dropped += d
    drop_log.close()
    if current is not None:
        coll_log.close()
        agg = defaultdict(lambda: [0, set()])
        for (old, new, t), n in remapped.items():
            agg[(old, new)][0] += n
            agg[(old, new)][1].add(t)
        with open(f"{OUT}/gene_id_report/gene_id_remapped.tsv", "w") as r:
            r.write("old_id\tnew_id\tpairs\ttissues\n")
            for (old, new), (n, ts) in sorted(agg.items(), key=lambda kv: -kv[1][0]):
                r.write(f"{old}\t{new}\t{n}\t{len(ts)}\n")
        with open(f"{OUT}/gene_id_report/gene_id_held.tsv", "w") as r:
            r.write("old_id\tpairs\ttissues\tsymbol\tnote\n")
            for g, h in sorted(held.items(), key=lambda kv: -kv[1]["pairs"]):
                r.write(f"{g}\t{h['pairs']}\t{len(h['tissues'])}\t{h['symbol']}\t{notes.get(g, 'not current and no usable mapping')}\n")
        n_remap_ids = len({k[:2] for k in remapped})

    # ---- merged species-level BED
    mg_total = mg_written = mg_dropped = mg_resized = 0
    mg_reasons = Counter()
    if not SKIP_MERGED:
        mg_iv = []
        with open_text(MERGED_BED) as f:
            for line in f:
                if not line.strip() or line.startswith(("#", "track", "browser")):
                    continue
                p = line.split("\t", 3)
                mg_iv.append((p[0], int(p[1]), int(p[2])))
        mg_total = len(mg_iv)
        m_mg, r_mg = run_liftover(mg_iv, "merged")
        mg_log = open(f"{OUT}/unmapped_merged_regions.tsv", "w")
        mg_log.write("row\tchr\tstart\tend\treason\n")
        with open_text(MERGED_BED) as f, gzip.open(f"{OUT}/{P['merged_out']}", "wt") as o:
            idx = 0
            for line in f:
                if not line.strip() or line.startswith(("#", "track", "browser")):
                    continue
                p = line.rstrip("\r\n").split("\t", 3)
                src = mg_iv[idx]
                if idx in m_mg:
                    c, s, e = m_mg[idx]
                    o.write(f"{namer(c)}\t{s}\t{e}\t{p[3] if len(p) > 3 else ''}\t{src[0]}\t{src[1]}\t{src[2]}\n")
                    mg_written += 1
                    mg_resized += (e - s) != (src[2] - src[1])
                else:
                    mg_log.write(f"{idx}\t{src[0]}\t{src[1]}\t{src[2]}\t{r_mg.get(idx, 'unknown')}\n")
                    mg_dropped += 1
                idx += 1
        mg_log.close()
        mg_reasons = Counter(r_mg.values())
        print(f"merged BED: {mg_total:,} regions -> written {mg_written:,}, dropped {mg_dropped:,}")

    # ---- node union (unique lifted intervals)
    seen, dest, collisions = set(), Counter(), 0
    n_nodes = 0
    n_ep_only = 0
    with open(f"{OUT}/nodes_union.bed", "w") as o, open(f"{OUT}/ep_only_nodes.bed", "w") as oe:
        for iv, i in sorted(iv_id.items(), key=lambda kv: kv[1]):
            if i not in m_iv:
                continue
            c, s, e = m_iv[i]
            if (c, s, e) in seen:
                collisions += 1
                continue
            seen.add((c, s, e))
            n_nodes += 1
            dest[namer(c)] += 1
            row = f"{namer(c)}\t{s}\t{e}\t{'ep_only' if iv in ep_only else 'bed'}\t{iv[0]}\t{iv[1]}\t{iv[2]}\n"
            o.write(row)
            if iv in ep_only:
                oe.write(row)
                n_ep_only += 1

    # ---- summary
    reasons = Counter(r_iv.values())
    lines = [
        f"EnhancerAtlas 2.0 offline lift v{VERSION} | species {SPECIES} ({P['src']} -> {P['dst']}) | {datetime.datetime.now().isoformat(timespec='seconds')}",
        f"chain {CHAIN} md5 {md5(CHAIN)} | liftOver {shutil.which(LIFTOVER)} | minMatch {MINMATCH} | no -multiple | name mode {NAME_MODE}: {name_desc}",
        f"per-tissue BED files {len(bed_files)} | records {n_bed_records:,} -> written {bed_written:,}, dropped {bed_dropped:,}",
        f"unique intervals {len(ivs):,} (EP-only {len(ep_only):,}) -> lifted {len(m_iv):,}, unmapped {len(r_iv):,} {dict(reasons)}",
        f"nodes_union.bed: {n_nodes:,} unique lifted intervals (collapsed duplicates after lift: {collisions:,}) | ep_only_nodes.bed: {n_ep_only:,} rows",
        f"EP files {len(ep_files)} | pairs {n_pairs:,} -> written {ep_written:,}, dropped (enhancer not lifted) {ep_dropped:,}",
        f"TSS in kept pairs: lifted {tss_ok:,}, blanked after a failed lift {tss_blank:,}",
    ]
    if current is not None:
        lines.append(f"gene ids: GENE_IDS {GENE_IDS} md5 {md5(GENE_IDS)} ({len(current):,} ids)"
                     + (f" | GENE_ID_MAP {GENE_ID_MAP} md5 {md5(GENE_ID_MAP)} ({len(idmap):,} mappings)" if GENE_ID_MAP else " | no GENE_ID_MAP")
                     + (f" | GENE_ID_NOTES {GENE_ID_NOTES}" if GENE_ID_NOTES else ""))
        lines.append(f"EP pairs by gene id: current {n_cur:,} | remapped {n_remap:,} ({n_remap_ids:,} ids) | held out {n_held:,} ({len(held):,} ids) -> "
                     f"enhancer_gene_held/ and gene_id_report/ | same tissue+enhancer+gene on several lines after a remap: {n_coll:,} groups"
                     + (" (all kept)" if GENE_ID_COLLISION == "keep_all" else
                        f" | policy dedupe_identical: {n_moved:,} lines with an already seen score moved to enhancer_gene_duplicates/, groups still "
                        f"with several lines (different scores): {n_multi:,}"))
    if not SKIP_MERGED:
        lines.append(f"merged BED {MERGED_BED}: {mg_total:,} regions -> {P['merged_out']} written {mg_written:,} (length changed by the lift: {mg_resized:,}), "
                     f"dropped {mg_dropped:,} {dict(mg_reasons)}; column 4 copied unchanged (lengths refer to the source build)")
    contigs = sorted(dest.items(), key=lambda kv: -kv[1])
    lines.append(f"destination contigs: {len(contigs)}; largest: " + ", ".join(f"{c} {n:,}" for c, n in contigs[:12]))
    if NAME_MODE == "flybase":
        kept = [(c, n) for c, n in contigs if c.startswith("chr")]
        lines.append(f"contigs still carrying UCSC names (no alias found in the GFF): {len(kept)} contigs, {sum(n for _, n in kept):,} nodes"
                     + (f"; e.g. {', '.join(c for c, _ in kept[:5])}" if kept else ""))
    with open(f"{OUT}/summary.txt", "w") as o:
        o.write("\n".join(lines) + "\n")
    print("\n" + "\n".join(lines))
    print(f"\noutputs in {OUT}")


if __name__ == "__main__":
    main()
