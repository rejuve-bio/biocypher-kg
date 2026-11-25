#!/usr/bin/env python3
"""
Empirical coordinate-convention check for the PEREGRINE enhancer bulk files.

PEREGRINE documents neither its coordinate convention nor how it lifted its
(mostly hg19-native) enhancer sets to hg38. This script infers the convention
per sub-source (ENCODE, FANTOM, Ensembl, VISTA) by comparing every PEREGRINE
record against the original source data, whose convention is known.

Two independent tests are run per sub-source, whenever their inputs exist:

  1. ID/length test (assembly-independent, no liftover needed).
     Only for sources whose original IDs survive in PEREGRINE (ENCODE cCRE
     EH37E... IDs). Tallies  len_PEREGRINE - len_reference,  with both lengths
     computed as end - start on the raw numbers.
       delta  0 -> same convention as the reference for the start
                   (or both ends shifted; test 2 disambiguates)
       delta -1 -> start shifted +1 vs a BED reference => 1-based closed
     Liftover can change a few lengths (indels in the chain), so read the
     distribution, not individual records.

  2. Positional test (needs the reference in hg38).
     For every PEREGRINE record, find the reference interval on the same
     chromosome whose start and end both lie within +/-WINDOW bp and tally the
     offsets (dStart, dEnd) = PEREGRINE - reference(BED). Chance matches with
     both ends within 2 bp are negligible, so even a modest match rate gives a
     clean tally. Interpretation (reference normalised to BED, 0-based half-open):
       ( 0,  0) -> 0-based half-open (BED)
       (+1,  0) -> 1-based closed (GFF/VCF-style)
       (+1, +1) -> 1-based half-open
       ( 0, -1) -> 0-based closed
     If the reference is hg19, pass --chain and --liftover-bin; the script
     lifts the BED-normalised reference to hg38 with UCSC liftOver. Because
     liftOver itself treats input as BED, this reproduces what PEREGRINE most
     likely did; minMatch differences only lower the match rate.

Reference spec (repeatable):  --ref SOURCE:FORMAT:ASSEMBLY:PATH
  SOURCE    ENCODE | FANTOM | Ensembl | VISTA  (as in PEREGRINEenhancersources)
  FORMAT    bed  (0-based half-open; BED3+, BED12 ok; name in column 4)
            gff  (1-based closed; GFF2/GFF3/GTF; ID from ID=/Name= if present)
            tsv1 (1-based closed, columns chr<TAB>start<TAB>end[<TAB>id])
  ASSEMBLY  hg19 | hg38   (hg19 => positional test needs --chain/--liftover-bin)
  PATH      plain or .gz

Expected conventions are read from the adapter's COORDINATES dict (parsed
with ast, the adapter is not imported) so the report shows PASS/FAIL against
what the adapter currently assumes.

Example (all full bulk files):
  python check_peregrine_coords.py \
    --enhancers peregrine/PEREGRINEenhancershg38.gz \
    --sources   peregrine/PEREGRINEenhancersources.gz \
    --adapter   biocypher_metta/adapters/hsa/peregrine_adapter.py \
    --ref ENCODE:bed:hg19:refs/ENCFF788SJC.bed.gz \
    --ref FANTOM:bed:hg19:refs/human_permissive_enhancers_phase_1_and_2.bed.gz \
    --ref Ensembl:gff:hg19:refs/homo_sapiens.GRCh37.Regulatory_Build.regulatory_features.gff.gz \
    --chain refs/hg19ToHg38.over.chain.gz --liftover-bin ./liftOver \
    --report peregrine_coord_check.tsv
"""
import argparse
import ast
import collections
import gzip
import os
import re
import shutil
import subprocess
import sys
import tempfile

WINDOW = 2
OFFSET_LABELS = {
    (0, 0): '0-based half-open',
    (1, 0): '1-based closed',
    (1, 1): '1-based half-open',
    (0, -1): '0-based closed',
}
# Fraction of matched records the dominant offset class must reach for a verdict.
VERDICT_MIN_FRACTION = 0.95


def open_any(path):
    return gzip.open(path, 'rt') if path.endswith('.gz') else open(path)


# ---------------------------------------------------------------- PEREGRINE
def read_sources(path):
    src = {}
    with open_any(path) as f:
        for line in f:
            line = line.rstrip('\n')
            if not line:
                continue
            eid, s = line.split('\t')
            src[eid] = s
    return src


def read_enhancers(path, source_map):
    """{source: [(chr, start, end, id), ...]} with raw numbers as stored."""
    by_src = collections.defaultdict(list)
    no_source = 0
    with open_any(path) as f:
        for line in f:
            p = line.rstrip('\n').split('\t')
            if len(p) < 4:
                continue
            s = source_map.get(p[3])
            if s is None:
                no_source += 1
                continue
            by_src[s].append((p[0], int(p[1]), int(p[2]), p[3]))
    return by_src, no_source


def expected_from_adapter(path):
    """Parse PEREGRINEAdapter.COORDINATES['components'] without importing."""
    if not path:
        return {}
    tree = ast.parse(open(path).read())
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign) and any(
                isinstance(t, ast.Name) and t.id == 'COORDINATES' for t in node.targets):
            coords = ast.literal_eval(node.value)
            return {k: v.get('source_convention')
                    for k, v in coords.get('components', {}).items()}
    return {}


# ---------------------------------------------------------------- references
CCRE_ID = re.compile(r'^EH3[78]E\d+$')
GFF_ID = re.compile(r'(?:^|;)\s*(?:ID|Name)[= ]"?([^";]+)"?')


def read_reference_as_bed(path, fmt):
    """Yield (chr, start0, end, name) normalised to BED (0-based half-open)."""
    with open_any(path) as f:
        for line in f:
            if not line.strip() or line.startswith(('#', 'track', 'browser')):
                continue
            p = line.rstrip('\n').split('\t')
            if fmt == 'bed':
                # cCRE files may carry the EH37E... accession outside column 4
                name = next((x for x in p[3:] if CCRE_ID.match(x)), p[3] if len(p) > 3 else '')
                yield p[0], int(p[1]), int(p[2]), name
            elif fmt == 'gff':
                if len(p) < 9:
                    continue
                m = GFF_ID.search(p[8])
                name = m.group(1) if m else ''
                if name.startswith('regulatory_region:') or name.startswith('enhancer:'):
                    name = name.split(':', 1)[1]
                yield p[0], int(p[3]) - 1, int(p[4]), name
            elif fmt == 'tsv1':
                name = p[3] if len(p) > 3 else ''
                yield p[0], int(p[1]) - 1, int(p[2]), name
            else:
                raise ValueError(f'unknown reference format {fmt!r}')


def add_chr(c):
    return c if c.startswith('chr') else ('chrM' if c == 'MT' else 'chr' + c)


def lift_to_hg38(records, chain, liftover_bin, workdir):
    """Run UCSC liftOver on BED-normalised records; returns lifted list."""
    inp = os.path.join(workdir, 'in.bed')
    out = os.path.join(workdir, 'out.bed')
    unm = os.path.join(workdir, 'unmapped.bed')
    with open(inp, 'w') as f:
        for i, (c, s, e, n) in enumerate(records):
            # keep a unique row key so original name and row survive liftover
            f.write(f'{add_chr(c)}\t{s}\t{e}\t{i}\n')
    subprocess.run([liftover_bin, inp, chain, out, unm], check=True)
    lifted = []
    with open(out) as f:
        for line in f:
            c, s, e, i = line.rstrip('\n').split('\t')[:4]
            lifted.append((c, int(s), int(e), records[int(i)][3]))
    n_unm = sum(1 for l in open(unm) if not l.startswith('#'))
    return lifted, n_unm


# ---------------------------------------------------------------- tests
def length_test(peregrine, ref_records):
    ref_len = {}
    dup = set()
    for c, s, e, n in ref_records:
        if not n:
            continue
        if n in ref_len:
            dup.add(n)
        ref_len[n] = e - s
    tally = collections.Counter()
    for c, s, e, eid in peregrine:
        if eid in ref_len and eid not in dup:
            tally[(e - s) - ref_len[eid]] += 1
    return tally, len(dup)


def positional_test(peregrine, ref_records):
    index = collections.defaultdict(list)   # (chr, start) -> [end, ...]
    for c, s, e, n in ref_records:
        index[(add_chr(c), s)].append(e)
    tally = collections.Counter()
    ambiguous = unmatched = 0
    for c, s, e, eid in peregrine:
        best = []
        for ds in range(-WINDOW, WINDOW + 1):
            for re_ in index.get((c, s - ds), ()):
                de = e - re_
                if abs(de) <= WINDOW:
                    best.append((abs(ds) + abs(de), ds, de))
        if not best:
            unmatched += 1
            continue
        # Deduplicate: a reference interval listed twice must not hide a tie with another one.
        best = sorted(set(best))
        if len(best) > 1 and best[0][0] == best[1][0] and best[0][1:] != best[1][1:]:
            ambiguous += 1
            continue
        tally[best[0][1:]] += 1
    return tally, ambiguous, unmatched


def verdict(tally, label_of):
    n = sum(tally.values())
    if n == 0:
        return 'NO DATA', 0.0
    top, k = tally.most_common(1)[0]
    frac = k / n
    label = label_of(top)
    if frac < VERDICT_MIN_FRACTION:
        return f'INCONCLUSIVE (top: {label})', frac
    return label, frac


def length_label(delta):
    return {0: 'same start convention as reference (BED => 0-based half-open)',
            -1: '1-based closed (start +1 vs BED)'}.get(delta, f'length delta {delta:+d}')


# ---------------------------------------------------------------- main
def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--enhancers', required=True)
    ap.add_argument('--sources', required=True)
    ap.add_argument('--ref', action='append', default=[],
                    help='SOURCE:FORMAT:ASSEMBLY:PATH (repeatable)')
    ap.add_argument('--adapter', help='peregrine_adapter.py, for expected conventions')
    ap.add_argument('--chain', help='hg19ToHg38.over.chain.gz (for hg19 references)')
    ap.add_argument('--liftover-bin', default=shutil.which('liftOver'))
    ap.add_argument('--report', help='write a TSV with every tally row')
    args = ap.parse_args()

    source_map = read_sources(args.sources)
    by_src, no_source = read_enhancers(args.enhancers, source_map)
    expected = expected_from_adapter(args.adapter)

    print('== PEREGRINE input')
    print(f'  sources file: {len(source_map):,} IDs; enhancers file rows without a source: {no_source:,}')
    for s in sorted(by_src):
        n_src = sum(1 for v in source_map.values() if v == s)
        print(f'  {s:8s} {len(by_src[s]):>9,} hg38 records ({n_src - len(by_src[s]):,} IDs absent from hg38 file)')

    rows = []
    summary = []
    for spec in args.ref:
        try:
            src, fmt, asm, path = spec.split(':', 3)
        except ValueError:
            sys.exit(f'bad --ref {spec!r}; expected SOURCE:FORMAT:ASSEMBLY:PATH')
        if src not in by_src:
            sys.exit(f'--ref source {src!r} not in PEREGRINE sources {sorted(by_src)}')
        pe = by_src[src]
        print(f'\n== {src}  (reference {os.path.basename(path)}, {fmt}, {asm})')
        ref = list(read_reference_as_bed(path, fmt))
        print(f'  reference records: {len(ref):,}')

        # Test 1: ID/length
        tally, ndup = length_test(pe, ref)
        n1 = sum(tally.values())
        if n1:
            v1, f1 = verdict(tally, length_label)
            print(f'  [length test] {n1:,} PEREGRINE records joined by ID '
                  f'({100 * n1 / len(pe):.1f}%); duplicate ref IDs skipped: {ndup:,}')
            for d, k in sorted(tally.items(), key=lambda x: -x[1])[:6]:
                print(f'      len delta {d:+6d}: {k:>9,} ({100 * k / n1:.2f}%)')
                rows.append((src, 'length', f'{d:+d}', k, n1))
            print(f'    -> {v1}  [{100 * f1:.2f}%]')
            summary.append((src, 'length', v1, f1, n1))
        else:
            print('  [length test] skipped: no PEREGRINE IDs found among reference names')

        # Test 2: positional
        ref38 = ref
        if asm != 'hg38':
            if not (args.chain and args.liftover_bin):
                print(f'  [positional test] skipped: reference is {asm}; '
                      f'pass --chain and --liftover-bin')
                continue
            with tempfile.TemporaryDirectory() as td:
                ref38, n_unm = lift_to_hg38(ref, args.chain, args.liftover_bin, td)
            print(f'  lifted to hg38: {len(ref38):,} (unmapped {n_unm:,})')
        tally, amb, unm = positional_test(pe, ref38)
        n2 = sum(tally.values())
        print(f'  [positional test] matched {n2:,}/{len(pe):,} ({100 * n2 / len(pe):.1f}%), '
              f'ambiguous {amb:,}, unmatched {unm:,}')
        for (ds, de), k in sorted(tally.items(), key=lambda x: -x[1])[:6]:
            lab = OFFSET_LABELS.get((ds, de), '?')
            print(f'      dStart {ds:+d} dEnd {de:+d}: {k:>9,} ({100 * k / n2:.2f}%)  {lab}')
            rows.append((src, 'positional', f'{ds:+d},{de:+d}', k, n2))
        v2, f2 = verdict(tally, lambda o: OFFSET_LABELS.get(o, f'offset {o}'))
        print(f'    -> {v2}  [{100 * f2:.2f}%]')
        summary.append((src, 'positional', v2, f2, n2))

    print('\n== Summary vs adapter COORDINATES')
    for src, test, v, f, n in summary:
        exp = expected.get(src)
        if test == 'positional' and exp and v in OFFSET_LABELS.values():
            status = 'PASS' if v == exp else 'FAIL'
        elif test == 'length' and exp and n:
            implied = {'same start convention as reference (BED => 0-based half-open)':
                       '0-based half-open', '1-based closed (start +1 vs BED)': '1-based closed'}.get(v)
            status = ('PASS' if implied == exp else 'FAIL') if implied else 'CHECK'
        else:
            status = 'CHECK'
        print(f'  {src:8s} {test:10s} observed: {v:<62s} n={n:>9,}  '
              f'adapter: {exp or "-":<18s} {status}')
    no_ref = sorted(set(by_src) - {s.split(":")[0] for s in args.ref})
    if no_ref:
        print(f'  no reference given for: {", ".join(no_ref)}')

    if args.report:
        with open(args.report, 'w') as f:
            f.write('source\ttest\toffset\tcount\tmatched_total\n')
            for r in rows:
                f.write('\t'.join(map(str, r)) + '\n')
        print(f'\nTally written to {args.report}')


if __name__ == '__main__':
    main()
