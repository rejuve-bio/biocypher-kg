#!/usr/bin/env python3
"""Smoke test of EnhancerAtlasAdapter on a lifted folder.  Run from the biocypher-kg repo root, in its environment.

Env: LIFTED (folder with bed/, enhancer_gene/, ep_only_nodes.bed), PICKLE (tissue -> ontology pickle), TAXON (9606 or 7227), ASSEMBLY (GRCh38 or dm6),
TISSUE_IN_ID (1 default, 0 for IDs without tissue), TARGETS_OUT (optional tsv: every distinct ontology target of the active-in labels, with its edge
count, to check that those terms exist as nodes in the graph). Read-only apart from TARGETS_OUT. Exit code 1 when an edge starts at a node that was not emitted.
"""
import logging, os, sys, time
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))  # run from anywhere
from collections import Counter
from biocypher_metta.adapters.enhancer_atlas_adapter import EnhancerAtlasAdapter, ACTIVE_IN_LABELS

LIFTED, PICKLE = os.environ.get("LIFTED", ""), os.environ.get("PICKLE", "")
TAXON, ASSEMBLY = os.environ.get("TAXON", ""), os.environ.get("ASSEMBLY", "")
TIID = os.environ.get("TISSUE_IN_ID", "1") == "1"
TARGETS_OUT = os.environ.get("TARGETS_OUT", "")
tcount = Counter()
if not (LIFTED and PICKLE and TAXON and ASSEMBLY):
    sys.exit("set LIFTED=, PICKLE=, TAXON=, ASSEMBLY= [TISSUE_IN_ID=0|1]")
logging.basicConfig(level=logging.WARNING, format="  %(levelname)s %(message)s")


def make(label):
    return EnhancerAtlasAdapter(f"{LIFTED}/bed", f"{LIFTED}/enhancer_gene", f"{LIFTED}/ep_only_nodes.bed", PICKLE, True, True,
                                int(TAXON), ASSEMBLY, label, tissue_in_id=TIID)


t0 = time.time()
ids, dup, nprops = set(), 0, Counter()
sample = None
for nid, label, props in make("enhancer").get_nodes():
    if nid in ids:
        dup += 1
    ids.add(nid)
    nprops.update(props.keys())
    sample = sample or (nid, label, props)
print(f"nodes: {len(ids):,} | duplicated ids: {dup} | properties: {dict(nprops)}\n  sample: {sample}")
bad = 0
report = {}
for label in ["enhancer_gene"] + list(ACTIVE_IN_LABELS):
    n, dangling, props_seen, targets, first = 0, 0, Counter(), set(), None
    for src, dst, lab, props in make(label).get_edges():
        n += 1
        dangling += src not in ids
        if label != "enhancer_gene":
            tcount[(label, dst)] += 1
        props_seen.update(props.keys())
        if len(targets) < 100000:
            targets.add(dst)
        first = first or (src, dst, lab, props)
    bad += dangling
    report[label] = n
    print(f"{label:30s} edges {n:>10,} | edges starting at a missing node: {dangling:,} | distinct targets: {len(targets):,} | properties: {dict(props_seen)}")
    if first:
        print(f"    sample: {first}")
if TARGETS_OUT:
    with open(TARGETS_OUT, "w") as f:
        f.write("label\ttarget\tedges\n")
        for (lab, dst), n in sorted(tcount.items()):
            f.write(f"{lab}\t{dst}\t{n}\n")
    print(f"\nontology targets written to {TARGETS_OUT}: {len(tcount)} distinct (label, term) pairs")
print(f"\nactive-in edges in total: {sum(v for k, v in report.items() if k != 'enhancer_gene'):,} (one per BED record of the tissues that have a usable ontology term)")
print(f"elapsed {time.time() - t0:.0f} s | result: {'OK' if bad == 0 else 'DANGLING EDGES FOUND'}")
sys.exit(1 if bad else 0)
