#!/usr/bin/env python3
"""Check that ontology terms exist in an OWL file and are not obsolete (streaming parse, works for large files; read-only).

Env: OWL (path to the .owl file), PREFIX (e.g. FBbt), and one of
  TARGETS  tsv with a column 'target' holding CURIEs like FBbt:00001766 (e.g. the TARGETS_OUT of smoke_test_adapter.py), or a plain list with one CURIE
           per line. Prints, per target: found / label / deprecated, then a summary. Exit code 1 if a target is missing or deprecated.
  SEARCH   a regular expression (case-insensitive) matched against the labels and the synonyms of the terms with that PREFIX; prints id, label,
           status and the synonym that matched. Use it to look for a better term.
"""
import os, re, sys
import xml.etree.ElementTree as ET

OWL, TARGETS, PREFIX, SEARCH = (os.environ.get(k, "") for k in ("OWL", "TARGETS", "PREFIX", "SEARCH"))
if not (OWL and PREFIX and (TARGETS or SEARCH)):
    sys.exit("set OWL=<file.owl> PREFIX=<e.g. FBbt> and TARGETS=<tsv or list of CURIEs> or SEARCH=<regex>")
SYN = ["{http://www.geneontology.org/formats/oboInOwl#}" + t for t in ("hasExactSynonym", "hasRelatedSynonym", "hasBroadSynonym", "hasNarrowSynonym")]
if SEARCH:
    rx = re.compile(SEARCH, re.I)
    hits = 0
    for ev, el in ET.iterparse(OWL, events=("end",)):
        if el.tag != "{http://www.w3.org/2002/07/owl#}Class":
            continue
        about = el.get("{http://www.w3.org/1999/02/22-rdf-syntax-ns#}about")
        if about is None:
            continue
        m = re.search(r"/" + re.escape(PREFIX) + r"_(\d+)$", about)
        if m:
            lab = el.find("{http://www.w3.org/2000/01/rdf-schema#}label")
            lab = lab.text if lab is not None and lab.text else ""
            dep = el.find("{http://www.w3.org/2002/07/owl#}deprecated")
            status = "DEPRECATED" if (dep is not None and (dep.text or "").strip().lower() == "true") else "ok"
            syns = [e.text for t in SYN for e in el.findall(t) if e.text]
            matched = lab if rx.search(lab) else next((x for x in syns if rx.search(x)), "")
            if matched:
                hits += 1
                print(f"{PREFIX}:{m.group(1)}\t{lab}\t{status}" + ("" if matched == lab else f"\tsynonym: {matched}"))
        el.clear()
    print(f"\n{hits} {PREFIX} terms match /{SEARCH}/")
    sys.exit(0)
wanted = set()
with open(TARGETS) as f:
    for line in f:
        cells = line.rstrip("\n").split("\t")
        for c in cells:
            c = c.strip()
            if re.fullmatch(re.escape(PREFIX) + r"[:_]\d+", c):
                wanted.add(c.replace("_", ":"))
if not wanted:
    sys.exit(f"no {PREFIX} terms found in {TARGETS}")
RDF_ABOUT = "{http://www.w3.org/1999/02/22-rdf-syntax-ns#}about"
OWL_CLASS = "{http://www.w3.org/2002/07/owl#}Class"
LABEL = "{http://www.w3.org/2000/01/rdf-schema#}label"
DEPR = "{http://www.w3.org/2002/07/owl#}deprecated"
found = {}
for ev, el in ET.iterparse(OWL, events=("end",)):
    if el.tag != OWL_CLASS:
        continue
    about = el.get(RDF_ABOUT)
    if about is None:                       # anonymous class inside a restriction: do not clear, its parent still needs it
        continue
    m = re.search(r"/" + re.escape(PREFIX) + r"_(\d+)$", about)
    if m and f"{PREFIX}:{m.group(1)}" in wanted:
        lab = el.find(LABEL); dep = el.find(DEPR)
        found[f"{PREFIX}:{m.group(1)}"] = ((lab.text if lab is not None else ""), (dep is not None and (dep.text or "").strip().lower() == "true"))
    el.clear()
bad = 0
for t in sorted(wanted):
    if t not in found:
        print(f"{t}\tMISSING"); bad += 1
    else:
        lab, dep = found[t]
        print(f"{t}\tfound\t{lab}\t{'DEPRECATED' if dep else 'ok'}")
        bad += dep
print(f"\n{len(wanted)} {PREFIX} terms checked: {len(found)} found, {sum(1 for v in found.values() if v[1])} deprecated, {len(wanted) - len(found)} missing")
sys.exit(1 if bad else 0)
