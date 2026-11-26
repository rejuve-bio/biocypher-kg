#!/usr/bin/env python3
"""Content fingerprint of a tissue -> ontology pickle (independent of the Python version that wrote it).  Usage: python3 pickle_fingerprint.py file.pkl [...]"""
import hashlib, pickle, sys
for path in sys.argv[1:]:
    d = pickle.load(open(path, "rb"))
    text = "\n".join(f"{k}\t{v}" for k, v in sorted(d.items()))
    print(f"{len(d)} entries  {hashlib.sha256(text.encode()).hexdigest()[:12]}  {path}")
