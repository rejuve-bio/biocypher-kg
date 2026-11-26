# EnhancerAtlas 2.0 (human and Drosophila)

Status (2026-11-24): dmel is verified from the offline lift to the adapter smoke test. Human has been lifted with the gene-id step (hold-out, no remap) its adapter smoke test passed and its 103 ontology targets exist in the OWL files;
a real build is pending. Facts below were measured in this work; items marked *not verified* were not.

## 1. Source and builds
- Source: EnhancerAtlas 2.0 downloads (http://singlecelldb.com/downloadv2.php): per-tissue enhancer BEDs, enhancer-gene (EP) files, merged BEDs.
- Builds: human hg19, fly dm3 (Gao and Qian, NAR 2020;48:D58-D64; scEnhancer, NAR 2022;50:D371-D379). BioAS uses GRCh38 and dm6.
- EnhancerAtlas 3.0 was not used (README only; no publication found). Merged BED column 4 = "length:score:tissue" (maintainer, EvansGao/EnhancerAtlas3 issue 1).

## 2. Offline step (outside the BioAS build)
[`scripts/lift_enhancer_atlas.py`](../../scripts/lift_enhancer_atlas.py) v1.7 (UCSC liftOver, minMatch 0.90, no -multiple, same chain for BEDs and EP files; intervals that do not lift are dropped and logged).
Optional gene-id step: GENE_IDS (current ids), GENE_ID_MAP (old to new), GENE_ID_NOTES, GENE_ID_COLLISION=dedupe_identical. Lines that cannot be given a current
id go to `enhancer_gene_held/` (not read by the adapter); collapsed duplicate lines go to `enhancer_gene_duplicates/`; reports in `gene_id_report/`.
Chain md5: dm3ToDm6 582f6f4067e41542a4b77a7401dadd45; hg19ToHg38 35887f73fe5e2231656504d1f6430900 (not yet compared with the UCSC md5sum.txt).
Human gene-id input: GENCODE v50 main GTF ids (78,733; md5 7e052412d94e9a184e2c0cabe574dc5e), no map.
Dmel gene-id inputs: Ensembl BDGP6.54 GTF ids (24,254; md5 1267ab0c5db932f1fe0cb28df1ae2e37), FlyBase FB2026_03 fbgn_annotation_ID map v2 (412 mappings;
md5 b64bf6536bede1488f89b6e78ce431d0), notes v2 (118 ids; md5 df82af03eb6855f6500839019f45dc76).

## 3. Model
- Node: `SO:{chr}_{start}_{end}_{assembly}_{taxon_id}[_{tissue}]`, start 1-based. Human keeps `chr`; fly uses FlyBase names (no `chr`). `tissue_in_id` is true "for now".
- Edges: `enhancer_gene` (score, biological_context), and five `active_in` families by target node type (anatomy, cell_type, cell_line, tissue, developmental_stage).
- Tissue to ontology: pickle (case-insensitive lookup). Human: UBERON, CL, CLO, BTO. Fly: FBbt, FBdv.

## 4. Results
| | dmel (final) | human (lifted, gene-id step not applied yet) |
|---|---|---|
| BED records, source to lifted | 294,158 to 272,149 | 4,035,437 to 4,029,881 |
| Nodes with tissue in the ID | 272,149 | 4,546,586 (4,029,881 from BEDs + 516,705 from EP files only; 519,970 before the hold-out) |
| EP pairs, source to lifted | 317,588 to 315,734 | 7,578,822 to 7,572,718 |
| Written / held / duplicates | 311,176 / 4,076 (118 ids) / 482 | 7,224,299 / 348,419 (3,635 ids) / 0 |
| enhancer_gene edges | 311,176 | 7,224,299 (40,101 distinct genes) |
| active_in | 195,138 anatomy + 77,011 developmental stage (6 FBdv terms) | 2,690,577: 648,775 anatomy (19 terms), 364,403 cell type (15), 1,089,661 cell line (49), 587,738 tissue (20), with pickle v2 |
Smoke tests (dmel and human): 0 duplicated node ids, 0 edges starting at a missing node. Dmel: the 12 FBbt and 6 FBdv targets exist and are not obsolete in the OWL files. Human (pickle v2): 103 distinct
ontology targets (UBERON 19, CL 15, CLO 49, BTO 20), all found and not obsolete in the OWL files (CL:2000001 and CL:0000540, added by pickle v2, were checked with the search mode). Human skips 91 tissues for active_in (87 without a term, 4 mapped to EFO/NCIT): 1,339,304 of 4,029,881 BED records (33.2%).

## 5. Decisions taken
- ID order `{chr}_{start}_{end}_{assembly}_{taxon_id}`; tissue appended for now.
- Collisions after a gene-id remap: identical scores are collapsed (lossless for the edges the adapter writes); different scores are all kept.
- Fly windows: embryo_6-8 to late extended germ band stage (FBdv:00004450), embryo_2-3 to blastoderm stage (FBdv:00005304); other windows stay on embryonic stage.
- Fly cell lines: Kc167 and S2 to their FBbt cell-line terms; BG3-c2 and OSC to their tissue of origin (anatomy). Known simplification (section 6).
- Human tissue terms reviewed with a one-off curation script, not kept in the repo (95 of 110 look alike, 11 flagged, 4 EFO/NCIT without OWL; none was a wrong term like BTO:0003582 = SUP-T1):
  PBMC changes from CLO:0037274 ("primary cultured" PBMC) to CL:2000001 (49 source rows describe PBMC from donors, none mentions culture; 20,788 edges move from cell_line to cell_type);
  ESC_neuron changes from CL:0002322 (embryonic stem cell) to CL:0000540 (neuron; the source rows say "Neurons", none says progenitor). Both are in pickle v2; expected after the change:
  cell_type 364,403 and cell_line 1,089,661 edges, 103 targets (19 + 15 + 49 + 20): confirmed by the smoke test.
- Fly Embryo_muscle stays on FBbt:00005647 (presumptive embryonic/larval muscle system, "the structures in the embryo that will develop into the embryonic/larval muscle system"): the source rows are
  Dmef2 muscle cells sorted from embryos at 6-8, 8-10, 10-12 and 14-17 h (stage 15 also appears). The later time points are already formed muscle, so the term is an approximation for them.

## 6. Open questions and future refinements
1. Human gene ids: 3,635 ids (348,419 pairs, 4.6%) are not in GENCODE v50 and are held out (the earlier audit counted 3,628 ids and 347,505 pairs on the pairs it could compare). Candidate (unofficial): remap 494 genes whose symbol has exactly one
   current id (64,980 pairs); 117 genes are ambiguous (9,718 pairs); 3,017 genes (272,807 pairs) have no symbol match. An official ID-history source was not found.
2. Dmel: 371 groups keep several lines with different scores for one tissue, enhancer and gene. 118 ids stay held (97 absent from the FlyBase table, e.g. FBgn0085777 is
   "Withdrawn"; 19 split genes; 2 with a primary outside the GTF).
3. Fly cell lines. BG3-c2 (31,290 edges) and OSC (7,697) point to their tissue, Kc167 and S2 to cell-line terms. Options: add a second target (needs list values in the pickle):
   `FBbt:00005816` CNS-derived cell-line (generic) or `BTO:0004801` "ML-DmBG3-c2 cell" (label verified; BTO covers all taxa, not only human). CLO and CL were searched
   for "BG3": no term in either, so no `cell_line` target exists for this line.
   Same rule for all lines (tissue of origin plus line term) would need the term for each. BG3 comes from the third-instar larval CNS (DGRC, FlyBase, Cellosaurus).
4. Fly embryo windows: embryo_1-3, 2-4, 5-13 span several FBdv substages and stay on the umbrella term; embryo_6-8 has one pooled sci-ATAC row (2-12 h) out of 15.
5. Human tissues: 87 tissues have a BED but no EP file and no ontology term (no active_in until mapped); 4 tissues map to EFO or NCIT, which have no node type yet
   (CUTLL1, AML_blast, CD36+, CD20+); `CD4+` is stored as BTO:0005453 (colon) while the others use underscores (cosmetic).
6. `tissue_in_id`: in human 58.9% of EP edges start at a node that exists only because of an EP file; the EP tissue label is unreliable for many tissues (section 7).
7. Mouse, rat, worm: builds, chains and ID maps not checked. snp_to_enhancer adapter not written.

## 7. Data caveats
- EP vs BED (human, exact intervals): 25 of 110 EP files have 50% or more of their enhancers in the same tissue's BED, 33 have under 10%; fly: all 7 are 100%.
  Not caused by the lift (same numbers before and after) nor by the superseded `.bed.old` files. Seven unrelated tissues (CD36+, Liver, Fetal_heart, Pancreas, PANC-1, Fetal_kidney,
  HCT116) have nested EP lists. Tissues with no Resource file on the download page are much more consistent with their own BED (median 64% against 22% of pairs); unexplained.
  Questions were sent to the authors (no reply yet).
- Score: unnormalised, per species (human Astrocyte 0.500105-3.816449; fly 20.0-27.948432); no threshold applied.

## 8. Reproducing the lifted input

Layout, relative to each species' `input_dir`:

| Folder | Content | Written by |
|---|---|---|
| `enhancer_atlas/non_lifted/` | Files as published (hg19 / dm3): `bed/<tissue>.bed`, `enhancer_gene/<tissue>_EP.txt`, merged `hs.bed.gz` / `dm.bed.gz` | The downloader (`subdir: non_lifted` in the data source config) |
| `enhancer_atlas/` | Lifted files the adapter reads: `bed/`, `enhancer_gene/`, `ep_only_nodes.bed`, plus reports (`summary.txt`, `unmapped_*.tsv`, `gene_id_report/`, ...) | `scripts/lift_enhancer_atlas.py` |

Step-by-step commands, expected counts and validation: [enhancer-atlas-liftover.md](enhancer-atlas-liftover.md).
