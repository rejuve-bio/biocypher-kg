# Genomic coordinate conventions: per-source verification

The KG stores every genomic position as **1-based closed**. The conventions,
the `COORDINATES` constant that each adapter declares, and its enforcement are
described in the README section
[Genomic coordinates](../../README.md#genomic-coordinates). This document keeps
the evidence for sources whose convention is not documented by the provider
and had to be established empirically.

---

## PEREGRINE

PEREGRINE does not document whether its enhancer coordinates are 0-based or 1-based, and its hg38 enhancer file (`PEREGRINEenhancershg38.gz`) **mixes conventions**: each record keeps the convention of its original sub-source, as listed in `PEREGRINEenhancersources.gz`. Most sub-sources are hg19-native, and PEREGRINE lifted them to hg38 without normalising their coordinates. `PEREGRINEAdapter` therefore applies a per-sub-source start offset (`START_OFFSET_BY_SOURCE`, documented in `COORDINATES`) to store all positions as 1-based closed.

These offsets were verified empirically with [`scripts/hsa/check_peregrine_coords.py`](../../scripts/hsa/check_peregrine_coords.py).

### What the script does

For each PEREGRINE sub-source, the script compares every record in the bulk files against the original source data, whose convention is known:

- **Positional test.** The reference is normalised to BED (0-based half-open) and, if it is hg19, lifted to hg38 with UCSC `liftOver`. Each PEREGRINE record is then matched to the reference interval whose start and end both lie within ±2 bp. The script tallies the offsets `(dStart, dEnd) = PEREGRINE − reference`. A dominant `(0, 0)` means 0-based half-open, and a dominant `(+1, 0)` means 1-based closed.
- **Length test.** This applies only where the original IDs survive in PEREGRINE (ENCODE `EH37E…`). It compares `end − start` on the raw numbers, so it does not depend on assembly or liftover.

The script reads the expected conventions from the adapter's `COORDINATES` dict (parsed with `ast`; the adapter is not imported) and reports PASS, FAIL or CHECK for each sub-source. A verdict requires at least 95% of matched records to fall in one offset class.

### Reference files

| Sub-source | Reference | Assembly | Format |
|---|---|---|---|
| ENCODE | `ENCFF788SJC.bed.gz` (ENCODE cCRE registry v1), https://www.encodeproject.org/files/ENCFF788SJC/ | hg19 | BED |
| FANTOM | `human_permissive_enhancers_phase_1_and_2.bed.gz`, https://fantom.gsc.riken.jp/5/datafiles/phase2.0/extra/Enhancers/ | hg19 | BED |
| Ensembl | `homo_sapiens.GRCh37.Regulatory_Build.regulatory_features.20161117.gff.gz` (release 87), https://ftp.ensembl.org/pub/grch37/release-87/regulation/homo_sapiens/ | hg19 | GFF |
| (liftover) | `hg19ToHg38.over.chain.gz` and the `liftOver` binary, https://hgdownload.soe.ucsc.edu/ | — | — |

VISTA was verified separately against [vista-data](https://gitlab.com/egsb-mfgl/vista-data): its `locus.tsv.gz` coordinates are 1-based (the repository's `locus_ucsc_hg38.bed` has the same loci with starts 1 lower), and 685 of PEREGRINE's 959 VISTA records match them exactly.

### Usage

```bash
python ./scripts/hsa/check_peregrine_coords.py \
  --enhancers PEREGRINEenhancershg38.gz \
  --sources   PEREGRINEenhancersources.gz \
  --adapter   biocypher_metta/adapters/hsa/peregrine_adapter.py \
  --ref ENCODE:bed:hg19:refs/ENCFF788SJC.bed.gz \
  --ref FANTOM:bed:hg19:refs/human_permissive_enhancers_phase_1_and_2.bed.gz \
  --ref Ensembl:gff:hg19:refs/homo_sapiens.GRCh37.Regulatory_Build.regulatory_features.20161117.gff.gz \
  --chain refs/hg19ToHg38.over.chain.gz --liftover-bin refs/liftOver \
  --report peregrine_coord_check.tsv
```

Each reference is given as `SOURCE:FORMAT:ASSEMBLY:PATH`, where `FORMAT` is one of `bed`, `gff` or `tsv1`, and `ASSEMBLY` is `hg19` or `hg38`.

### Results (November 2026, full bulk files)

| Sub-source | PEREGRINE records | Matched | Dominant offset | Convention | Adapter start offset | Status |
|---|---:|---:|---|---|:---:|:---:|
| ENCODE (positional) | 990,965 | 990,965 (100%) | `(0, 0)`: 100% | 0-based half-open | +1 | PASS |
| ENCODE (length) | 990,965 | 990,965 by ID | Δlen 0: 99.89% | 0-based half-open | +1 | PASS |
| FANTOM | 65,407 | 65,407 (100%) | `(0, 0)`: 100% | 0-based half-open | +1 | PASS |
| Ensembl | 28,207 | 28,173 (99.9%) | `(+1, 0)`: 99.58% | 1-based closed | 0 | PASS |
| VISTA | 959 | — | — | 1-based closed | 0 | Verified against vista-data |

Notes:

- **Same liftover as PEREGRINE.** For FANTOM, our liftover left 16 records unmapped, and PEREGRINE is missing exactly 16 FANTOM IDs from its hg38 file. Together with the exact matches, this indicates that PEREGRINE used UCSC `liftOver` with the standard hg19→hg38 chain.
- **ENCODE length test.** The ~0.1% of records with nonzero length deltas span small indels in the liftover chain; the positional test, which uses the same chain, matches all of them exactly.
- **Ensembl build.** The release-87 Regulatory Build matches 99.9% of PEREGRINE's Ensembl records. The current GRCh37 build (v114) matches only 4, so it is not the source build. The 34 Ensembl records missing from the matched count each matched two reference intervals equally well and were excluded from the tally. The small residual among matched records (0.4%, `(0, −1)` or `(0, 0)`) is consistent with PEREGRINE lifting 1-based values as if they were BED. This explanation has not been verified, and it does not affect the verdict.
- **Missing records.** 256 IDs in `PEREGRINEenhancersources.gz` have no row in the hg38 file (ENCODE 208, Ensembl 32, FANTOM 16), consistent with liftover failures.
