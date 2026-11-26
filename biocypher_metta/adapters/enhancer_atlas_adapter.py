"""EnhancerAtlas 2.0 adapter for human (9606, GRCh38) and Drosophila melanogaster (7227, dm6).

WHAT IT READS
  The adapter does not read the files EnhancerAtlas distributes. It reads the output of the offline lift script
  (lift_enhancer_atlas.py), which moves the data from the builds EnhancerAtlas 2.0 uses to the builds BioAS uses:
    human  hg19 -> GRCh38     fly  dm3 -> dm6
  Per species the lifted folder holds
    bed/<tissue>.bed            one lifted BED per tissue/cell type: chr start end signal orig_chr orig_start orig_end (start is 0-based)
    enhancer_gene/<tissue>_EP.txt   enhancer-gene predictions, key  chr:start-end_GENE$symbol$gene_chr$TSS[$strand]  <TAB>  score
    ep_only_nodes.bed           EP enhancer intervals that are in no per-tissue BED (human only; empty for fly); used only if tissue_in_id is false
  and the tissue -> ontology pickle (aux_files/<species>/enhancer_atlas_tissues_to_ontology.pkl, values such as UBERON_0001134).

WHAT IT PRODUCES (one YAML record per kind; the kind is chosen by `label`)
  label enhancer       nodes  tissue_in_id true: one per (interval, tissue) of the per-tissue BEDs plus the (interval, EP tissue) pairs found
                              only in EP files; false: one per unique interval of all BEDs plus the ep_only nodes
  label enhancer_gene  edges  enhancer -> gene, properties score and biological_context (the tissue's ontology term)
  labels enhancer_anatomy | enhancer_cell_type | enhancer_cell_line | enhancer_tissue | enhancer_developmental_stage
                       edges  enhancer -> ontology term of the tissue ("active in"), property signal (the BED enrichment score).
                              One label per target node type, like the schema's "gene to pathway association" family: a record only writes
                              the tissues whose ontology term belongs to its node type (UBERON, FBbt = anatomy; CL = cell_type;
                              CLO = cell_line; BTO = tissue; FBdv = developmental_stage).

NODE ID
  SO:{chr}_{start}_{end}_{assembly}_{taxon_id}[_{tissue}]   start is 1-based (BED start + 1), end as in the BED.
  Human names keep the 'chr' prefix (GENCODE style); fly names are FlyBase notation without it (2L, mitochondrion_genome,
  Unmapped_Scaffold_8_D1580_D1567 ...). Contig names and tissue names can contain '_', so IDs are not meant to be split on '_'.
  The tissue is part of the ID while `tissue_in_id` is true (the current setting, decided "for now"): a node is then an interval IN a
  tissue, e.g. SO:chr1_101_200_GRCh38_9606_HepG2. The tissue token is the BED file name (EP file names are matched to it ignoring case:
  Hela-S3 -> HeLa-S3, th1 -> Th1). Consequence: an enhancer-gene edge starts at the node of its EP tissue, so every (EP tissue, interval)
  that is not in that tissue's BED gets a node of its own, created from the EP files (human: many, since only part of the EP pairs have
  their enhancer in the tissue's own BED; fly: none). With tissue_in_id false the ID has no tissue, identical intervals of different
  tissues collapse into one node, and the ep_only nodes file supplies the intervals that are in no BED.
  Only (9606, GRCh38) and (7227, dm6) are accepted, so coordinates cannot be labelled with the wrong assembly.

TISSUE NAMES are matched to the pickle ignoring case (BED HeLa-S3 / Th1 vs pickle Hela-S3 / th1). Names that collide when lowercased are an error.
A tissue without an ontology term is skipped and logged: its enhancer_gene file gives no edges (as before) and its active-in edges are not written
(the active-in warnings come from the enhancer_anatomy record only, so they are not repeated five times).
Terms are written as PREFIX:NNN (the pickle's PREFIX_NNN is converted); the writers handle the prefix.

SOURCE AND METHOD (verified in the papers/downloads; see the lift script for the evidence)
  Gao T, Qian J. EnhancerAtlas 2.0: an updated resource with enhancer annotation in 586 tissue/cell types across nine species.
    Nucleic Acids Res 2020;48(D1):D58-D64. doi:10.1093/nar/gkz980. PMID 31740966. Data builds (Materials and Methods, "Data sources"): human hg19, fly dm3.
  Gao T, Qian J. EAGLE: an algorithm that utilizes a small number of genomic features to predict tissue/cell type-specific enhancer-gene
    interactions. PLoS Comput Biol 2019;15(10):e1007436. doi:10.1371/journal.pcbi.1007436. PMID 31665135. Method behind the EP files of 2.0
    (EnhancerAtlas 1.0 used IM-PET: He B et al. PNAS 2014;111(21):E2191-E2199, doi:10.1073/pnas.1320308111, which the old code comment named).
  Gao T, He B, Liu S, Zhu H, Tan K, Qian J. EnhancerAtlas: a resource for enhancer annotation and analysis in 105 human cell/tissue types.
    Bioinformatics 2016;32(23):3543-3551. doi:10.1093/bioinformatics/btw495. PMID 27515742 (version 1).
  Gao T, Zheng Z, Pan Y, Zhu C, Wei F, Yuan J, Sun R, Fang S, Wang N, Zhou Y, Qian J. scEnhancer: a single-cell enhancer resource with annotation
    across hundreds of tissue/cell types in three species. NAR 2022;50(D1):D371-D379. doi:10.1093/nar/gkab1032. PMID 34761274. It also states
    GRCh37/hg19 and BDGP5/dm3 for human and fly (supporting evidence for the builds).
  Download page: http://singlecelldb.com/downloadv2.php (titled "EnhancerAtlas 2.0"). The README of github.com/EvansGao/EnhancerAtlas3 describes a 3.0
  release in hg38/dm6 (no publication found yet); no 3.0 data were used.
  Merged BED (hs.bed.gz / dm.bed, not read by this adapter): column 4 lists "length:score:tissue" entries, one per merged interval. The maintainer
  (Tianshun Gao) confirms "enhancer length, score and the relative tissue/cell type" in github.com/EvansGao/EnhancerAtlas3/issues/1 (13 Feb 2026), so
  the merged rows carry lengths, not positions: the per-tissue BEDs are the only place with each interval's coordinates.

THE LIFT (UCSC liftOver, minMatch 0.90, no -multiple, same chain for BEDs and EP files; chain md5s and counts are in each summary.txt)
  Intervals that do not lift are dropped with their nodes and edges and logged by the lift script. Human: 5,108 of 3,596,268 unique intervals;
  6,104 of 7,578,822 EP pairs. Fly: 22,009 of 294,157 intervals (97% on chrU/chrUextra); 1,854 of 317,588 EP pairs.
  The 48 fly contigs that chromAlias cannot name were mapped to FlyBase ids by identical sequence (md5), see build_dm6_contig_name_map.py.

THINGS TO KNOW WHEN USING THE DATA
  * score is the unnormalised output of a per-species model: human (Astrocyte file) 0.500105-3.816449, fly (all seven files) 20.0-27.948432.
    Scores are not comparable across species. The files seem to contain only predicted-positive pairs (unverified: question to the authors
    pending). No threshold is applied here.
  * biological_context comes from the EP file name and must be read with care for human. Checked on the original downloads (exact chr:start-end):
      - fly: all 7 EP files have 100% of their enhancers in the BED of the same tissue.
      - human, 110 EP files: 25 have >= 50% of their unique enhancers as exact intervals of the same tissue's BED and 33 have < 10% (by EP pairs
        instead of unique enhancers: 22 of 110 below 10%). Below 10%, e.g.: Lung, CD14+, CD36+, Thymus, PBMC, Calu-3, MS1.
      - some EP files hold many enhancers that are in no tissue BED at all (PBMC 41%, Calu-3 56%, MS1 62%); for those three tissues almost none are
        in the superseded .bed.old files either (2 of 135, 9 of 140, 0 of 8), so this is not a matter of an older BED version.
      - some EP files share the same enhancer list: seven tissues with no biological relation (CD36+, Fetal_heart, Fetal_kidney, HCT116, Liver,
        PANC-1, Pancreas; 220-842 enhancers) form a nested group (at least 199 of the 220 CD36+ enhancers are in the EP files of Liver, Pancreas,
        Fetal_heart and Fetal_kidney); eight GM* cell lines also share lists, which is plausible.
      - tissues with no "Data source" file on the download page are much more consistent with their own BED (median 64% of pairs vs 22%); unexplained.
    The cause is unknown (questions to the authors sent, no reply yet). active_in comes from the per-tissue BEDs and does not have this issue.
  * tissue_in_id true makes the EP files create their own nodes. Human (lifted data after the gene-id hold-out): 4,546,586 nodes, of which 4,029,881 come from
    the BED records and 516,705 exist only because an EP file lists that interval for that tissue (519,970 before the hold-out). Such a node asserts
    "interval in tissue" only through the EP file name, which the points above show is unreliable for many tissues. Fly: none.
  * Gene ids are written as they appear in the EP files the adapter is given (ENSEMBL: prefix, also for FlyBase ids); the adapter neither maps nor
    filters them. The input data should therefore carry the ids the graph's gene nodes use: lift_enhancer_atlas.py v1.7 can rewrite old ids with a
    map (GENE_ID_MAP), moves the lines of ids that are not current to enhancer_gene_held/ (not read here), and writes reports in gene_id_report/.
    Fly (lift of 2026-11-24, GENE_ID_COLLISION=dedupe_identical): GENE_IDS = the 24,254 ids of the Ensembl BDGP6.54 GTF; GENE_ID_MAP = 412 mappings
    from FlyBase FB2026_03 fbgn_annotation_ID (a secondary id with exactly one primary that is in the GTF). Of the 317,588 EP pairs: 300,903 had a
    current id, 10,755 were remapped, 4,076 (118 ids) were held out and 1,854 were dropped because the enhancer did not lift; 311,176 are written.
    The 118 held ids: 97 are absent from the FlyBase table (FBgn0085777 is "Withdrawn" in FlyBase; the others were not checked), 19 are split genes
    with two candidate primaries, 2 have a primary that is not in the GTF. Human (lift of 2026-11-24, GENE_IDS only, no map): GENE_IDS = the 78,733 ids of the GENCODE v50
    main GTF (md5 7e052412d94e9a184e2c0cabe574dc5e). Of the 7,578,822 EP pairs, 7,224,299 are written, 348,419 (3,635 ids, 4.6% of the pairs whose
    enhancer lifted) are held out and 6,104 were dropped because the enhancer did not lift. Remapping the 494 genes whose symbol has a unique current
    id is a documented option, not applied.
  * A remap can make several lines share tissue, enhancer and gene (fly: 760 groups). Lines with an identical score were collapsed (482 lines moved to
    enhancer_gene_duplicates/, the line that already had the current id is kept); 371 groups still have several lines because their scores differ
    (all kept; the policy for them is open).
  * Fly tissue -> ontology (dmel pickle: FBbt for anatomy and cell lines, FBdv for stages). The 'embryo_a-b' windows are hours after egg laying (the
    Resource files say "hours"/"hr"; checked on their first rows and on a descriptor summary of embryo_2-3 and embryo_6-8). FBdv has no terms for
    windows, so a window gets the narrowest FBdv term that contains it: embryo_6-8 -> late extended germ band stage (FBdv:00004450, stages 11-12;
    14 of its 15 source rows are 6-8 h or stage 11-12, one sci-ATAC row pools 2-12 h), embryo_2-3 -> blastoderm stage (FBdv:00005304, stages 4-5; the
    window overlaps stage 6 for about 10 of its 60 minutes), embryo_16-20 -> embryonic stage 17, embryo_NC14 -> embryonic cycle 14. embryo_1-3,
    embryo_2-4 and embryo_5-13 span several groups and stay on 'embryonic stage' (FBdv:00005289). Stage times come from a secondary reproduction of
    the Campos-Ortega and Hartenstein table, not from the OWL.
  * Fly cell lines: Kc167 and S2 map to their own FBbt cell-line terms (FBbt:00005349, FBbt:00005737). BG3-c2 and OSC map to the tissue they come from
    (larval central nervous system FBbt:00001919; somatic cell of ovariole FBbt:00006030), because FBbt has no term for either line (FBbt has 14 'cell-line'
    terms; only Kc167 and S2 are named; a generic 'CNS-derived cell-line' FBbt:00005816 exists). The two conventions are not consistent; this is a known
    simplification. Refinement options (BTO:0004801 "ML-DmBG3-c2 cell" exists; several terms per tissue would need list values in the pickle and a small
    change here) are listed in the design notes (enhancer_atlas_design_notes.md).
  * Lifting made some intervals overlap others of the same tissue (human 1,536, fly 2,474 intervals); they are kept as they are.

OPEN POINTS (kept explicit on purpose)
  * Tissues mapped to EFO or NCIT terms (human: CUTLL1, AML_blast, CD36+, CD20+) have no node type for the active-in edges yet: skipped and logged.
    Ontology terms are still being sought for them and for the 87 human tissues that have a BED but no EP file (and so no term). Their sources:
    CUTLL1 = the CUTLL1 cell line (GSE29600); CD36+ = erythroid progenitors derived from hematopoietic stem cells (GSE26501, GSE12646); AML_blast =
    pooled primary AML samples (215 of 218 datasets are chromatin-accessibility profiles); CD20+ has no source file. BTO_0003582 is SUP-T1, not CUTLL1.
  * The names of the five active-in input labels are a proposal (ACTIVE_IN_LABELS below); rename them there and in the schema together.
"""
import glob
import logging
import os
import pickle
import re
from collections import Counter

from biocypher_metta.adapters import Adapter
from biocypher_metta.adapters.helpers import check_genomic_location, to_float

log = logging.getLogger(__name__)

SUPPORTED_ASSEMBLY = {9606: "GRCh38", 7227: "dm6"}
# input_label of an active-in edge -> node type of its target
ACTIVE_IN_LABELS = {"enhancer_anatomy": "anatomy", "enhancer_cell_type": "cell_type", "enhancer_cell_line": "cell_line",
                    "enhancer_tissue": "tissue", "enhancer_developmental_stage": "developmental_stage"}
# ontology prefix -> node type of the target of an active-in edge (prefixes not listed have no node type yet)
TARGET_NODE_TYPE = {"UBERON": "anatomy", "FBBT": "anatomy", "CL": "cell_type", "CLO": "cell_line",
                    "BTO": "tissue", "FBDV": "developmental_stage"}
EP_KEY = re.compile(r"^([^:]+):(\d+)-(\d+)_([^$\s]+)\$")
SKIP_PREFIXES = ("#", "track", "browser")


class EnhancerAtlasAdapter(Adapter):
    # Coordinate convention of the source file(s); see README 'Genomic coordinates'.
    # Stored KG position = source position + offset.
    COORDINATES = {
        'source_convention': '0-based half-open',
        'source_assembly': {
            9606: 'GRCh38',
            7227: 'dm6',
        },
        'start_offset': 1,
        'end_offset': 0,
        'note': ('Reads the output of scripts/lift_enhancer_atlas.py, which lifts EnhancerAtlas 2.0 '
                 '(published on hg19 and dm3) offline with UCSC liftOver; BED starts and EP keys are 0-based.'),
    }

    def __init__(self, enhancer_dirpath, enhancer_gene_dirpath, ep_only_nodes_filepath, tissue_to_ontology_filepath,
                 write_properties, add_provenance, taxon_id, assembly, label,
                 chr=None, start=None, end=None, tissue_in_id=True):
        taxon_id = int(taxon_id)
        if SUPPORTED_ASSEMBLY.get(taxon_id) != assembly:
            raise ValueError(f"unsupported (taxon_id, assembly) = ({taxon_id}, {assembly!r}); supported: {SUPPORTED_ASSEMBLY}")
        for name, path in (("enhancer_dirpath", enhancer_dirpath), ("enhancer_gene_dirpath", enhancer_gene_dirpath)):
            # A wrong folder would otherwise glob to nothing and yield an empty graph without an error.
            if not os.path.isdir(path):
                raise FileNotFoundError(f"{name}: no such directory: {path}")
        self.enhancer_dirpath = enhancer_dirpath
        self.enhancer_gene_dirpath = enhancer_gene_dirpath
        self.ep_only_nodes_filepath = ep_only_nodes_filepath
        self.taxon_id = taxon_id
        self.assembly = assembly
        self.label = label
        self.chr = chr
        self.start = start
        self.end = end
        self.tissue_in_id = bool(tissue_in_id)

        self.source = 'Enhancer Atlas'
        self.version = '2.0'
        self.source_url = 'http://singlecelldb.com/downloadv2.php'

        with open(tissue_to_ontology_filepath, 'rb') as f:
            mapping = pickle.load(f)
        self.tissue_term = {}
        for name, term in mapping.items():
            if name.lower() in self.tissue_term:
                raise ValueError(f"tissue names collide when lowercased: {name!r}")
            self.tissue_term[name.lower()] = term
        # canonical tissue token = the BED file name; EP file names are matched to it ignoring case
        self._canonical = {os.path.basename(p)[:-len('.bed')].lower(): os.path.basename(p)[:-len('.bed')] for p in self._bed_files()}
        super(EnhancerAtlasAdapter, self).__init__(write_properties, add_provenance)

    # ------------------------------------------------------------------ helpers
    def _enhancer_id(self, chrom, start0, end, tissue=None):
        base = f"SO:{chrom}_{start0 + 1}_{end}_{self.assembly}_{self.taxon_id}"      # BED start is 0-based
        if self.tissue_in_id:
            if not tissue:
                raise ValueError("tissue_in_id is true but no tissue was given")
            base += f"_{self._canonical.get(tissue.lower(), tissue)}"
        return base

    def _provenance(self, props):
        if self.write_properties and self.add_provenance:
            props['source'] = self.source
            props['source_url'] = self.source_url

    @staticmethod
    def _intervals(path):
        """yield (chr, start0, end, fourth column or None) from a BED file; extra columns are ignored"""
        with open(path, newline='') as f:
            for line in f:
                line = line.rstrip('\r\n')
                if not line or line.startswith(SKIP_PREFIXES):
                    continue
                p = line.split('\t')
                yield p[0], int(p[1]), int(p[2]), (p[3] if len(p) > 3 else None)

    def _bed_files(self):
        return sorted(p for p in glob.glob(os.path.join(self.enhancer_dirpath, '*.bed')))

    def _in_region(self, chrom, start0, end):
        return check_genomic_location(self.chr, self.start, self.end, chrom, start0 + 1, end)

    @staticmethod
    def _curie(term):
        if ':' in term:
            return term
        prefix, _, number = term.partition('_')
        return f"{prefix}:{number}"

    # ------------------------------------------------------------------ nodes
    def _node(self, eid, chrom, start0, end):
        props = {}
        if self.write_properties:
            props['chr'] = chrom
            props['start'] = start0 + 1
            props['end'] = end
            props['taxon_id'] = self.taxon_id
            self._provenance(props)
        return eid, self.label, props

    def get_nodes(self):
        seen, stats = set(), Counter()
        for path in self._bed_files():
            tissue = os.path.basename(path)[:-len('.bed')]
            for chrom, start0, end, _ in self._intervals(path):
                eid = self._enhancer_id(chrom, start0, end, tissue)
                if eid in seen:
                    stats['repeated_id'] += 1
                    continue
                seen.add(eid)
                if not self._in_region(chrom, start0, end):
                    stats['outside_requested_region'] += 1
                    continue
                stats['nodes_from_beds'] += 1
                yield self._node(eid, chrom, start0, end)
        if self.tissue_in_id:
            # (interval, EP tissue) pairs that are not in that tissue's BED still need a node: the edge starts there
            for path in sorted(glob.glob(os.path.join(self.enhancer_gene_dirpath, '*_EP.txt'))):
                tissue = os.path.basename(path)[:-len('_EP.txt')]
                if not self.tissue_term.get(tissue.lower()):
                    continue                                        # no edges are written for this file either
                with open(path, newline='') as f:
                    for line in f:
                        m = EP_KEY.match(line)
                        if not m:
                            continue
                        chrom, start0, end = m.group(1), int(m.group(2)), int(m.group(3))
                        eid = self._enhancer_id(chrom, start0, end, tissue)
                        if eid in seen:
                            continue
                        seen.add(eid)
                        if not self._in_region(chrom, start0, end):
                            continue
                        stats['nodes_from_ep_files_only'] += 1
                        yield self._node(eid, chrom, start0, end)
        elif self.ep_only_nodes_filepath and os.path.exists(self.ep_only_nodes_filepath):
            for chrom, start0, end, _ in self._intervals(self.ep_only_nodes_filepath):
                eid = self._enhancer_id(chrom, start0, end)
                if eid in seen:
                    stats['repeated_id'] += 1
                    continue
                seen.add(eid)
                if not self._in_region(chrom, start0, end):
                    stats['outside_requested_region'] += 1
                    continue
                stats['nodes_ep_only'] += 1
                yield self._node(eid, chrom, start0, end)
        log.info("EnhancerAtlas %s nodes (tissue_in_id=%s): %s", self.assembly, self.tissue_in_id, dict(stats))

    # ------------------------------------------------------------------ edges
    def get_edges(self):
        if self.label == 'enhancer_gene':
            yield from self._enhancer_gene_edges()
        elif self.label in ACTIVE_IN_LABELS:
            yield from self._active_in_edges(ACTIVE_IN_LABELS[self.label])
        else:
            raise ValueError(f"unknown edge label {self.label!r}; expected 'enhancer_gene' or one of {sorted(ACTIVE_IN_LABELS)}")

    def _enhancer_gene_edges(self):
        stats = Counter()
        for path in sorted(glob.glob(os.path.join(self.enhancer_gene_dirpath, '*_EP.txt'))):
            tissue = os.path.basename(path)[:-len('_EP.txt')]
            context = self.tissue_term.get(tissue.lower())
            if not context:
                log.warning("EnhancerAtlas: no ontology term for EP tissue %r; its edges are skipped", tissue)
                stats['ep_files_without_ontology'] += 1
                continue
            with open(path, newline='') as f:
                for line in f:
                    line = line.rstrip('\r\n')
                    if not line:
                        continue
                    key, _, score_text = line.partition('\t')
                    m = EP_KEY.match(key)
                    if not m:
                        stats['unparseable_lines'] += 1
                        continue
                    chrom, start0, end, gene = m.group(1), int(m.group(2)), int(m.group(3)), m.group(4)
                    if not self._in_region(chrom, start0, end):
                        stats['outside_requested_region'] += 1
                        continue
                    props = {}
                    if self.write_properties:
                        try:
                            props['score'] = to_float(score_text)
                        except ValueError:
                            stats['bad_score'] += 1
                            continue
                        props['biological_context'] = context
                        props['taxon_id'] = self.taxon_id
                        self._provenance(props)
                    stats['edges'] += 1
                    yield self._enhancer_id(chrom, start0, end, tissue), f"ENSEMBL:{gene}", self.label, props
        log.info("EnhancerAtlas %s enhancer_gene edges: %s", self.assembly, dict(stats))

    def _active_in_edges(self, target_type):
        stats, warned = Counter(), set()
        reports = self.label == next(iter(ACTIVE_IN_LABELS))      # only the first record warns about unusable tissues
        for path in self._bed_files():
            tissue = os.path.basename(path)[:-len('.bed')]
            term = self.tissue_term.get(tissue.lower())
            if not term:
                if reports:
                    log.warning("EnhancerAtlas: no ontology term for BED tissue %r; active-in edges skipped", tissue)
                    stats['tissues_without_ontology'] += 1
                continue
            curie = self._curie(term)
            node_type = TARGET_NODE_TYPE.get(curie.split(':', 1)[0].upper())
            if node_type is None:
                if reports and curie not in warned:
                    log.warning("EnhancerAtlas: no node type for %s (tissue %r); active-in edges skipped", curie, tissue)
                    warned.add(curie)
                    stats['tissues_with_unsupported_ontology'] += 1
                continue
            if node_type != target_type:
                continue
            stats['tissues'] += 1
            for chrom, start0, end, signal in self._intervals(path):
                if not self._in_region(chrom, start0, end):
                    stats['outside_requested_region'] += 1
                    continue
                props = {}
                if self.write_properties:
                    if signal not in (None, ''):
                        props['signal'] = to_float(signal)
                    props['taxon_id'] = self.taxon_id
                    self._provenance(props)
                stats['edges'] += 1
                yield self._enhancer_id(chrom, start0, end, tissue), curie, self.label, props
        log.info("EnhancerAtlas %s %s edges: %s", self.assembly, self.label, dict(stats))
