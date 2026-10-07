"""
FlyBase regulatory-annotation adapter for *Drosophila melanogaster*.

Parses the whole-genome GFF3 from FlyBase and produces:

  - **Enhancer nodes** from ``regulatory_region`` entries whose ``Name=``
    contains "enhancer" and that carry an ``associated_genes=`` attribute.
  - **Enhancer → gene edges** linking each enhancer to its target gene(s)
    via the ``enhancer to gene association`` schema.
  - **Regulatory-region nodes** for non-TFBS regulatory features (insulator,
    protein_binding_site, regulatory_region, TSS).
  - **Gene → TFBS binding edges** connecting ``bound_moiety`` genes to
    location-based TF_binding_site nodes.
  - **Regulatory feature → SO edges** typing each feature to its
    Sequence Ontology term.

Source: https://s3ftp.flybase.org/genomes/Drosophila_melanogaster/current/gff/
"""

import gzip
import logging
import re
import tarfile
from io import TextIOWrapper
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

from biocypher_metta.adapters import Adapter
from biocypher_metta.adapters.helpers import build_regulatory_region_id

logger = logging.getLogger(__name__)

# Main chromosome arms only (per issue scope).
VALID_CHROMOSOMES: Set[str] = {"2L", "2R", "3L", "3R", "4", "X", "Y"}

# SO terms for the five regulatory feature types in scope.
SO_TERM_MAP: Dict[str, str] = {
    "TF_binding_site": "SO:0000235",
    "insulator": "SO:0000627",
    "protein_binding_site": "SO:0000410",
    "regulatory_region": "SO:0005836",
    "TSS": "SO:0000315",
}

# GFF3 attribute parser — semicolon-separated key=value pairs.
_ATTR_RE = re.compile(r"([^;=]+)=([^;]*)")


def _parse_attributes(attr_str: str) -> Dict[str, str]:
    """Parse a GFF3 attributes column into a dict."""
    return dict(_ATTR_RE.findall(attr_str))


def _parse_library(attrs: Dict[str, str]) -> Optional[str]:
    """Extract the library identifier (text after first ':') from ``library=``."""
    raw = attrs.get("library")
    if raw and ":" in raw:
        return raw.split(":", 1)[1]
    return raw


def _open_gff3(filepath: str):
    """
    Transparently open a FlyBase GFF3 file that may be:
      - a plain .gff file
      - a gzip-compressed .gff.gz
      - a gzipped *tarball* containing a single .gff member (FlyBase default)
    """
    path = Path(filepath)
    # Try tarball first (FlyBase ships .gff.gz as a gzipped tar).
    try:
        tf = tarfile.open(filepath, "r:gz")
        members = tf.getmembers()
        gff_members = [m for m in members if m.name.endswith(".gff")]
        if gff_members:
            f = tf.extractfile(gff_members[0])
            return TextIOWrapper(f, encoding="utf-8")
        tf.close()
    except (tarfile.TarError, Exception):
        pass

    # Fallback: plain gzip.
    if str(path).endswith(".gz"):
        return gzip.open(filepath, "rt", encoding="utf-8")

    # Plain text.
    return open(filepath, "r", encoding="utf-8")


class FlyBaseRegulatoryAdapter(Adapter):
    """
    Adapter for FlyBase whole-genome GFF3 regulatory annotations.

    Depending on the ``label`` parameter it emits different node/edge types:

    =============================================  ===================  ==========================
    label                                          get_nodes()          get_edges()
    =============================================  ===================  ==========================
    ``flybase_enhancer``                           enhancer nodes       —
    ``flybase_enhancer_gene``                      —                    enhancer → gene edges
    ``flybase_regulatory_region``                  reg-region nodes     —
    ``flybase_regulatory_region_so``               —                    reg-feature → SO edges
    ``flybase_gene_tfbs``                          —                    gene → TFBS edges
    =============================================  ===================  ==========================
    """

    # Drosophila melanogaster assembly identifier.
    DMEL_ASSEMBLY = "BDGP6"

    def __init__(
        self,
        write_properties,
        add_provenance,
        filepath: str,
        label: str = "flybase_enhancer",
        taxon_id: int = 7227,
    ):
        self.filepath = filepath
        self.label = label
        self.taxon_id = taxon_id

        self.source = "FlyBase"
        self.source_url = "https://flybase.org/"

        super().__init__(write_properties, add_provenance)

    # ------------------------------------------------------------------
    #  Internal helpers
    # ------------------------------------------------------------------
    @staticmethod
    def _is_enhancer(attrs: Dict[str, str]) -> bool:
        """Return True if this regulatory_region qualifies as an enhancer."""
        name = attrs.get("Name", "")
        return "enhancer" in name.lower() and attrs.get("associated_genes")

    def _iter_features(self):
        """Yield (chr, source, feature_type, start, end, attrs_dict) tuples."""
        with _open_gff3(self.filepath) as fh:
            for line in fh:
                line = line.rstrip("\r\n")
                if not line or line.startswith("#"):
                    continue
                parts = line.split("\t")
                if len(parts) < 9:
                    continue

                chrom = parts[0]
                if chrom not in VALID_CHROMOSOMES:
                    continue

                feature_type = parts[2]
                if feature_type not in SO_TERM_MAP:
                    continue

                start = int(parts[3])
                end = int(parts[4])
                attrs = _parse_attributes(parts[8])

                yield chrom, parts[1], feature_type, start, end, attrs

    # ------------------------------------------------------------------
    #  Public API: get_nodes / get_edges
    # ------------------------------------------------------------------
    def get_nodes(self):
        if self.label == "flybase_enhancer":
            yield from self._enhancer_nodes()
        elif self.label == "flybase_regulatory_region":
            yield from self._regulatory_region_nodes()

    def get_edges(self):
        if self.label == "flybase_enhancer_gene":
            yield from self._enhancer_gene_edges()
        elif self.label == "flybase_gene_tfbs":
            yield from self._gene_tfbs_edges()
        elif self.label == "flybase_regulatory_region_so":
            yield from self._regulatory_feature_so_edges()

    # ------------------------------------------------------------------
    #  Enhancer nodes + enhancer → gene edges
    # ------------------------------------------------------------------
    def _enhancer_nodes(self):
        """
        Yield enhancer nodes for regulatory_region entries whose Name contains
        "enhancer" and that have an ``associated_genes`` attribute.
        """
        seen = set()
        for chrom, _src, feat, start, end, attrs in self._iter_features():
            if feat != "regulatory_region":
                continue
            if not self._is_enhancer(attrs):
                continue

            fb_id = attrs.get("ID", "")
            if fb_id in seen:
                continue
            seen.add(fb_id)

            enhancer_id = f"FlyBase:{fb_id}"
            props = {}
            if self.write_properties:
                props["chr"] = chrom
                props["start"] = start
                props["end"] = end
                props["name"] = attrs.get("Name", "")
                props["taxon_id"] = self.taxon_id
                if self.add_provenance:
                    props["source"] = self.source
                    props["source_url"] = self.source_url

            yield enhancer_id, "enhancer", props

    def _enhancer_gene_edges(self):
        """
        Yield enhancer → gene edges.  One edge per associated gene.
        ``associated_genes`` value format: ``gene_symbol:FBgnXXXXXXX``
        (comma-separated when multiple).
        """
        for chrom, _src, feat, start, end, attrs in self._iter_features():
            if feat != "regulatory_region":
                continue
            if not self._is_enhancer(attrs):
                continue

            fb_id = attrs.get("ID", "")
            enhancer_id = f"FlyBase:{fb_id}"

            # Parse associated_genes — comma-separated, each is symbol:FBgnXXX
            for gene_entry in attrs["associated_genes"].split(","):
                gene_entry = gene_entry.strip()
                if ":" in gene_entry:
                    fbgn = gene_entry.split(":")[-1].strip()
                else:
                    fbgn = gene_entry.strip()

                if not fbgn.startswith("FBgn"):
                    continue

                gene_id = f"FlyBase:{fbgn}"
                props = {}
                if self.write_properties:
                    props["confirmed"] = True
                    props["taxon_id"] = self.taxon_id
                    if self.add_provenance:
                        props["source"] = self.source
                        props["source_url"] = self.source_url

                yield enhancer_id, gene_id, "enhancer_gene", props

    # ------------------------------------------------------------------
    #  Regulatory region nodes (excludes TF_binding_site and enhancers)
    # ------------------------------------------------------------------
    def _regulatory_region_nodes(self):
        """
        Yield regulatory_region nodes for non-TFBS and non-enhancer features.

        TF_binding_site entries are excluded (TFBS nodes use location-based
        IDs and a separate label).  Enhancer-qualifying regulatory_region
        entries are also excluded — they are emitted by ``_enhancer_nodes()``
        to avoid creating duplicate Neo4j nodes with the same FlyBase ID.
        """
        seen = set()
        for chrom, _src, feat, start, end, attrs in self._iter_features():
            if feat == "TF_binding_site":
                continue
            # Skip enhancer-qualifying entries (emitted by _enhancer_nodes).
            if feat == "regulatory_region" and self._is_enhancer(attrs):
                continue

            fb_id = attrs.get("ID", "")
            if fb_id in seen:
                continue
            seen.add(fb_id)

            node_id = f"FlyBase:{fb_id}"
            name = attrs.get("Name", "")
            library = _parse_library(attrs)

            props = {}
            if self.write_properties:
                props["chr"] = chrom
                props["start"] = start
                props["end"] = end
                props["name"] = name
                props["feature_type"] = feat
                props["so_term"] = SO_TERM_MAP.get(feat, "")
                props["taxon_id"] = self.taxon_id
                if library:
                    props["library"] = library
                if self.add_provenance:
                    props["source"] = self.source
                    props["source_url"] = self.source_url

            yield node_id, "regulatory_region", props

    # ------------------------------------------------------------------
    #  Gene → TFBS binding edges
    # ------------------------------------------------------------------
    def _gene_tfbs_edges(self):
        """
        For every TF_binding_site record, create a gene → TFBS edge from the
        ``bound_moiety`` gene (FBgn) to a location-based TFBS node ID.

        TFBS nodes use location-based IDs:
        ``FLYBASE_TFBS:{chr}_{start}_{end}_{BDGP6}``.
        """
        for chrom, _src, feat, start, end, attrs in self._iter_features():
            if feat != "TF_binding_site":
                continue

            bound = attrs.get("bound_moiety", "")
            if not bound:
                continue

            # bound_moiety format: gene_symbol:FBgnXXXXXXX
            if ":" in bound:
                fbgn = bound.split(":")[-1].strip()
            else:
                fbgn = bound.strip()

            if not fbgn.startswith("FBgn"):
                continue

            gene_id = f"FlyBase:{fbgn}"
            tfbs_id = f"FLYBASE_TFBS:{build_regulatory_region_id(chrom, start, end, self.DMEL_ASSEMBLY)}"

            source_fb_id = attrs.get("ID", "")

            props = {}
            if self.write_properties:
                props["source_id"] = source_fb_id
                props["taxon_id"] = self.taxon_id
                if self.add_provenance:
                    props["source"] = self.source
                    props["source_url"] = self.source_url

            yield gene_id, tfbs_id, "gene_tfbs", props

    # ------------------------------------------------------------------
    #  Regulatory feature → sequence type edges
    # ------------------------------------------------------------------
    def _regulatory_feature_so_edges(self):
        """
        For each regulatory feature, link it to the corresponding SO term
        node via a ``regulatory_feature_classified_as`` edge.

        Enhancer-qualifying entries are included here (they still get an SO
        typing edge even though their *node* is emitted as ``enhancer``).

        TFBS entries use a typed tuple source ``('tfbs', id)`` so the Neo4j
        loader can match nodes labelled ``tfbs`` rather than
        ``regulatory_region``.
        """
        seen = set()
        for chrom, _src, feat, start, end, attrs in self._iter_features():
            fb_id = attrs.get("ID", "")
            if fb_id in seen:
                continue
            seen.add(fb_id)

            so_id = SO_TERM_MAP.get(feat)
            if not so_id:
                continue

            # For TFBS, use location-based ID and typed tuple; for others,
            # use the FlyBase ID directly.
            if feat == "TF_binding_site":
                node_id = f"FLYBASE_TFBS:{build_regulatory_region_id(chrom, start, end, self.DMEL_ASSEMBLY)}"
            else:
                node_id = f"FlyBase:{fb_id}"

            so_node_id = ("sequence_type", so_id.replace(":", "_"))

            props = {}
            if self.write_properties:
                props["taxon_id"] = self.taxon_id
                if self.add_provenance:
                    props["source"] = self.source
                    props["source_url"] = self.source_url

            yield node_id, so_node_id, "regulatory_feature_classified_as", props
