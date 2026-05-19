#!/usr/bin/env python3
"""Source file inspector for BioCypher data files.

Analyzes data files to extract structural metadata including:
- Compression format (gzip vs plain text)
- Comment line count
- Headers or generated column indices
- Delimiter detection
- Sample data rows
"""

import gzip
import json
import sys
import pickle
from pathlib import Path
from typing import List, Tuple, Dict, Any
import csv


class SourceInspector:
    """Inspects data files to extract structural metadata."""
    
    def __init__(self, filepath: str):
        """Initialize inspector with file path.
        
        Args:
            filepath: Path to the data file to inspect
        """
        self.filepath = Path(filepath)
        if not self.filepath.exists():
            raise FileNotFoundError(f"File not found: {filepath}")
    
    def inspect(self) -> Dict[str, Any]:
        """Analyze file and return metadata with multi-line delimiter validation.
        
        Returns:
            dict: {
                "compression": "gzip" | "none",
                "comment_lines": int,
                "headers": List[str],
                "delimiter": str,
                "sample_rows": List[List[str]],
                "has_header": bool,
                "delimiter_confidence": float (0.0-1.0)
            }
        """
        compression = self.detect_compression()
        
        # Open file with appropriate handler
        if compression == "gzip":
            file_handle = gzip.open(self.filepath, 'rt', encoding='utf-8')
        else:
            file_handle = open(self.filepath, 'r', encoding='utf-8')
        
        try:
            comment_lines = self.count_comment_lines(file_handle)
            
            # Reset file to beginning
            file_handle.seek(0)
            
            # Skip comment lines
            for _ in range(comment_lines):
                file_handle.readline()
            
            # Extract headers or generate indices
            headers, has_header = self.extract_headers(file_handle)
            
            file_handle.seek(0)
            for _ in range(comment_lines):
                file_handle.readline()
            
            candidate_lines = []
            if has_header:
                candidate_lines.append(file_handle.readline().strip()) 
            
            for _ in range(5):
                line = file_handle.readline().strip()
                if line:
                    candidate_lines.append(line)
                else:
                    break
            
            # Detect delimiter with multi-line validation
            delimiter, confidence = self._detect_delimiter_with_validation(candidate_lines)
            
            # Extract sample rows
            file_handle.seek(0)
            for _ in range(comment_lines):
                file_handle.readline()
            
            # Skip header if present
            if has_header:
                file_handle.readline()
            
            sample_rows = self.extract_samples(file_handle, delimiter)
            
            warnings = []
            if sample_rows and headers:
                expected_cols = len(headers)
                actual_col_counts = [len(row) for row in sample_rows]
                min_cols = min(actual_col_counts) if actual_col_counts else 0
                max_cols = max(actual_col_counts) if actual_col_counts else 0
                
                if min_cols != expected_cols or max_cols != expected_cols:
                    if min_cols == max_cols:
                        warnings.append(
                            f"Column count mismatch: Header has {expected_cols} columns, "
                            f"but all data rows have {min_cols} columns. "
                            f"Missing columns: {', '.join(headers[min_cols:])}"
                        )
                    else:
                        warnings.append(
                            f"Inconsistent column counts: Header has {expected_cols} columns, "
                            f"but data rows vary from {min_cols} to {max_cols} columns. "
                            f"This file has variable-length rows."
                        )
            
            return {
                "compression": compression,
                "comment_lines": comment_lines,
                "headers": headers,
                "delimiter": delimiter,
                "delimiter_confidence": confidence,
                "sample_rows": sample_rows,
                "has_header": has_header,
                "warnings": warnings
            }
        finally:
            file_handle.close()

    
    def detect_compression(self) -> str:
        """Detect if file is gzip compressed.
        
        Returns:
            "gzip" if compressed, "none" otherwise
        """
        # Check extension first
        if self.filepath.suffix == '.gz':
            try:
                with gzip.open(self.filepath, 'rt') as f:
                    f.read(1)  # Try to read one character
                return "gzip"
            except (gzip.BadGzipFile, OSError):
                return "none"
        return "none"
    
    def count_comment_lines(self, file_handle) -> int:
        """Count leading lines that are true comments, not the column-header line.

        Rules (applied in order):
        1. ``##`` lines (double-hash) are ALWAYS meta-information comments
           (VCF, GFF3, BCF, etc.) – count them unconditionally.
        2. ``!`` lines (GAF format) are always comments – count them.
        3. A single-``#`` line is the column header – stop counting and do NOT
           include it in the comment count.
        4. Any other non-empty line is data – stop counting.

        Args:
            file_handle: Open file handle

        Returns:
            Number of comment lines (the column-header ``#CHROM`` / ``#ID``
            line is NOT included in this count).
        """
        count = 0
        file_handle.seek(0)
        for line in file_handle:
            stripped = line.strip()
            if not stripped:         
                count += 1
                continue

            if stripped.startswith('##'):
                count += 1
                continue

            if stripped.startswith('!'):
                count += 1
                continue

            # Skip BED-style track and browser lines
            if stripped.lower().startswith('track') and (len(stripped) == 5 or stripped[5] in (' ', '\t')):
                count += 1
                continue

            if stripped.lower().startswith('browser') and (len(stripped) == 7 or stripped[7] in (' ', '\t')):
                count += 1
                continue

            if stripped.startswith('#'):
                break

            break

        return count
    
    def _looks_like_header_line(self, line: str) -> bool:
        """Check if a line looks like column headers.
        
        Args:
            line: Line to check (without # prefix)
            
        Returns:
            True if line looks like column headers
        """
        if not line:
            return False
        
        # Check for common delimiters
        delimiters = ['\t', ',', '|', ';']
        has_delimiter = any(delim in line for delim in delimiters)
        
        if not has_delimiter:
            return False
        
        # Detect delimiter and split
        delimiter = self.detect_delimiter(line)
        columns = [col.strip() for col in line.split(delimiter)]
        
        if len(columns) < 2:
            return False
        
        # Check if columns look like header names (mostly non-numeric, reasonable length)
        header_indicators = 0
        for col in columns:
            if not col:
                continue
            
            if (any(c.isalpha() for c in col) and 
                len(col) <= 50 and
                not col.isdigit()):
                header_indicators += 1
        
        # If most columns look like headers, this is likely a header line
        return header_indicators >= len(columns) * 0.6
    
    def extract_headers(self, file_handle) -> Tuple[List[str], bool]:
        """Extract headers or generate column indices with robust heuristics.
        
        Args:
            file_handle: Open file handle positioned after comment lines
            
        Returns:
            Tuple of (headers_list, has_header_bool)
        """
        # Read first two non-empty, non-comment lines
        first_line = file_handle.readline().strip()
        
        # Skip empty lines at the beginning
        while first_line == '':
            first_line = file_handle.readline().strip()
            if not first_line:  # EOF reached
                return [], False
        
        second_line = file_handle.readline().strip()
        
        while second_line == '':
            second_line = file_handle.readline().strip()
            if not second_line: 
                break
        
        if first_line.startswith('#'):
            header_line = first_line[1:].strip()
            delimiter = self.detect_delimiter(header_line)
            columns = [col.strip() for col in header_line.split(delimiter)]
            return columns, True
        
        delimiter = self.detect_delimiter(first_line)
        header_cols = [c.strip() for c in first_line.split(delimiter)]
        
        if not second_line:
            # Single line file - check if it looks like a header
            return header_cols, self._looks_like_header_line(first_line)
            
        first_data_cols = [c.strip() for c in second_line.split(delimiter)]
        
        numeric_in_both = 0
        for i in range(min(len(header_cols), len(first_data_cols))):
            h_val = header_cols[i]
            d_val = first_data_cols[i]
            if h_val.replace('.','').isdigit() and d_val.replace('.','').isdigit():
                numeric_in_both += 1
        
        non_numeric_headers = sum(1 for c in header_cols if not c.replace('.', '').replace('-', '').isdigit())
        
        has_header = False
        if numeric_in_both == 0 and non_numeric_headers > len(header_cols) / 2:
            has_header = True
            
        if has_header:
            return header_cols, True
        else:
            return [str(i) for i in range(len(header_cols))], False
    
    def detect_delimiter(self, line: str) -> str:
        """Detect delimiter character from a line with multi-strategy validation.
        
        Uses 4-layer validation strategy:
        1. Count-based detection (fast path for obvious cases)
        2. Consistency validation (check if delimiter creates consistent columns)
        3. csv.Sniffer intelligent detection (pattern analysis)
        4. Fallback to highest count
        
        Args:
            line: A line from the file
            
        Returns:
            Detected delimiter character
        """
        if not line or len(line) < 2:
            return '\t'  # Default to tab for empty/short lines
        
        delimiters = {
            '\t': line.count('\t'),
            ',': line.count(','),
            '|': line.count('|'),
            ';': line.count(';')
        }
        
        if delimiters['\t'] >= 3:
            tab_split = line.split('\t')
            if self._validate_delimiter_split(tab_split):
                return '\t'
        
        try:
            sniffer = csv.Sniffer()
            detected = sniffer.sniff(line, delimiters='\t,|;').delimiter
            
            split_result = line.split(detected)
            if self._validate_delimiter_split(split_result):
                return detected
        except Exception:
            pass 
        
        best_delimiter = '\t'
        best_score = -1
        
        for delim, count in delimiters.items():
            if count == 0:
                continue
            
            split_result = line.split(delim)
            score = self._score_delimiter_consistency(split_result, count)
            
            if score > best_score:
                best_score = score
                best_delimiter = delim
        
        if best_score <= 0 or len(line.split(best_delimiter)) < 2:
            space_split = line.split(' ')
            if len(space_split) >= 3 and self._validate_delimiter_split(space_split):
                return ' '
        
        return best_delimiter
    
    def _validate_delimiter_split(self, split_result: List[str]) -> bool:
        """Validate if a delimiter split creates reasonable columns.
        
        Args:
            split_result: Result of line.split(delimiter)
            
        Returns:
            True if split looks valid, False otherwise
        """
        # Must have at least 2 columns
        if len(split_result) < 2:
            return False
        
        # Check for empty columns (indicates wrong delimiter)
        empty_count = sum(1 for col in split_result if not col or col.isspace())
        if empty_count > len(split_result) * 0.3:  
            return False
        
        non_empty = [col for col in split_result if col and not col.isspace()]
        if non_empty:
            avg_len = sum(len(col) for col in non_empty) / len(non_empty)
            max_len = max(len(col) for col in non_empty)
            
            if max_len > avg_len * 5:
                return False
        
        return True
    
    def _score_delimiter_consistency(self, split_result: List[str], delimiter_count: int) -> float:
        """Score how consistent a delimiter split is.
        
        Higher score = better delimiter choice.
        
        Args:
            split_result: Result of line.split(delimiter)
            delimiter_count: Number of times delimiter appears
            
        Returns:
            Consistency score (0.0 to 1.0+)
        """
        if len(split_result) < 2:
            return 0.0
        
        column_score = len(split_result) / 10.0 
        
        non_empty = [col for col in split_result if col and not col.isspace()]
        if not non_empty:
            return 0.0
        
        lengths = [len(col) for col in non_empty]
        avg_len = sum(lengths) / len(lengths)
        variance = sum((l - avg_len) ** 2 for l in lengths) / len(lengths)
        
        consistency_score = 1.0 / (1.0 + variance / (avg_len + 1))
        
        empty_count = sum(1 for col in split_result if not col or col.isspace())
        empty_penalty = empty_count / len(split_result)
        
        # Combined score
        total_score = (column_score * 0.4) + (consistency_score * 0.5) - (empty_penalty * 0.1)
        
        return max(0.0, total_score)
    
    def _detect_delimiter_with_validation(self, lines: List[str]) -> Tuple[str, float]:
        """Detect delimiter using multiple lines for validation.
        
        Analyzes multiple lines to ensure delimiter consistency across the file.
        
        Args:
            lines: List of lines to analyze (header + data lines)
            
        Returns:
            Tuple of (delimiter, confidence_score)
            - delimiter: The detected delimiter character
            - confidence: Confidence score (0.0 to 1.0)
        """
        if not lines:
            return '\t', 0.5

        non_empty_lines = [l for l in lines if l]
        if non_empty_lines:
            tab_counts = [l.count('\t') for l in non_empty_lines]
            if all(c >= 3 for c in tab_counts):
                data_lines = non_empty_lines[1:] if len(non_empty_lines) > 1 else non_empty_lines
                col_counts = [len(l.split('\t')) for l in data_lines]
                if col_counts and max(col_counts) - min(col_counts) <= 2:
                    confidence = min(1.0, sum(tab_counts) / (len(tab_counts) * 10))
                    return '\t', max(0.5, confidence)
        
        delimiter_scores = {}
        for delim in ['\t', ',', '|', ';']:
            scores = []
            column_counts = []
            
            for line in lines:
                if not line:
                    continue
                
                split_result = line.split(delim)
                
                if len(split_result) < 2:
                    scores.append(0.0)
                    continue
                
                if not self._validate_delimiter_split(split_result):
                    scores.append(0.0)
                    continue
                
                # Score the consistency
                score = self._score_delimiter_consistency(split_result, line.count(delim))
                scores.append(score)
                column_counts.append(len(split_result))
            
            if not scores or all(s == 0.0 for s in scores):
                delimiter_scores[delim] = 0.0
                continue
            
            avg_score = sum(scores) / len(scores)
            
            
            if column_counts and len(set(column_counts)) == 1:
                avg_score *= 1.2  
            
            delimiter_scores[delim] = avg_score
        
        if not delimiter_scores or all(v == 0.0 for v in delimiter_scores.values()):
            return self.detect_delimiter(lines[0]), 0.5
        
        best_delim = max(delimiter_scores, key=delimiter_scores.get)
        best_score = delimiter_scores[best_delim]
       
        if best_score <= 0 or len(non_empty_lines[0].split(best_delim)) < 2:
            space_col_counts = [len(l.split(' ')) for l in non_empty_lines if l]
            if (space_col_counts and
                    min(space_col_counts) >= 3 and
                    max(space_col_counts) - min(space_col_counts) <= 2):
                return ' ', 0.5
        
        confidence = min(1.0, best_score)
        return best_delim, confidence
    
    def extract_samples(self, file_handle, delimiter: str, num_samples: int = 5) -> List[List[str]]:
        """Extract sample data rows.
        
        Args:
            file_handle: Open file handle positioned at first data row
            delimiter: Delimiter character
            num_samples: Number of sample rows to extract
            
        Returns:
            List of sample rows (each row is a list of column values)
        """
        samples = []
        for line in file_handle:
            stripped = line.strip()
            if not stripped:  # Skip empty lines
                continue
            
            # Split by delimiter
            row = [col.strip() for col in stripped.split(delimiter)]
            samples.append(row)
            
            if len(samples) >= num_samples:
                break
        
        return samples

    @staticmethod
    def is_inspectable_path(path_str: str) -> bool:
        if not isinstance(path_str, str):
            return False
        
        path_str = path_str.lower()
        
        valid_extensions = (
            '.gz', '.csv', '.tsv', '.txt',
            '.pkl', '.pickle', '.gtf', '.gff', '.bed', '.dat', '.vcf',
        )
        
        return path_str.endswith(valid_extensions)

    @staticmethod
    def find_all_paths(data: Any, current_label: str = "") -> List[Dict[str, str]]:
        """Recursively find all inspectable file paths in a nested config structure.

        Args:
            data: dict, list, or str to search
            current_label: dotted key path accumulated so far (for labelling)

        Returns:
            List of {"path": str, "label": str}
        """
        paths: List[Dict[str, str]] = []

        if isinstance(data, str):
            if SourceInspector.is_inspectable_path(data):
                paths.append({"path": data, "label": current_label})
        elif isinstance(data, dict):
            for k, v in data.items():
                new_label = f"{current_label}.{k}" if current_label else str(k)
                paths.extend(SourceInspector.find_all_paths(v, new_label))
        elif isinstance(data, list):
            for i, v in enumerate(data):
                new_label = f"{current_label}[{i}]"
                paths.extend(SourceInspector.find_all_paths(v, new_label))

        return paths


