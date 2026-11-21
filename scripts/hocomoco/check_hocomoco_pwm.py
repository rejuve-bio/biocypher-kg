"""
Verification script for HOCOMOCO v11 Position Weight Matrices (PWM).

Verifies the mathematical relationship between the log-odds values stored in
HOCOMOCO .pwm files and nucleotide position probabilities:
    PWM_ij = ln( P_ij / B_j )
where B_j = 0.25 (uniform background frequency).
Inverting this relationship gives:
    P_ij = B_j * exp(PWM_ij) = 0.25 * exp(PWM_ij)
For every valid position i, the sum across the 4 nucleotides (A, C, G, T)
must equal 1.0 (within float rounding tolerance).
"""

import csv
import math
import os
import sys
from pathlib import Path
from typing import Tuple


def check_pwm_file(filepath: Path) -> Tuple[bool, float, int]:
    """Check a single HOCOMOCO .pwm file."""
    with open(filepath, "r", encoding="utf-8") as f:
        lines = f.readlines()

    if not lines:
        return False, 0.0, 0

    # First line is header, e.g. >AHR_HUMAN.H11MO.0.B
    header = lines[0].strip()
    max_dev = 0.0
    rows_checked = 0

    for line_idx, line in enumerate(lines[1:], start=2):
        parts = line.strip().split()
        if len(parts) != 4:
            continue
        pwm_vals = [float(p) for p in parts]
        # Invert natural log-odds with uniform background 0.25
        probs = [0.25 * math.exp(v) for v in pwm_vals]
        prob_sum = sum(probs)
        dev = abs(prob_sum - 1.0)
        if dev > max_dev:
            max_dev = dev
        rows_checked += 1

    valid = max_dev < 1e-3 and rows_checked > 0
    return valid, max_dev, rows_checked


def main():
    sample_dir = Path("samples/hsa/hocomoco/pwm")
    if not sample_dir.exists():
        print(f"Sample directory not found: {sample_dir}")
        return

    pwm_files = sorted(sample_dir.glob("*.pwm"))
    print(f"Found {len(pwm_files)} HOCOMOCO .pwm files in {sample_dir}")

    all_valid = True
    overall_max_dev = 0.0
    total_positions = 0

    for pwm_path in pwm_files:
        valid, max_dev, n_pos = check_pwm_file(pwm_path)
        if max_dev > overall_max_dev:
            overall_max_dev = max_dev
        total_positions += n_pos
        if not valid:
            print(f"FAILED: {pwm_path.name} (max dev: {max_dev:.6f})")
            all_valid = False

    print(f"Checked {total_positions} positions across {len(pwm_files)} models.")
    print(f"Maximum deviation from probability sum 1.0: {overall_max_dev:.6e}")
    if all_valid:
        print("SUCCESS: All HOCOMOCO PWM matrices confirm natural log-odds: ln(P / 0.25).")
    else:
        sys.exit(1)


if __name__ == "__main__":
    from typing import Tuple
    main()
