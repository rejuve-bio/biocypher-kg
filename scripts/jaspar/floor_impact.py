"""
Analysis script for the impact of probability floor values on JASPAR PWM calculation.

Evaluates how different floor values (1e-3, 1e-4, 1e-5, 1e-6) affect log-odds
calculations for zero and small probabilities in JASPAR letter-probability matrices.

Formula:
    PWM_ij = ln( max(P_ij, floor) / background_j )
where background_j = 0.25.
"""

import math


def evaluate_floors():
    bg = 0.25
    floors = [1e-2, 1e-3, 1e-4, 1e-5, 1e-6]
    test_probs = [0.0, 1e-5, 1e-4, 1e-3, 0.05, 0.25, 0.50, 0.85, 1.0]

    print("=" * 70)
    print("FLOOR IMPACT ON NATURAL LOG-ODDS: ln(max(P, floor) / 0.25)")
    print("=" * 70)
    print(f"{'Input P':<12}" + "".join(f"{f'floor={fl:<6}':<14}" for fl in floors))
    print("-" * 70)

    for p in test_probs:
        row_strs = [f"{p:<12.5f}"]
        for fl in floors:
            p_eff = max(p, fl)
            log_odds = math.log(p_eff / bg)
            row_strs.append(f"{log_odds:<14.4f}")
        print("".join(row_strs))

    print("-" * 70)
    print("Summary:")
    print("  - At floor=1e-4, P=0 yields -7.8240, closely matching observed empirical ranges.")
    print("  - Preserves dynamic range while avoiding math domain errors (log(0)).")
    print("=" * 70)


if __name__ == "__main__":
    evaluate_floors()

