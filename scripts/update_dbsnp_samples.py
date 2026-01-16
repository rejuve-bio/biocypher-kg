
import pickle
import os
import argparse
import sys

def update_dbsnp_samples(full_rsid_path, full_pos_path, sample_rsid_path, sample_pos_path):
    print("--- Starting dbSNP Sample Update ---")
    
    # Missing Data extracted from logs
    # RSIDs to extract
    missing_rsids = [
        # From roadmap_dhs
        'rs1000001', 'rs10000017', 'rs10000018', 'rs10000022', 
        'rs10000025', 'rs10000027', 'rs1000003', 'rs10000033',
        # From cadd
        'rs10000003', 
        # From refseq_closest_gene
        'rs1000000', 'rs10000005', 'rs10000006', 'rs10000008'
    ]
    
    # Positions to extract
    # The TOPLD adapter uses chr16 in the sample config.
    # keys are: f"{chr}_{pos}"
    missing_positions_raw = [
        10038, 594354, 10058, 346911, 552461, 579087, 784715, 820546, 865014,
        10150, 274065, 338483, 636735, 644697, 1003339, 1010086, 1010116,
        10226, 110182, 360020, 372632, 791688, 10256, 83291, 128921, 242469, 
        323931, 561021, 573808, 630531, 653346, 10386, 87085, 584765, 748761, 
        786151, 888446, 10424, 369091, 724173, 872575, 10556, 116821, 705040, 
        10639, 422637, 732540, 10825, 82870, 10856, 202802, 659705, 716722, 
        717773, 727985, 830362, 962278, 11187, 184274, 525023
    ]
    
    # Roadmap logs show errors for rs10000018 which is chr11_120468962 normally.
    # However, the user wants us to extract from "FULL" dicts. 
    # For TopLD positions, they appear to be relative to the chromosome the adapter is running on.
    # We will try 'chr16' first as it's the sample default.
    assumed_chr_topld = 'chr16' 

    # 1. Load Sample Dictionaries
    print(f"Loading sample rsids from: {sample_rsid_path}")
    with open(sample_rsid_path, 'rb') as f:
        sample_rsids = pickle.load(f)
        
    print(f"Loading sample positions from: {sample_pos_path}")
    with open(sample_pos_path, 'rb') as f:
        sample_pos = pickle.load(f)

    # 2. Load Full Dictionaries (might be large)
    print(f"Loading FULL rsids from: {full_rsid_path} (this may take time)...")
    try:
        with open(full_rsid_path, 'rb') as f:
            full_rsids = pickle.load(f)
    except Exception as e:
        print(f"ERROR: Could not load full RSID file: {e}")
        return

    print(f"Loading FULL positions from: {full_pos_path} (this may take time)...")
    try:
        with open(full_pos_path, 'rb') as f:
            full_pos = pickle.load(f)
    except Exception as e:
        print(f"ERROR: Could not load full Position file: {e}")
        return

    # 3. Patch RSIDs
    print("\nPatching RSIDs...")
    added_rsids = 0
    for rsid in missing_rsids:
        if rsid not in sample_rsids:
            if rsid in full_rsids:
                sample_rsids[rsid] = full_rsids[rsid]
                added_rsids += 1
                print(f"  Added {rsid}: {full_rsids[rsid]}")
            else:
                print(f"  WARNING: {rsid} not found in FULL dict.")
        else:
            print(f"  {rsid} already in sample.")
            
    print(f"Total RSIDs added: {added_rsids}")

    # 4. Patch Positions
    print("\nPatching Positions...")
    added_pos = 0
    for pos in missing_positions_raw:
        key = f"{assumed_chr_topld}_{pos}"
        if key not in sample_pos:
            if key in full_pos:
                sample_pos[key] = full_pos[key]
                added_pos += 1
                print(f"  Added {key}: {full_pos[key]}")
            else:
                # If not in chr16, maybe search other chromosomes?
                # For now, let's just log failure.
                print(f"  WARNING: {key} not found in FULL dict.")
        else:
            # print(f"  {key} already in sample.")
            pass
            
    print(f"Total Positions added: {added_pos}")

    print(f"\nSaving updated sample rsids to: {sample_rsid_path}")
    with open(sample_rsid_path, 'wb') as f:
        pickle.dump(sample_rsids, f)
        
    print(f"Saving updated sample positions to: {sample_pos_path}")
    with open(sample_pos_path, 'wb') as f:
        pickle.dump(sample_pos, f)
        
    print("\nUpdate Complete.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Patch sample dbSNP dictionaries with missing variants from full dictionaries.")
    parser.add_argument("--full-rsid", required=True, help="Path to full dbsnp_rsids.pkl")
    parser.add_argument("--full-pos", required=True, help="Path to full dbsnp_pos.pkl")
    parser.add_argument("--sample-rsid", required=True, help="Path to sample_dbsnp_rsids.pkl to update")
    parser.add_argument("--sample-pos", required=True, help="Path to sample_dbsnp_pos.pkl to update")
    
    args = parser.parse_args()
    
    update_dbsnp_samples(args.full_rsid, args.full_pos, args.sample_rsid, args.sample_pos)
