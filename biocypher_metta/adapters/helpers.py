from inspect import getfullargspec
import hashlib
from math import log10, floor, isinf
from liftover import get_lifter

import hgvs.dataproviders.uta
from hgvs.easy import parser
from hgvs.extras.babelfish import Babelfish
import time
import random
import functools

from contextlib import contextmanager

ALLOWED_ASSEMBLIES = ['GRCh38']
_lifters = {}

def retry_connection(max_retries=5, base_delay=1, max_delay=30):
    # exponential backoff
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            retries = 0
            while retries < max_retries:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    retries += 1
                    if retries == max_retries:
                        print(f"Failed after {max_retries} retries: {e}")
                        return None
                    
                    # Exponential backoff with jitter
                    delay = min(max_delay, base_delay * (2 ** retries))
                    jitter = random.uniform(0, 0.1 * delay)
                    sleep_time = delay + jitter
                    
                    print(f"Connection failed ({e}). Retrying in {sleep_time:.2f} seconds...")
                    time.sleep(sleep_time)
            return func(*args, **kwargs)
        return wrapper
    return decorator


@retry_connection()
def _raw_connect():
    return hgvs.dataproviders.uta.connect()

#context managed connection
@contextmanager
def uta_connection():
    conn = _raw_connect()
    try:
        yield conn
    finally:
        if conn:
            conn.close()


@retry_connection()
def get_hdp_connection():
    return hgvs.dataproviders.uta.connect()


def assembly_check(id_builder):
    def wrapper(*args, **kwargs):
        argspec = getfullargspec(id_builder)

        if 'assembly' in argspec.args:
            assembly_index = argspec.args.index('assembly')
            if assembly_index >= len(args):
                pass
            elif args[assembly_index] not in ALLOWED_ASSEMBLIES:
                raise ValueError('Assembly not supported')
        return id_builder(*args, *kwargs)

    return wrapper

#id string
@assembly_check
def build_variant_id(chr, pos_first_ref_base, ref_seq, alt_seq, assembly='GRCh38'):
    key = f"{str(chr).lower()}_{pos_first_ref_base}_{ref_seq}_{alt_seq}_{assembly}"
    return key


@assembly_check
def build_regulatory_region_id(chr, pos_start, pos_end, assembly='GRCh38'):
    return f"{chr}_{pos_start}_{pos_end}_{assembly}"

#cache the conversion
@functools.lru_cache(maxsize=10000)
def _hgvs_to_vcf_cached(hgvs_id, assembly):
    with uta_connection() as hdp:
        babelfish = Babelfish(hdp, assembly_name=assembly)
        return babelfish.hgvs_to_vcf(parser.parse(hgvs_id))


@assembly_check
def build_variant_id_from_hgvs(hgvs_id, validate=True, assembly='GRCh38'):
    if validate:
        try:
            chr, pos_start, ref, alt, var_type = _hgvs_to_vcf_cached(hgvs_id, assembly)
            
            if var_type in ('sub', 'delins'):
                return build_variant_id(chr, pos_start + 1, ref[1:], alt[1:])
            else:
                return build_variant_id(chr, pos_start, ref, alt)
        except Exception as e:
            print(f"HGVS validation failed: {e}")
            return None

    # Fast path (no DB)
    if not hgvs_id.startswith('NC_'):
        print('Error: wrong hgvs format.')
        return None

    try:
        chr_num_str = hgvs_id.split('.')[0].split('_')[1]
        chr_num = int(chr_num_str)
        chr = str(chr_num) if chr_num < 23 else 'X' if chr_num == 23 else 'Y'
        
        pos_ref, alt = hgvs_id.split('.')[2].split('>')
        pos_start = pos_ref[:-1]
        ref = pos_ref[-1]

        return build_variant_id(chr, pos_start, ref, alt)
    except Exception:
        print('Error: wrong hgvs format.')
        return None


# Arangodb converts a number to string if it can't be represented in signed 64-bit
# Using the approximation of a limit +/- 308 decimal points for 64 bits


def to_float(str):
    MAX_EXPONENT = 307

    number = float(str)

    if number == 0:
        return number

    if isinf(number) and number > 0:
        return float('1e307')

    if isinf(number) and number < 0:
        return float('1e-307')

    base10 = log10(abs(number))
    exponent = floor(base10)

    if abs(exponent) > MAX_EXPONENT:
        if exponent < 0:
            number = number * float(f'1e{abs(exponent) - MAX_EXPONENT}')
        else:
            number = number / float(f'1e{abs(exponent) - MAX_EXPONENT}')

    return number


def check_genomic_location(chr, start, end,
                           curr_chr, curr_start, curr_end):
    """
    Checks if the curr locations are within the specified locations (chr, start, end)
    If no chr is specified, then it returns True b/c that means we want to import all chromosomes
    Used when we want to filter the data imported from a file by location
    """
    if chr is None:  # import the data on all chromosomes
        return True
    else:  # filter by chromosome and (if specified) by location
        if chr != curr_chr:
            return False
        else:
            if start and end:
                if int(curr_start) >= start and int(curr_end) <= end:
                    return True
            elif start:
                if int(curr_start) >= start:
                    return True
            elif end:
                if int(curr_end) <= end:
                    return True
            else:
                return True
    return False


def convert_genome_reference(chr, pos, from_build='hg19', to_build='hg38'):
    """
    Convert a genomic coordinate from one reference build to another.

    Args:
        from_build (str): The reference build version to convert from (must be 'hg19' or 'hg38').
        to_build (str): The reference build version to convert to (must be 'hg19' or 'hg38', and different from `from_build`).
        chr (str): The chromosome identifier (e.g., 'chr1', 'chrX').
        pos (int): The genomic position on the chromosome.

    Returns:
        int: The converted genomic position in the target reference build, or None if the conversion fails.
    """
    if from_build not in ['hg19', 'hg38'] or to_build not in ['hg19', 'hg38'] or from_build == to_build:
        raise ValueError("Invalid reference build versions. 'from_build' and 'to_build' must be different and one of 'hg19' or 'hg38'.")

    lifter_key = f"{from_build}_{to_build}"

    # Initialize the lifter for the specified build conversion if not already cached
    if lifter_key not in _lifters:
        _lifters[lifter_key] = get_lifter(from_build, to_build)

    # Convert the chromosome identifier to a format compatible with the liftover library
    chr_no = chr.replace('chr', '').replace('ch', '')

    try:
        # Perform the liftover conversion using the cached lifter object
        converted = _lifters[lifter_key].query(chr_no, pos)[0][1]
        return int(converted)
    except:
        return None