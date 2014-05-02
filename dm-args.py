#!/usr/bin/python

import argparse
import os
import sys

SUFFIXES = ['KiB', 'MiB', 'GiB', 'TiB']

def readable_size(size):
  # Copyright (c) 2009, Mark Pilgrim, All rights reserved.
  assert size >= 0, "Number must be non-negative"
  for suffix in SUFFIXES:
    size /= 1024.0
    if size < 1024.0:
      return '{0:.1f} {1}'.format(size, suffix)
  assert False

def device_size(device):
  fd = os.open(device, os.O_RDONLY)
  try:
    return os.lseek(fd, 0, os.SEEK_END)
  finally:
    os.close(fd)

parser = argparse.ArgumentParser(
  description="Calculate device mapper arguments for a shingled device.")

parser.add_argument("device")
parser.add_argument("track_size_in_bytes", type=int)
parser.add_argument("band_size_in_tracks", type=int)
parser.add_argument("cache_percent", type=int)

args = parser.parse_args()

# Sanity checks.
assert args.track_size_in_bytes % 4096 == 0 and \
    512 * 1024 <= args.track_size_in_bytes <= 2 * 1024 * 1024, \
    "Invalid track size."
assert 20 <= args.band_size_in_tracks <= 200, "Invalid band size."
assert 1 <= args.cache_percent <= 20, "Invalid cache percent."

#total_size_in_bytes = device_size(args.device)
total_size_in_bytes = 350 * 1024 * 1024 * 1024
band_size_in_bytes = args.band_size_in_tracks * args.track_size_in_bytes
num_bands = total_size_in_bytes / band_size_in_bytes

num_cache_bands = num_bands * args.cache_percent / 100
cache_size_in_bytes = num_cache_bands * band_size_in_bytes

# Make |num_data_bands| a multiple of |num_cache_bands| so that all cache bands
# are equally loaded.
num_data_bands = (num_bands / num_cache_bands - 1) * num_cache_bands
data_size_in_bytes = num_data_bands * band_size_in_bytes
assert data_size_in_bytes % 512 == 0
num_usable_sectors = data_size_in_bytes / 512

wasted_size_in_bytes = total_size_in_bytes - cache_size_in_bytes - \
    data_size_in_bytes

print '# Total disk size: {} [{} bytes]'. \
    format(readable_size(total_size_in_bytes), total_size_in_bytes)
print '# Band size: {} [{} bytes]'. \
    format(readable_size(band_size_in_bytes), band_size_in_bytes)
print '# Total number of bands: {}'.format(num_bands)
print '# Number of cache bands: {}'.format(num_cache_bands)
print '# Cache size: {} [{} bytes]'. \
    format(readable_size(cache_size_in_bytes), cache_size_in_bytes)
print '# Number of data bands: {}'.format(num_data_bands)
print '# Usable disk size: {} [{} bytes]'. \
    format(readable_size(data_size_in_bytes), data_size_in_bytes)
print '# Wasted disk size: {} [{} bytes]'. \
    format(readable_size(wasted_size_in_bytes), wasted_size_in_bytes)
print
print 0, num_usable_sectors, 'shingle', \
    args.device, args.track_size_in_bytes, \
    args.band_size_in_tracks, args.cache_percent,
