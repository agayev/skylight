#!/usr/bin/python

import argparse
import os

SUFFIXES = {1000: ['KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB'],
            1024: ['KiB', 'MiB', 'GiB', 'TiB', 'PiB', 'EiB', 'ZiB', 'YiB']}

def approximate_size(size, a_kilobyte_is_1024_bytes=True):
  # Copyright (c) 2009, Mark Pilgrim, All rights reserved.
  assert size >= 0, "Number must be non-negative"
  multiple = 1024.0 if a_kilobyte_is_1024_bytes else 1000.0
  for suffix in SUFFIXES[multiple]:
    size /= multiple
    if size < multiple:
      return '{0:.1f} {1}'.format(size, suffix)
  assert False

def device_size(device):
  fd = os.open(device, os.O_RDONLY)
  try:
    return os.lseek(fd, 0, os.SEEK_END)
  finally:
    os.close(fd)

parser = argparse.ArgumentParser(description="Calculate the usable space " \
                                   "left on a device when emulating " \
                                   "shingling on top of it.");

parser.add_argument("device")
parser.add_argument("track_size_in_bytes", type=int)
parser.add_argument("band_size_in_tracks", type=int)
parser.add_argument("cache_percent", type=int)
parser.add_argument("-v", "--verbose", action="store_true")

args = parser.parse_args()

assert args.track_size_in_bytes % 4096 == 0, \
    "Track size must be a multiple of 4K."

total_size_in_bytes = device_size(args.device)
band_size_in_bytes = args.band_size_in_tracks * args.track_size_in_bytes
num_bands = total_size_in_bytes / band_size_in_bytes

num_cache_bands = num_bands * args.cache_percent / 100
cache_size_in_bytes = num_cache_bands * band_size_in_bytes

# Make |num_data_bands| a multiple of |num_cache_bands| so that all cache bands
# are equally loaded.
num_data_bands = (num_bands / num_cache_bands - 1) * num_cache_bands
data_size_in_bytes = num_data_bands * band_size_in_bytes

if args.verbose:
  print 'Total size:', approximate_size(total_size_in_bytes)
  print 'Band size:', approximate_size(band_size_in_bytes)
  print 'Total number of bands:', num_bands
  print 'Number of cache bands:', num_cache_bands
  print 'Cache size:', approximate_size(cache_size_in_bytes)
  print 'Number of data bands:', num_data_bands
  print 'Usable disk size:', approximate_size(data_size_in_bytes)
  print 'Wasted disk size due to alignment:', \
      approximate_size(total_size_in_bytes -
                       cache_size_in_bytes -
                       data_size_in_bytes)
else:
  print data_size_in_bytes,
