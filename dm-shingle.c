#include <linux/device-mapper.h>

#include <linux/module.h>
#include <linux/init.h>
#include <linux/bio.h>
#include <linux/bitops.h>

#define DM_MSG_PREFIX "shingle"

#define TOTAL_SIZE (76LL << 10)
#define EXTERNAL_SECTOR_SIZE 512
#define INTERNAL_SECTOR_SIZE 4096

struct cache_band {
  sector_t begin_sector;    /* Where cache band begins. */
  sector_t current_sector;  /* Where the next write will go. */

  /* There are usually multiple data bands assigned to each band.
   * |start_data_band| is the index of the starting data band assigned to this
   * cache band.  The index of the next data band assigned to this cache band is
   * |start_data_band + sc->num_cache_bands|.  See below for its use. */
  int32_t start_data_band;

  /* Although multiple data bands are assigned to a cache band, not all of them
   * may have sectors on the cache band.  |bitmap| keeps track of data bands
   * having sectors on this cache band.  This is used during garbage
   * collection.
   *
   * Assume there are 6 data bands and 2 cache bands.  Then data bands 0, 2, 4
   * are assigned to cache band 0 and data bands 1, 3, 5 are assigned to cache
   * band 1 (using modulo arithmetic).  Consider cache band 1.  If it contains
   * sectors only for data band 5, then |bitmap| will be "001", if it contains
   * sectors for data bands 1 and 3, then |bitmap| will be "110".  That is,
   * given data band, its position in the the bitmap is calculated as below, and
   * the bit in that position is set:
   *
   * |position = (data_band - start_data_band) / sc->num_cache_bands|
   */
  unsigned long *bitmap;
};

struct shingle_c {
  struct dm_dev *dev;
  int32_t track_size_in_bytes;
  int32_t band_size_in_tracks;
  int32_t band_size_in_sectors;
  int32_t cache_percent;
  int32_t num_valid_sectors;  /* Number of valid sectors, including cache. */
  int32_t num_usable_sectors; /* Number of usable sectors, excluding cache. */
  int32_t *sector_map;
  struct cache_band *cache_bands;

  char *rmw_buffer;           /* Buffer used for read-modify-write. */
  
  int64_t total_size_in_bytes;
  int64_t cache_size_in_bytes;
  int64_t band_size_in_bytes;
  int64_t usable_size_in_bytes; /* Usable size, excluding cache. */
  int64_t wasted_size_in_bytes;
  
  int32_t num_bands;          /* Total number of available bands. */
  int32_t num_cache_bands;
  int32_t num_data_bands;

  int32_t cache_associativity; /* Number of data bands associated with each
                                * cache band. */
};

static int shingle_get_args(struct dm_target *ti, struct shingle_c *sc,
                            int argc, char **argv)
{
  unsigned long long tmp;
  char dummy;
  
  if (argc != 4) {
    ti->error = "dm-shingle: Invalid argument count.";
    return -1;
  }

  /* TODO: FIX THOSE BOUNDS */
  if (sscanf(argv[1], "%llu%c", &tmp, &dummy) != 1 || tmp & 0xfff ||
      (tmp < /* 512 */ 4 * 1024 || tmp > 2 * 1024 * 1024)) {
    ti->error = "dm-shingle: Invalid track size.";
    return -1;
  }
  sc->track_size_in_bytes = tmp;

  if (sscanf(argv[2], "%llu%c", &tmp, &dummy) != 1 || tmp < 1 /* 20 */ || tmp > 200) {
    ti->error = "dm-shingle: Invalid band size.";
    return -1;
  }
  sc->band_size_in_tracks = tmp;

  if (sscanf(argv[3], "%llu%c", &tmp, &dummy) != 1 || tmp < 1 || tmp > 50 /* 20 */) {
    ti->error = "dm-shingle: Invalid cache percent.";
    return -1;
  }
  sc->cache_percent = tmp;

  if (dm_get_device(ti, argv[0], dm_table_get_mode(ti->table), &sc->dev)) {
    ti->error = "dm-shingle: Device lookup failed.";
    return -1;
  }
  return 0;
}

static void shingle_debug_print(struct shingle_c *sc)
{
  DMERR("Constructing...");
  DMERR("Device: %s", sc->dev->name);
  DMERR("Total disk size: %Lu bytes", sc->total_size_in_bytes);
  DMERR("Band size: %Lu bytes", sc->band_size_in_bytes);
  DMERR("Band size: %d sectors", sc->band_size_in_sectors);
  DMERR("Total number of bands: %d", sc->num_bands);
  DMERR("Number of cache bands: %d", sc->num_cache_bands);
  DMERR("Cache size: %Lu bytes", sc->cache_size_in_bytes);
  DMERR("Number of data bands: %d", sc->num_data_bands);
  DMERR("Usable disk size: %Lu bytes", sc->usable_size_in_bytes);
  DMERR("Number of usable sectors: %d", sc->num_usable_sectors);
  DMERR("Wasted disk size: %Lu bytes", sc->wasted_size_in_bytes);
}

static void reset_cache_band(struct shingle_c *sc, struct cache_band *cb)
{
  BUG_ON(!sc || !cb);
  cb->current_sector = cb->begin_sector;
  bitmap_zero(cb->bitmap, sc->cache_associativity);
}

static int shingle_ctr(struct dm_target *ti, unsigned int argc, char **argv)
{
  struct shingle_c *sc;
  int32_t i, begin_sector, bitmap_size, sector_map_size;
  
  sc = kmalloc(sizeof(*sc), GFP_KERNEL);
  if (!sc) {
    ti->error = "dm-shingle: Cannot allocate shingle context.";
    return -ENOMEM;
  }

  if (shingle_get_args(ti, sc, argc, argv) < 0) {
    kfree(sc);
    return -EINVAL;
  }

  /* sc->total_size_in_bytes = i_size_read(sc->dev->bdev->bd_inode); */
  sc->total_size_in_bytes = TOTAL_SIZE;
  sc->band_size_in_bytes = sc->band_size_in_tracks * sc->track_size_in_bytes;
  sc->band_size_in_sectors = sc->band_size_in_bytes / INTERNAL_SECTOR_SIZE;
  sc->num_bands = sc->total_size_in_bytes / sc->band_size_in_bytes;
  sc->num_cache_bands = sc->num_bands * sc->cache_percent / 100;
  sc->cache_size_in_bytes = sc->num_cache_bands * sc->band_size_in_bytes;

  /* Make |num_data_bands| a multiple of |num_cache_bands| so that all cache
   * bands are equally loaded. */
  sc->num_data_bands = (sc->num_bands / sc->num_cache_bands - 1) *
                       sc->num_cache_bands;
  sc->cache_associativity = sc->num_data_bands / sc->num_cache_bands;
  sc->usable_size_in_bytes = sc->num_data_bands * sc->band_size_in_bytes;
  sc->wasted_size_in_bytes = sc->total_size_in_bytes -
                             sc->cache_size_in_bytes -
                             sc->usable_size_in_bytes;
  sc->num_valid_sectors = (sc->usable_size_in_bytes +
                           sc->cache_size_in_bytes) / INTERNAL_SECTOR_SIZE;
  sc->num_usable_sectors = sc->usable_size_in_bytes / INTERNAL_SECTOR_SIZE;
  BUG_ON(sc->usable_size_in_bytes % INTERNAL_SECTOR_SIZE);

  sector_map_size = (sizeof(sector_t) * sc->num_usable_sectors);
  sc->sector_map = vmalloc(sector_map_size);
  BUG_ON(!sc->sector_map);
  memset(sc->sector_map, -1, sector_map_size);

  sc->cache_bands = vmalloc(sizeof(struct cache_band) * sc->num_cache_bands);
  BUG_ON(!sc->cache_bands);
  
  sc->rmw_buffer = vmalloc(sc->band_size_in_bytes);
  BUG_ON(!sc->rmw_buffer);

  begin_sector = sc->num_usable_sectors;
  bitmap_size = BITS_TO_LONGS(sc->cache_associativity) * sizeof(long);
  for (i = 0; i < sc->num_cache_bands; ++i) {
    sc->cache_bands[i].bitmap = kmalloc(bitmap_size, GFP_KERNEL);
    BUG_ON(!sc->cache_bands[i].bitmap);
    sc->cache_bands[i].begin_sector = begin_sector;
    reset_cache_band(sc, &sc->cache_bands[i]);
    begin_sector += sc->band_size_in_sectors;
  }
  
  shingle_debug_print(sc);
  
  ti->num_flush_bios = 1;
  ti->num_discard_bios = 1;
  ti->num_write_same_bios = 1;
  ti->private = sc;
  return 0;
}

static void shingle_dtr(struct dm_target *ti)
{
  int i;
  struct shingle_c *sc = (struct shingle_c *) ti->private;

  DMERR("Destructing...");
  dm_put_device(ti, sc->dev);
  for (i = 0; i < sc->num_cache_bands; ++i)
    kfree(sc->cache_bands[i].bitmap);
  vfree(sc->sector_map);
  vfree(sc->cache_bands);
  vfree(sc->rmw_buffer);
  kfree(sc);
}

static sector_t map_sector(struct shingle_c *sc, sector_t sector)
{
  sector_t mapped_sector;
  BUG_ON(sector & 0x7);
  sector /= (INTERNAL_SECTOR_SIZE / EXTERNAL_SECTOR_SIZE);
  BUG_ON(sector >= sc->num_usable_sectors);
  mapped_sector = sc->sector_map[sector];
  if (!~mapped_sector)
    mapped_sector = sector;
  BUG_ON(mapped_sector >= sc->num_valid_sectors);
  return mapped_sector;
}

static int shingle_map(struct dm_target *ti, struct bio *bio)
{
  unsigned long flags = bio_rw(bio);
  struct shingle_c *sc = ti->private;
  BUG_ON(bio->bi_size & 0xfff);
  
  if (flags == READ) {
    bio->bi_sector = map_sector(sc, bio->bi_sector);
    bio->bi_bdev = sc->dev->bdev;
    DMERR("READ at %lu of size %u", bio->bi_sector, bio->bi_size);
    return DM_MAPIO_REMAPPED;
  }
  if (flags == READA) {
    return -EIO;
  } else if (flags == WRITE) {
    DMERR("WRITE at %lu of size %u", bio->bi_sector, bio->bi_size);
  }
  bio_endio(bio, 0);
  return DM_MAPIO_SUBMITTED;
}

static void shingle_status(struct dm_target *ti, status_type_t type,
                           unsigned status_flags, char *result, unsigned maxlen)
{
  struct shingle_c *sc = (struct shingle_c *) ti->private;

  switch (type) {
    case STATUSTYPE_INFO:
      result[0] = '\0';
      break;

    case STATUSTYPE_TABLE:
      snprintf(result, maxlen, "%s %d %d %d%%",
               sc->dev->name,
               sc->track_size_in_bytes,
               sc->band_size_in_tracks,
               sc->cache_percent);
      break;
  }
}

static struct target_type shingle_target = {
  .name   = "shingle",
  .version = {1, 0, 0},
  .module = THIS_MODULE,
  .ctr    = shingle_ctr,
  .dtr    = shingle_dtr,
  .map    = shingle_map,
  .status = shingle_status,
};

static int __init dm_shingle_init(void)
{
  int r = dm_register_target(&shingle_target);

  if (r < 0)
    DMERR("register failed %d", r);

  return r;
}

static void __exit dm_shingle_exit(void)
{
  dm_unregister_target(&shingle_target);
}

module_init(dm_shingle_init)
module_exit(dm_shingle_exit)

MODULE_AUTHOR("Abutalib Aghayev <aghayev@ccs.neu.edu>");
MODULE_DESCRIPTION(DM_NAME " shingled disk emulator target");
MODULE_LICENSE("GPL");
