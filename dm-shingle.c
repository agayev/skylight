#include <linux/device-mapper.h>

#include <linux/module.h>
#include <linux/init.h>
#include <linux/bio.h>
#include <linux/bitops.h>

#define DM_MSG_PREFIX "shingle"

#define TOTAL_SIZE (76LL << 10)

#define EXTERNAL_SECTOR_SIZE 512
#define INTERNAL_SECTOR_SIZE 4096
#define SECTOR_RATIO (INTERNAL_SECTOR_SIZE / EXTERNAL_SECTOR_SIZE)

#define EXT2INT(sector) ((int32_t) (sector / SECTOR_RATIO))
#define INT2EXT(sector) (((sector_t) sector) * SECTOR_RATIO)

struct cache_band {
  int32_t begin_sector;    /* Where cache band begins. */
  int32_t current_sector;  /* Where the next write will go. */

  /* There are usually multiple data bands assigned to each band.
   * |begin_data_band| is the index of the first data band assigned to this
   * cache band.  The index of the next data band assigned to this cache band is
   * |begin_data_band + sc->num_cache_bands|.  See below for its use. */
  int32_t begin_data_band;

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
   * |position = (data_band - begin_data_band) / sc->num_cache_bands|
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

  sector_map_size = (sizeof(int32_t) * sc->num_usable_sectors);
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

static int full_cache_band(struct shingle_c *sc, struct cache_band *cb)
{
  return cb->current_sector == cb->begin_sector + sc->band_size_in_sectors;
}

static int write_to_cache_band(struct shingle_c *sc, struct cache_band *cb,
                               struct bio *bio)
{
  int32_t data_band, data_band_bit;
  int32_t internal_sector;

  BUG_ON(full_cache_band(sc, cb));

  /* Set the bit for the data band. */
  internal_sector = EXT2INT(bio->bi_sector);
  data_band = internal_sector / sc->band_size_in_sectors;
  BUG_ON(data_band >= sc->num_data_bands);
  data_band_bit = (data_band - cb->begin_data_band) / sc->num_cache_bands;
  set_bit(data_band_bit, cb->bitmap);
  
  /* Map the sector to a sector in the cache band and start the write. */
  sc->sector_map[internal_sector] = cb->current_sector++;
  bio->bi_sector = INT2EXT(sc->sector_map[internal_sector]);
  bio->bi_bdev = sc->dev->bdev;
  DMERR("W %d", EXT2INT(bio->bi_sector));
  return DM_MAPIO_REMAPPED;
}

/* TODO: Make these reads and writes actually happen. */
static void read_modify_write_data_band(struct shingle_c *sc, int32_t data_band)
{
  int32_t begin_sector = data_band * sc->band_size_in_sectors;
  int32_t end_sector = begin_sector + sc->band_size_in_sectors;

  int32_t next_sector = begin_sector;
  for ( ; next_sector < end_sector; ++next_sector)
    DMERR("R %d", next_sector);

  for (next_sector = begin_sector; next_sector < end_sector; ++next_sector) {
    if (~sc->sector_map[next_sector]) {
      DMERR("R %d", sc->sector_map[next_sector]);
      sc->sector_map[next_sector] = -1;
    }
  }

  for (next_sector = begin_sector; next_sector < end_sector; ++next_sector)
    DMERR("W %d", next_sector);
}

static void garbage_collect(struct shingle_c *sc, struct cache_band *cb)
{
  int bit;
  for_each_set_bit(bit, cb->bitmap, sc->cache_associativity) {
    int32_t data_band = bit * sc->num_cache_bands + cb->begin_data_band;
    DMERR("GC: data band %d has data in the cache buffer.", data_band);
    read_modify_write_data_band(sc, data_band);
  }
  reset_cache_band(sc, cb);
}

static int write_bio(struct shingle_c *sc, struct bio *bio)
{
  int32_t cbi = (EXT2INT(bio->bi_sector) / sc->band_size_in_sectors) %
                sc->num_cache_bands;
  struct cache_band *cb = &sc->cache_bands[cbi];
  if (full_cache_band(sc, cb))
    garbage_collect(sc, cb);
  write_to_cache_band(sc, cb, bio);
  return DM_MAPIO_REMAPPED;
}
                          
static int bad_bio(struct shingle_c *sc, struct bio *bio)
{
  return (bio->bi_sector & 0x7) || (bio->bi_size & 0xfff) ||
      (EXT2INT(bio->bi_sector) >= sc->num_usable_sectors);
}

static int32_t map_sector(struct shingle_c *sc, int32_t sector)
{
  int32_t mapped_sector;
  mapped_sector = sc->sector_map[sector];
  if (!~mapped_sector)
    mapped_sector = sector;
  BUG_ON(mapped_sector >= sc->num_valid_sectors);
  return mapped_sector;
}

static int read_bio(struct shingle_c *sc, struct bio *bio)
{
  bio->bi_sector = INT2EXT(map_sector(sc, EXT2INT(bio->bi_sector)));
  bio->bi_bdev = sc->dev->bdev;
  DMERR("R %d", EXT2INT(bio->bi_sector));
  return DM_MAPIO_REMAPPED;
}

static int shingle_map(struct dm_target *ti, struct bio *bio)
{
  unsigned long flags = bio_rw(bio);
  struct shingle_c *sc = ti->private;

  BUG_ON(bad_bio(sc, bio));
  if (flags == READ)
    return read_bio(sc, bio);
  if (flags == READA)
    return -EIO;
  if (flags == WRITE)
    return write_bio(sc, bio);
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
