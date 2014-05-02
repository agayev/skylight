#include <linux/device-mapper.h>

#include <linux/module.h>
#include <linux/init.h>
#include <linux/bio.h>

#define DM_MSG_PREFIX "shingle"

#define SECTOR_SIZE 4096

struct shingle_c {
  struct dm_dev *dev;
  int32_t track_size_in_bytes;
  int32_t band_size_in_tracks;
  int32_t cache_percent;
  int32_t num_sectors;
  int32_t *sector_map;
  
  /* For debugging. */
  int64_t total_size_in_bytes;
  int64_t cache_size_in_bytes;
  int64_t band_size_in_bytes;
  int64_t data_size_in_bytes;
  int64_t wasted_size_in_bytes;
  
  int32_t num_bands;
  int32_t num_cache_bands;
  int32_t num_data_bands;
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
  
  if (sscanf(argv[1], "%llu%c", &tmp, &dummy) != 1 || tmp & 0xfff ||
      (tmp < 512 * 1024 || tmp > 2 * 1024 * 1024)) {
    ti->error = "dm-shingle: Invalid track size.";
    return -1;
  }
  sc->track_size_in_bytes = tmp;

  if (sscanf(argv[2], "%llu%c", &tmp, &dummy) != 1 || tmp < 20 || tmp > 200) {
    ti->error = "dm-shingle: Invalid band size.";
    return -1;
  }
  sc->band_size_in_tracks = tmp;

  if (sscanf(argv[3], "%llu%c", &tmp, &dummy) != 1 || tmp < 1 || tmp > 20) {
    ti->error = "dm-shingle: Invalid cache percent.";
    return -1;
  }
  sc->cache_percent = tmp;

  if (dm_get_device(ti, argv[0], dm_table_get_mode(ti->table), &sc->dev)) {
    ti->error = "dm-shingle: Device lookup failed.";
    return -1;
  }

  /* sc->total_size_in_bytes = i_size_read(sc->dev->bdev->bd_inode); */
  sc->total_size_in_bytes = 350LL << 30;
  sc->band_size_in_bytes = sc->band_size_in_tracks * sc->track_size_in_bytes;
  sc->num_bands = sc->total_size_in_bytes / sc->band_size_in_bytes;
  sc->num_cache_bands = sc->num_bands * sc->cache_percent / 100;
  sc->cache_size_in_bytes = sc->num_cache_bands * sc->band_size_in_bytes;

  /* Make |num_data_bands| a multiple of |num_cache_bands| so that all cache
   * bands are equally loaded. */
  sc->num_data_bands = (sc->num_bands / sc->num_cache_bands - 1) *
                       sc->num_cache_bands;
  
  sc->data_size_in_bytes = sc->num_data_bands * sc->band_size_in_bytes;
  sc->wasted_size_in_bytes = sc->total_size_in_bytes -
                             sc->cache_size_in_bytes -
                             sc->data_size_in_bytes;
  sc->num_sectors = sc->data_size_in_bytes / SECTOR_SIZE;
  
  if (sc->data_size_in_bytes % SECTOR_SIZE) {
    ti->error = "dm-shingle: Something wrong with the alignment.";
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
  DMERR("Total number of bands: %d", sc->num_bands);
  DMERR("Number of cache bands: %d", sc->num_cache_bands);
  DMERR("Cache size: %Lu bytes", sc->cache_size_in_bytes);
  DMERR("Number of data bands: %d", sc->num_data_bands);
  DMERR("Usable disk size: %Lu bytes", sc->data_size_in_bytes);
  DMERR("Wasted disk size: %Lu bytes", sc->wasted_size_in_bytes);
}

static int shingle_ctr(struct dm_target *ti, unsigned int argc, char **argv)
{
  struct shingle_c *sc;
  
  sc = kmalloc(sizeof(*sc), GFP_KERNEL);
  if (!sc) {
    ti->error = "dm-shingle: Cannot allocate shingle context.";
    return -ENOMEM;
  }

  if (shingle_get_args(ti, sc, argc, argv) < 0) {
    kfree(sc);
    return -EINVAL;
  }

  sc->sector_map = vmalloc(sizeof(int32_t) * sc->num_sectors);
  if (!sc->sector_map) {
    ti->error = "dm-shingle: Cannot allocate sector map.";
    return -ENOMEM;
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
  struct shingle_c *sc = (struct shingle_c *) ti->private;

  DMERR("Destructing...");
  dm_put_device(ti, sc->dev);
  vfree(sc->sector_map);
  kfree(sc);
}

static int shingle_map(struct dm_target *ti, struct bio *bio)
{
  switch(bio_rw(bio)) {
    case READ:
      zero_fill_bio(bio);
      break;
    case READA:
      /* readahead of null bytes only wastes buffer cache */
      return -EIO;
    case WRITE:
      /* writes get silently dropped */
      break;
  }

  bio_endio(bio, 0);

  /* accepted bio, don't make new request */
  return DM_MAPIO_SUBMITTED;
}

static struct target_type shingle_target = {
  .name   = "shingle",
  .version = {1, 0, 0},
  .module = THIS_MODULE,
  .ctr    = shingle_ctr,
  .dtr    = shingle_dtr,
  .map    = shingle_map,
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
