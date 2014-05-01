#include <linux/device-mapper.h>

#include <linux/module.h>
#include <linux/init.h>
#include <linux/bio.h>

#define DM_MSG_PREFIX "shingle"

struct shingle_c {
  struct dm_dev *dev;
  int32_t track_size;
  int32_t band_size;
  int32_t cache_percent;

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
  sc->track_size = tmp;

  if (sscanf(argv[2], "%llu%c", &tmp, &dummy) != 1 || tmp < 20 || tmp > 200) {
    ti->error = "dm-shingle: Invalid band size.";
    return -1;
  }
  sc->band_size = tmp;

  if (sscanf(argv[3], "%llu%c", &tmp, &dummy) != 1 || tmp < 1 || tmp > 20) {
    ti->error = "dm-shingle: Invalid cache percent.";
    return -1;
  }
  sc->cache_percent = tmp;

  if (dm_get_device(ti, argv[0], dm_table_get_mode(ti->table), &sc->dev)) {
    ti->error = "dm-shingle: Device lookup failed.";
    return -1;
  }

  sc->total_size_in_bytes = i_size_read(sc->dev->bdev->bd_inode);
  
  return 0;
}

static void shingle_debug_print(struct shingle_c *sc)
{
  DMERR("Constructing...");
  DMERR("device: %s", sc->dev->name);
  DMERR("track size: %d", sc->track_size);
  DMERR("band size: %d", sc->band_size);
  DMERR("cache percent: %d", sc->cache_percent);
  DMERR("Total disk size: %Lu", sc->total_size_in_bytes);
}

static int shingle_ctr(struct dm_target *ti, unsigned int argc, char **argv)
{
  struct shingle_c *sc;
  
  sc = kmalloc(sizeof(*sc), GFP_KERNEL);
  if (sc == NULL) {
    ti->error = "dm-shingle: Cannot allocate shingle context.";
    return -ENOMEM;
  }

  if (shingle_get_args(ti, sc, argc, argv) < 0) {
    kfree(sc);
    return -EINVAL;
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
