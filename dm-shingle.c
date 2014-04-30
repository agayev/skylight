#include <linux/device-mapper.h>

#include <linux/module.h>
#include <linux/init.h>
#include <linux/bio.h>

#define DM_MSG_PREFIX "shingle"

struct shingle_c {
  struct dm_dev *dev;
  int track_size;
  int band_size;
  int cache_percent;
};
  
static int shingle_ctr(struct dm_target *ti, unsigned int argc, char **argv)
{
  struct shingle_c *sc;
  unsigned long long tmp;
  char dummy;
  
  if (argc != 4) {
    ti->error = "dm-shingle: Invalid argument count.";
    return -EINVAL;
  }

  sc = kmalloc(sizeof(*sc), GFP_KERNEL);
  if (sc == NULL) {
    ti->error = "dm-shingle: Cannot allocate shingle context.";
    return -ENOMEM;
  }
  
  if (sscanf(argv[1], "%llu%c", &tmp, &dummy) != 1 || tmp & 0xfff ||
      (tmp < 512 * 1024 || tmp > 2 * 1024 * 1024)) {
    ti->error = "dm-shingle: Invalid track size.";
    return -EINVAL;
  }
  sc->track_size = tmp;

  if (sscanf(argv[2], "%llu%c", &tmp, &dummy) != 1 || tmp < 20 || tmp > 200) {
    ti->error = "dm-shingle: Invalid band size.";
    return -EINVAL;
  }
  sc->band_size = tmp;

  if (sscanf(argv[3], "%llu%c", &tmp, &dummy) != 1 || tmp < 1 || tmp > 20) {
    ti->error = "dm-shingle: Invalid cache percent.";
    return -EINVAL;
  }
  sc->cache_percent = tmp;

  if (dm_get_device(ti, argv[0], dm_table_get_mode(ti->table), &sc->dev)) {
    ti->error = "dm-shingle: Device lookup failed.";
    goto bad;
  }

  printk(KERN_CRIT "dm-shingle: device: %s\n", argv[0]);
  printk(KERN_CRIT "dm-shingle: track size: %d\n", sc->track_size);
  printk(KERN_CRIT "dm-shingle: band size: %d\n", sc->band_size);
  printk(KERN_CRIT "dm-shingle: cache percent: %d\n", sc->cache_percent);
  
  ti->num_flush_bios = 1;
  ti->num_discard_bios = 1;
  ti->num_write_same_bios = 1;
  ti->private = sc;
  return 0;

bad:
  kfree(sc);
  return -EINVAL;
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
  .version = {1, 1, 0},
  .module = THIS_MODULE,
  .ctr    = shingle_ctr,
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
