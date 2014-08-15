#include <linux/device-mapper.h>
#include <linux/module.h>
#include <linux/init.h>
#include <linux/bio.h>
#include <linux/completion.h>
#include <linux/string_helpers.h>

#define DM_MSG_PREFIX "sblock"

#define MIN_SBLOCK_SIZE (1 << 20)
#define MAX_SBLOCK_SIZE (50 << 20)

#define MIN_NR_SECTIONS 2
#define MAX_NR_SECTIONS 100

#define MIN_CACHE_PERCENT 1
#define MAX_CACHE_PERCENT 10

#define MIN_OP_SBLOCK_PERCENT 1
#define MAX_OP_SBLOCK_PERCENT 10

#define MIN_DISK_SIZE (76LL << 10)
#define MAX_DISK_SIZE (10LL << 40)

typedef sector_t lba_t;
typedef int32_t pba_t;

struct sblock_ctx {
        struct dm_dev *dev;

        int64_t disk_size;
        int32_t cache_percent;
        int32_t op_sblock_percent;
        int32_t nr_sections;
        int32_t sblock_size;

        int64_t section_usable_size;
        int64_t section_cache_size;
        int32_t section_nr_sblocks;
        int32_t section_nr_op_sblocks;
        int64_t total_usable_size;
};

static char *readable(u64 size)
{
        static char buf[10];

        string_get_size(size, STRING_UNITS_2, buf, sizeof(buf));

        return buf;
}

static bool get_args(struct dm_target *ti, struct sblock_ctx *sc,
                     int argc, char **argv)
{
        unsigned long long tmp;
        char d;

        if (argc != 6) {
                ti->error = "dm-sblock: Invalid argument count.";
                return false;
        }

        if (sscanf(argv[1], "%llu%c", &tmp, &d) != 1 || tmp & 0xfff ||
            (tmp < MIN_SBLOCK_SIZE || tmp > MAX_SBLOCK_SIZE)) {
                ti->error = "dm-sblock: Invalid s-block size.";
                return false;
        }
        sc->sblock_size = tmp;

        if (sscanf(argv[2], "%llu%c", &tmp, &d) != 1 ||
            tmp < MIN_NR_SECTIONS || tmp > MAX_NR_SECTIONS) {
                ti->error = "dm-sblock: Invalid section count.";
                return false;
        }
        sc->nr_sections = tmp;

        if (sscanf(argv[3], "%llu%c", &tmp, &d) != 1 ||
            tmp < MIN_OP_SBLOCK_PERCENT || tmp > MAX_OP_SBLOCK_PERCENT) {
                ti->error = "dm-sblock: Invalid over-provisioned sblock percent.";
                return false;
        }
        sc->op_sblock_percent = tmp;

        if (sscanf(argv[4], "%llu%c", &tmp, &d) != 1 ||
            tmp < MIN_CACHE_PERCENT || tmp > MAX_CACHE_PERCENT) {
                ti->error = "dm-sblock: Invalid cache percent.";
                return false;
        }
        sc->cache_percent = tmp;

        if (sscanf(argv[5], "%llu%c", &tmp, &d) != 1 ||
            tmp < MIN_DISK_SIZE || tmp > MAX_DISK_SIZE) {
                ti->error = "dm-sblock: Invalid disk size.";
                return false;
        }
        sc->disk_size = tmp;

        return true;
}

static void calc_params(struct sblock_ctx *sc)
{
        int64_t s = sc->disk_size / sc->nr_sections;

        sc->section_cache_size = s * sc->cache_percent / 100;
        s -= sc->section_cache_size;
        sc->section_nr_sblocks = s / sc->sblock_size;
        sc->section_nr_op_sblocks = sc->section_nr_sblocks * sc->op_sblock_percent / 100;
        sc->section_nr_sblocks -= sc->section_nr_op_sblocks;

        sc->section_usable_size = sc->section_nr_sblocks * (int64_t) sc->sblock_size;
        sc->total_usable_size = sc->section_usable_size * sc->nr_sections;
}

static void print_params(struct sblock_ctx *sc)
{
        DMINFO("Disk size: %s",           readable(sc->disk_size));
        DMINFO("S-block size: %s",        readable(sc->sblock_size));
        DMINFO("Cache size/section: %s",  readable(sc->section_cache_size));
        DMINFO("S-blocks/section: %d",    sc->section_nr_sblocks);
        DMINFO("OP s-blocks/section: %d", sc->section_nr_op_sblocks);
        DMINFO("Usable section size: %s", readable(sc->section_usable_size));
        DMINFO("Total usable size: %s",   readable(sc->total_usable_size));
}

static void sblock_dtr(struct dm_target *ti)
{
        struct sblock_ctx *sc = (struct sblock_ctx *) ti->private;

        DMINFO("Destructing...");

        ti->private = NULL;

        if (!sc)
                return;
        kzfree(sc);
}

static int sblock_ctr(struct dm_target *ti, unsigned int argc, char **argv)
{
        struct sblock_ctx *sc;
        int32_t ret;

        DMINFO("Constructing...");

        sc = kzalloc(sizeof(*sc), GFP_KERNEL);
        if (!sc) {
                ti->error = "dm-sblock: Cannot allocate sblock context.";
                return -ENOMEM;
        }
        ti->private = sc;

        if (!get_args(ti, sc, argc, argv)) {
                kzfree(sc);
                return -EINVAL;
        }

        calc_params(sc);
        print_params(sc);

        ret = -EINVAL;
        if (dm_get_device(ti, argv[0], dm_table_get_mode(ti->table), &sc->dev)) {
                ti->error = "dm-sblock: Device lookup failed.";
                return -1;
        }

        /* TODO: Reconsider proper values for these. */
        ti->num_flush_bios = 1;
        ti->num_discard_bios = 1;
        ti->num_write_same_bios = 1;

        return 0;
}

static int sblock_map(struct dm_target *ti, struct bio *bio)
{
        return DM_MAPIO_REMAPPED;
}

static void sblock_status(struct dm_target *ti, status_type_t type,
                        unsigned status_flags, char *result, unsigned maxlen)
{
        struct sblock_ctx *sc = (struct sblock_ctx *) ti->private;

        switch (type) {
        case STATUSTYPE_INFO:
                result[0] = '\0';
                break;

        /* TODO: get string representation of device name.*/
        case STATUSTYPE_TABLE:
                snprintf(result, maxlen, "%s cache: %%%d, over-provisioned sblock: %%%d, sections %d, sblock size: %d",
                         sc->dev->name,
                         sc->cache_percent,
                         sc->op_sblock_percent,
                         sc->nr_sections,
                         sc->sblock_size);
                break;
        }
}

static int sblock_iterate_devices(struct dm_target *ti,
                                  iterate_devices_callout_fn fn, void *data)
{
        struct sblock_ctx *sc = ti->private;

        return fn(ti, sc->dev, 0, ti->len, data);
}

static struct target_type sblock_target = {
        .name            = "sblock",
        .version         = {1, 0, 0},
        .module          = THIS_MODULE,
        .ctr             = sblock_ctr,
        .dtr             = sblock_dtr,
        .map             = sblock_map,
        .status          = sblock_status,
        .iterate_devices = sblock_iterate_devices,
};

static int __init sblock_init(void)
{
        int r = dm_register_target(&sblock_target);

        if (r < 0)
                DMERR("register failed %d", r);

        return r;
}

static void __exit sblock_exit(void)
{
        dm_unregister_target(&sblock_target);
}

module_init(sblock_init);
module_exit(sblock_exit);

MODULE_AUTHOR("Abutalib Aghayev <aghayev@ccs.neu.edu>");
MODULE_DESCRIPTION(DM_NAME " S-block STL emulator target");
MODULE_LICENSE("GPL");
