#include <linux/bio.h>
#include <linux/bitops.h>
#include <linux/device-mapper.h>
#include <linux/init.h>
#include <linux/mempool.h>
#include <linux/module.h>

#define DM_MSG_PREFIX "shingle"

/* 
 * Sizes of all names ending with _size or _SIZE is in bytes.
 */

#define TOTAL_SIZE (76LL << 10)

#define LBA_SIZE 512
#define PBA_SIZE 4096
#define SIZE_RATIO (PBA_SIZE / LBA_SIZE)

#define LBA_TO_PBA(lba) ((int32_t) (lba / SIZE_RATIO))
#define PBA_TO_LBA(pba) (((sector_t) pba) * SIZE_RATIO)

#define NUM_LBAS_PER_PAGE (PAGE_SIZE / LBA_SIZE)

struct shingle_c {
        struct dm_dev *dev;
        int64_t total_size;

        /* Percentage of space used for cache region. */
        int32_t cache_percent;
        
        /* Size of the cache region made up of multiple cache bands. */
        int64_t cache_size;
        int32_t num_cache_bands;
        
        int64_t usable_size;  /* Usable size, excluding the cache region. */
        int64_t wasted_size;  /* Wasted due to alignment. */
        
        int32_t track_size;
        int64_t band_size;
        int32_t band_size_tracks;
        int32_t band_size_pbas;

        /* Number of valid pbas.  Includes the cache region. */
        int32_t num_valid_pbas;

        /* Number of usable pbas.  Excludes the cache region. */
        int32_t num_usable_pbas;

        /* Maps a pba to its location in the cache region.  Contains -1 for
         * unmapped pbas. */
        int32_t *pba_map;
        
        struct cache_band *cache_bands;

        int32_t num_bands;    /* Total number of available bands. */

        /* Number of data bands.  These make up the usable region of the
         * disk. */
        int32_t num_data_bands;

        /* Number of data bands associated with each cache band. */
        int32_t cache_assoc;

        mempool_t *io_pool;   /* For per bio private data. */
        struct bio_set *bs;   /* For cloned bios. */

        /* All read and write operations will be put into this queue and will
         * be performed by worker threads. */
        struct workqueue_struct *queue;
};

struct cache_band {
        int32_t begin_pba;   /* Where the cache band begins. */
        int32_t current_pba; /* Where the next write will go. */

        /*
         * There are usually multiple data bands assigned to each band.
         * |begin_data_band| is the the first data band assigned to this cache
         * band.  The next is |begin_data_band + sc->num_cache_bands|.  See
         * below for its use.
         */
        int32_t begin_data_band;

        /*
         * Although multiple data bands are assigned to a cache band, not all of
         * them may have pbas on it.  |data_band_bitmap| keeps track of data
         * bands having pbas on this cache band.  This is used during garbage
         * collection.
         *
         * Assume there are 6 data bands and 2 cache bands.  Then data bands 0,
         * 2, 4 are assigned to cache band 0 and data bands 1, 3, 5 are assigned
         * to cache band 1 (using modulo arithmetic).  Consider cache band 1.
         * If it contains pbas only for data band 5, then |data_band_bitmap|
         * will be "001", if it contains pbas for data bands 1 and 3, then
         * |data_band_bitmap| will be "110".  That is, a given data band, its
         * position in the the bitmap is calculated as below, and the bit in
         * that position is set:
         *
         * |position = (data_band - begin_data_band) / sc->num_cache_bands|
         */
        unsigned long *data_band_bitmap;
};

/*
 * This represents a single I/O operation, either read or write.  The |bio|
 * associated with it may result in multiple bios being generated, all of which
 * should completed before this one can be considered completed.  Every
 * generated bio increments |pending| and every completed bio decrements
 * pending.  When |pending| reaches zero, we deallocate |io| and signal the
 * completion of |bio| by calling |bio_endio| on it.
 */
struct io {
        struct shingle_c *sc;
        struct bio *bio;
        struct work_struct work;
        int error;
        atomic_t pending;
};

#define MIN_IOS 16
#define MIN_POOL_PAGES 32

static struct kmem_cache *_io_pool;

static struct io *alloc_io(struct shingle_c *sc, struct bio *bio)
{
        struct io *io = mempool_alloc(sc->io_pool, GFP_NOIO);
        
        io->sc = sc;
        io->bio = bio;
        io->error = 0;
        atomic_set(&io->pending, 0);

        return io;
}

static void release_io(struct io *io)
{
        struct shingle_c *sc = io->sc;
        struct bio *bio = io->bio;
        int error = io->error;

        BUG_ON(atomic_read(&io->pending));
        mempool_free(io, sc->io_pool);
        bio_endio(bio, error);
}

static bool get_args(struct dm_target *ti, struct shingle_c *sc,
                     int argc, char **argv)
{
        unsigned long long tmp;
        char dummy;
  
        if (argc != 4) {
                ti->error = "dm-shingle: Invalid argument count.";
                return false;
        }

        /* TODO: FIX THOSE BOUNDS */
        if (sscanf(argv[1], "%llu%c", &tmp, &dummy) != 1 || tmp & 0xfff ||
            (tmp < /* 512 */ 4 * 1024 || tmp > 2 * 1024 * 1024)) {
                ti->error = "dm-shingle: Invalid track size.";
                return false;
        }
        sc->track_size = tmp;

        if (sscanf(argv[2], "%llu%c", &tmp, &dummy) != 1 || tmp < 1 /* 20 */ || tmp > 200) {
                ti->error = "dm-shingle: Invalid band size.";
                return false;
        }
        sc->band_size_tracks = tmp;

        if (sscanf(argv[3], "%llu%c", &tmp, &dummy) != 1 || tmp < 1 || tmp > 50 /* 20 */) {
                ti->error = "dm-shingle: Invalid cache percent.";
                return false;
        }
        sc->cache_percent = tmp;
        return true;
}

static void debug_print(struct shingle_c *sc)
{
        DMERR("Constructing...");
        DMERR("Device: %s", sc->dev->name);
        DMERR("Total disk size: %Lu bytes", sc->total_size);
        DMERR("Band size: %Lu bytes", sc->band_size);
        DMERR("Band size: %d pbas", sc->band_size_pbas);
        DMERR("Total number of bands: %d", sc->num_bands);
        DMERR("Number of cache bands: %d", sc->num_cache_bands);
        DMERR("Cache size: %Lu bytes", sc->cache_size);
        DMERR("Number of data bands: %d", sc->num_data_bands);
        DMERR("Usable disk size: %Lu bytes", sc->usable_size);
        DMERR("Number of usable pbas: %d", sc->num_usable_pbas);
        DMERR("Wasted disk size: %Lu bytes", sc->wasted_size);
}

static void reset_cache_band(struct shingle_c *sc, struct cache_band *cb)
{
        BUG_ON(!sc || !cb);
        cb->current_pba = cb->begin_pba;
        bitmap_zero(cb->data_band_bitmap, sc->cache_assoc);
}

static void calc_params(struct shingle_c *sc)
{
        sc->total_size = TOTAL_SIZE; /* i_size_read(sc->dev->bdev->bd_inode); */
        sc->band_size = sc->band_size_tracks * sc->track_size;
        sc->band_size_pbas = sc->band_size / PBA_SIZE;
        sc->num_bands = sc->total_size / sc->band_size;
        sc->num_cache_bands = sc->num_bands * sc->cache_percent / 100;
        sc->cache_size = sc->num_cache_bands * sc->band_size;

        /* Make |num_data_bands| a multiple of |num_cache_bands| so that all
         * cache bands are equally loaded. */
        sc->num_data_bands = (sc->num_bands / sc->num_cache_bands - 1) *
                sc->num_cache_bands;
  
        sc->cache_assoc = sc->num_data_bands / sc->num_cache_bands;
        sc->usable_size = sc->num_data_bands * sc->band_size;
        sc->wasted_size = sc->total_size - sc->cache_size - sc->usable_size;
        sc->num_valid_pbas = (sc->usable_size + sc->cache_size) / PBA_SIZE;
        sc->num_usable_pbas = sc->usable_size / PBA_SIZE;
        BUG_ON(sc->usable_size % PBA_SIZE);
}

static bool alloc_structs(struct shingle_c *sc)
{
        int32_t i, size, pba;

        size = sizeof(int32_t) * sc->num_usable_pbas;
        sc->pba_map = vmalloc(size);
        if (!sc->pba_map)
                return false;
        memset(sc->pba_map, -1, size);

        size = sizeof(struct cache_band) * sc->num_cache_bands;
        sc->cache_bands = vmalloc(size);
        if (!sc->cache_bands)
                return false;

        /* The first pba of the cache region is |sc->num_usable_pbas|. */
        pba = sc->num_usable_pbas;
        size = BITS_TO_LONGS(sc->cache_assoc) * sizeof(long);
        for (i = 0; i < sc->num_cache_bands; ++i) {
                unsigned long *bitmap = kmalloc(size, GFP_KERNEL);
                if (!bitmap)
                        return false;
                sc->cache_bands[i].data_band_bitmap = bitmap;
                
                sc->cache_bands[i].begin_pba = pba;
                reset_cache_band(sc, &sc->cache_bands[i]);
                pba += sc->band_size_pbas;
        }
        return true;
}

static void shingle_dtr(struct dm_target *ti);

static int shingle_ctr(struct dm_target *ti, unsigned int argc, char **argv)
{
        struct shingle_c *sc;
        int32_t ret;

        DMERR("Constructing...");
        
        sc = kmalloc(sizeof(*sc), GFP_KERNEL);
        if (!sc) {
                ti->error = "dm-shingle: Cannot allocate shingle context.";
                return -ENOMEM;
        }
        ti->private = sc;

        if (!get_args(ti, sc, argc, argv)) {
                kfree(sc);
                return -EINVAL;
        }

        calc_params(sc);
        
        ret = -ENOMEM;
        if (!alloc_structs(sc)) {
                ti->error = "Cannot allocate data structures.";
                goto bad;
        }

        sc->io_pool = mempool_create_slab_pool(MIN_IOS, _io_pool);
        if (!sc->io_pool) {
                ti->error = "Cannot allocate mempool.";
                goto bad;
        }
        
        sc->bs = bioset_create(MIN_IOS, 0);
        if (!sc->bs) {
                ti->error = "Cannot allocate bioset.";
                goto bad;
        }

        sc->queue = alloc_workqueue("shingled",
                                    WQ_NON_REENTRANT | WQ_MEM_RECLAIM,
                                    1);
        if (!sc->queue) {
                ti->error = "Cannot allocate work queue.";
                goto bad;
        }

        ret = -EINVAL;
        if (dm_get_device(ti, argv[0], dm_table_get_mode(ti->table), &sc->dev)) {
                ti->error = "dm-shingle: Device lookup failed.";
                return -1;
        }
        
        debug_print(sc);
  
        ti->num_flush_bios = 1;
        ti->num_discard_bios = 1;
        ti->num_write_same_bios = 1;
        return 0;

bad:
        shingle_dtr(ti);
        return ret;
}

static void shingle_dtr(struct dm_target *ti)
{
        int i;
        struct shingle_c *sc = (struct shingle_c *) ti->private;

        DMERR("Destructing...");
        
        ti->private = NULL;
        if (!sc)
                return;

        if (sc->queue)
                destroy_workqueue(sc->queue);
        if (sc->io_pool)
                mempool_destroy(sc->io_pool);
        if (sc->dev)
                dm_put_device(ti, sc->dev);
        
        for (i = 0; i < sc->num_cache_bands; ++i)
                if (sc->cache_bands[i].data_band_bitmap)
                        kfree(sc->cache_bands[i].data_band_bitmap);

        if (sc->pba_map)
                vfree(sc->pba_map);
        if (sc->cache_bands)
                vfree(sc->cache_bands);
        kfree(sc);
}

static int full_cache_band(struct shingle_c *sc, struct cache_band *cb)
{
        return cb->current_pba == cb->begin_pba + sc->band_size_pbas;
}

static void gc_cache_band(struct shingle_c *sc, struct cache_band *cb)
{
        BUG();
        /* int i; */
        /* for_each_set_bit(i, cb->data_band_bitmap, sc->cache_assoc) { */
        /*         int32_t db = i * sc->num_cache_bands + cb->begin_data_band; */
        /*         DMERR("GC: data band %d has data in the cache buffer.", db); */
        /*         read_modify_write_data_band(sc, db); */
        /* } */
        /* reset_cache_band(sc, cb); */
}

/*
 * For now we only handle READ and WRITE requests that are 4KB and aligned.
 */
static int bad_bio(struct shingle_c *sc, struct bio *bio)
{
        DMERR("%lx", bio->bi_rw);
        return (bio->bi_sector & 0x7) || (bio->bi_size & 0xfff) ||
                        (LBA_TO_PBA(bio->bi_sector) >= sc->num_usable_pbas);
}

/*
 * Translates |lba| to its physical location on disk.  Returns |lba| if it was
 * not mapped.
 */
static sector_t lookup_lba(struct shingle_c *sc, sector_t lba)
{
        int32_t pba = LBA_TO_PBA(lba);
        int32_t mapped_pba = sc->pba_map[pba];
        
        BUG_ON(mapped_pba >= sc->num_valid_pbas);
        
        if (!~mapped_pba)
                mapped_pba = pba;

        return PBA_TO_LBA(mapped_pba);
}

/*
 * Maps |lba| to an lba in the cache band.  Also sets the bit for the data band
 * to which |lba| belongs.
 */
static sector_t map_lba(struct shingle_c *sc, struct cache_band *cb,
                        sector_t lba)
{
        int32_t pba = LBA_TO_PBA(lba);
        int32_t data_band = pba / sc->band_size_pbas;
        int32_t pos = (data_band - cb->begin_data_band) / sc->num_cache_bands;
        int32_t mapped_pba = cb->current_pba++;

        BUG_ON(mapped_pba >= sc->num_valid_pbas);
        BUG_ON(full_cache_band(sc, cb));
        BUG_ON(data_band >= sc->num_data_bands);

        set_bit(pos, cb->data_band_bitmap);
        sc->pba_map[pba] = mapped_pba;

        return PBA_TO_LBA(mapped_pba);
}

static void endio(struct bio *clone, int error)
{
        struct io *io = clone->bi_private;
        
        if (unlikely(!bio_flagged(clone, BIO_UPTODATE) && !error))
                io->error = -EIO;

        DMERR("%s %c %lu",
              (io->error ? "!!" : "OK"),
              (bio_data_dir(clone) == READ ? 'R' : 'W'),
              clone->bi_sector);

        bio_put(clone);
        if (atomic_dec_and_test(&io->pending))
                release_io(io);
}

static void clone_init(struct io *io, struct bio *clone)
{
        struct shingle_c *sc = io->sc;

        atomic_inc(&io->pending);
        clone->bi_private = io;
        clone->bi_end_io = endio;
        clone->bi_bdev = sc->dev->bdev;
}

static struct cache_band *get_cache_band(struct shingle_c *sc, struct bio *bio)
{
        int32_t pba = LBA_TO_PBA(bio->bi_sector);
        int32_t cbi = (pba / sc->band_size_pbas) %
                      sc->num_cache_bands;
        return &sc->cache_bands[cbi];
}

static bool single_segment(struct bio *bio)
{
        return bio->bi_vcnt == 1 && bio->bi_size == PAGE_SIZE ;
}

static void read_io(struct io *io)
{
        struct shingle_c *sc = io->sc;
        struct bio *bio = io->bio;
        struct bio *clone;
        /* struct bio_vec *bv; */
        /* sector_t last_sector, last_xlated_sector; */
        /* int i; */

        BUG_ON(single_segment(bio));

        clone = bio_clone_bioset(bio, GFP_NOIO, sc->bs);
        if (!clone) {
                io->error = -ENOMEM;
                release_io(io);
                return;
        }
        clone_init(io, clone);

        /* last_sector = clone->bi_sector; */
        /* last_xlated_sector = lookup_sector(last_sector); */
        /* ++clone->bi_idx; */
        
        /* bio_for_each_segment(bv, clone, i) { */
        /*         sector_t xlated_sector = lookup_sector(sc, last_sector); */
        /*         if () { */
        /*         } */
        /*         last_sector +=  */

        clone->bi_sector = lookup_lba(sc, clone->bi_sector);
        clone->bi_bdev = sc->dev->bdev;
        
        generic_make_request(clone);
}

static void write_io(struct io *io)
{
        struct shingle_c *sc = io->sc;
        struct bio *bio = io->bio;
        struct bio *clone = bio_clone_bioset(bio, GFP_NOIO, sc->bs);
        struct cache_band *cb = get_cache_band(sc, bio);

        if (!clone) {
                io->error = -ENOMEM;
                release_io(io);
                return;
        }
        
        if (full_cache_band(sc, cb))
                gc_cache_band(sc, cb);

        clone_init(io, clone);
        clone->bi_sector = map_lba(sc, cb, clone->bi_sector);
        clone->bi_bdev = sc->dev->bdev;

        generic_make_request(clone);
}

/*
 * This is the entry point of the worker threads.
 */
static void shingled(struct work_struct *work)
{
        struct io *io = container_of(work, struct io, work);

        if (bio_data_dir(io->bio) == READ)
                read_io(io);
        else
                write_io(io);
}

static void queue_io(struct io *io)
{
        struct shingle_c *sc = io->sc;
        
        INIT_WORK(&io->work, shingled);
        queue_work(sc->queue, &io->work);
        
}

static int shingle_map(struct dm_target *ti, struct bio *bio)
{
        struct shingle_c *sc = ti->private;
        struct cache_band *cb;
         
        BUG_ON(bad_bio(sc, bio));

        /*
         * TODO: It is possible that even a multi-segment bio can proceed just
         * after a simple remap.  Handle that as well, in which case
         * single-segment I/O will become a special case of that.
         */
        if (single_segment(bio)) {
                if (bio_data_dir(bio) == READ) {
                        bio->bi_sector = lookup_lba(sc, bio->bi_sector);
                        bio->bi_bdev = sc->dev->bdev;
                        DMERR("0 R %lu", bio->bi_sector);
                        return DM_MAPIO_REMAPPED;
                }
                
                cb = get_cache_band(sc, bio);
                if (!full_cache_band(sc, cb)) {
                        bio->bi_sector = map_lba(sc, cb, bio->bi_sector);
                        bio->bi_bdev = sc->dev->bdev;
                        DMERR("0 W %lu", bio->bi_sector);
                        return DM_MAPIO_REMAPPED;
                }
        }

        queue_io(alloc_io(sc, bio));
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
                         sc->track_size,
                         sc->band_size_tracks,
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
        int r;

        _io_pool = KMEM_CACHE(io, 0);
        if (!_io_pool)
                return -ENOMEM;
        
        r = dm_register_target(&shingle_target);        
        if (r < 0) {
                DMERR("register failed %d", r);
                kmem_cache_destroy(_io_pool);
        }

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
