#include <linux/device-mapper.h>

#include <linux/module.h>
#include <linux/init.h>
#include <linux/bio.h>

/*
 * SADC (set-associative disk cache) STL.
 */

/*
 * From Seagate:
 *
 * Consider the following scale of sizes:
 *  - tracks in the range of 0.5 to 2 MiB
 *  - bands in the range of 20 to 200 tracks
 *  - drives in the range of 1 to 10 TB
 *  - a 1% cache space could be a single, very large band or many smaller bands
 *  - the 99% data space as tens of thousands of bands
 */

/*
 * Important TODOs.
 *
 * TODO: Consider using bio_split, bio_trim etc.
 * TODO: Test error handling.
 * TODO: Reconsider lba/pba boundaries.
 * TODO: Revisit cache_band, band, etc.
 * TODO: Post and pre-conditions for all functions.
 * TODO: Consider every functions signature.
 * TODO: Consider names.
 * TODO: Remove WARN_ONs.
 * TODO: Remove DBGs.
 * TODO: Fix comments to be consistent.
 */

#define DM_MSG_PREFIX "sadc"

/* Disk state reset IOCTL command. */
#define RESET_DISK 0xDEADBEEF

/*
 * This code assumes that maximum segment size in a bio is equal to PAGE_SIZE,
 * which is equal to 4096 bytes.  Things may break if this is not the case.
 */
#define MIN_DISK_SIZE (76LL << 10)
#define MAX_DISK_SIZE (10LL << 40)

#define LBA_SIZE 512
#define PBA_SIZE 4096
#define LBAS_IN_PBA (PBA_SIZE / LBA_SIZE)

/*
 * TODO: We can reduce memory usage by reducing this number, explore how
 * realistic 32 is, since in practice the number seems to be much smaller.
 */
#define MAX_SEGMENTS_IN_BIO 32

#define lba_to_pba(lba) ((int32_t) (lba / LBAS_IN_PBA))
#define pba_to_lba(pba) (((sector_t) pba) * LBAS_IN_PBA)

#define DBG(bio, error)                                  \
        DMERR("%s %c %d [%lu] %u",                       \
              (error ? "!!" : "OK"),                     \
              (bio_data_dir(bio) == READ ? 'R' : 'W'),   \
              lba_to_pba(bio->bi_sector),                \
              bio->bi_sector,                            \
              bio->bi_size / LBA_SIZE)

enum state {
        STATE_START_GC,
        STATE_START_CACHE_BAND_GC,
        STATE_READ_DATA_BAND,
        STATE_MODIFY_DATA_BAND,
        STATE_WRITE_DATA_BAND,
        STATE_RMW_COMPLETE,
        STATE_GC_COMPLETE,
        STATE_NONE,
};

struct sadc_c {
        struct dm_dev *dev;
        int64_t disk_size;

        /*
         * Control access to context.  Currently, this is only used when we want
         * to reset the disk while GC is happening.
         */
        struct mutex c_lock;

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
        mempool_t *page_pool; /* For GC bio pages. */
        struct bio_set *bs;   /* For cloned bios. */

        /* All read and write operations will be put into this queue and will
         * be performed by worker threads. */
        struct workqueue_struct *queue;

        /* GC related stuff follow. */
        enum state state;

        /* io that is currently pending due to GC. */
        struct io *pending_io;

        /* Cache band currently undergoing GC. */
        struct cache_band *gc_cache_band;

        /* Data band currently undergoing RMW. */
        int32_t rmw_data_band;

        /*
         * This are the bios that act as an RMW buffer.  We read the data band
         * to these, and then execute modify by cloning those of these that need
         * to be modified and reading into them and finally, write them back.
         */
        struct bio *bios[MAX_SEGMENTS_IN_BIO];
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
         * them may have pbas on it.  |map| keeps track of data
         * bands having pbas on this cache band.  This is used during garbage
         * collection.
         *
         * Assume there are 6 data bands and 2 cache bands.  Then data bands 0,
         * 2, 4 are assigned to cache band 0 and data bands 1, 3, 5 are assigned
         * to cache band 1 (using modulo arithmetic).  Consider cache band 1.
         * If it contains pbas only for data band 5, then |map|
         * will be "001", if it contains pbas for data bands 1 and 3, then
         * |map| will be "110".  That is, a given data band, its
         * position in the the bitmap is calculated as below, and the bit in
         * that position is set:
         *
         * |position = (data_band - begin_data_band) / sc->num_cache_bands|
         */
        unsigned long *map;
};

/*
 * This represents a single I/O operation, either read or write.  The |bio|
 * associated with it may result in multiple bios being generated (see |chunk|
 * above), all of which should complete before this one can be considered
 * complete.  Every generated bio increments |pending| and every completed bio
 * decrements it.  When |pending| reaches zero, we deallocate |io| and signal
 * the completion of |bio| by calling |bio_endio| on it.
 */
struct io {
        struct sadc_c *sc;
        struct bio *bio;
        struct work_struct work;
        int error;
        atomic_t pending;
};

#define MIN_IOS 16
#define MIN_POOL_PAGES 32

static struct kmem_cache *_io_pool;

static struct io *alloc_io(struct sadc_c *sc, struct bio *bio)
{
        struct io *io = mempool_alloc(sc->io_pool, GFP_NOIO);

        if (unlikely(!io)) {
                DMERR("Cannot allocate io from mempool.");
                return NULL;
        }

        memset(io, 0, sizeof(*io));
        io->sc = sc;
        io->bio = bio;
        io->error = 0;
        atomic_set(&io->pending, 0);

        return io;
}

static void release_io(struct io *io)
{
        struct sadc_c *sc = io->sc;
        struct bio *bio = io->bio;
        int error = io->error;

        WARN_ON(atomic_read(&io->pending));
        mempool_free(io, sc->io_pool);

        if (bio)
                bio_endio(bio, error);
}

static void gc(struct sadc_c *sc, int error);

static void endio(struct bio *clone, int error)
{
        struct io *io = clone->bi_private;
        struct sadc_c *sc = io->sc;

        if (unlikely(!bio_flagged(clone, BIO_UPTODATE) && !error))
                io->error = -EIO;

        DBG(clone, error);

        bio_put(clone);
        if (atomic_dec_and_test(&io->pending)) {
                bool gc_io = io->bio == NULL;
                release_io(io);
                if (gc_io)
                        gc(sc, error);
        }
}

static bool get_args(struct dm_target *ti, struct sadc_c *sc,
                     int argc, char **argv)
{
        unsigned long long tmp;
        char dummy;

        if (argc != 5) {
                ti->error = "dm-sadc: Invalid argument count.";
                return false;
        }

        /* TODO: Change bounds back after testing. */
        if (sscanf(argv[1], "%llu%c", &tmp, &dummy) != 1 || tmp & 0xfff ||
            (tmp < /* 512 */ 4 * 1024 || tmp > 2 * 1024 * 1024)) {
                ti->error = "dm-sadc: Invalid track size.";
                return false;
        }
        sc->track_size = tmp;

        if (sscanf(argv[2], "%llu%c", &tmp, &dummy) != 1 || tmp < 1 /* 20 */ || tmp > 200) {
                ti->error = "dm-sadc: Invalid band size.";
                return false;
        }
        sc->band_size_tracks = tmp;

        if (sscanf(argv[3], "%llu%c", &tmp, &dummy) != 1 || tmp < 1 || tmp > 50 /* 20 */) {
                ti->error = "dm-sadc: Invalid cache percent.";
                return false;
        }
        sc->cache_percent = tmp;

        if (sscanf(argv[4], "%llu%c", &tmp, &dummy) != 1 ||
            tmp < MIN_DISK_SIZE || tmp > MAX_DISK_SIZE) {
                ti->error = "dm-sadc: Invalid disk size.";
                return false;
        }
        sc->disk_size = tmp;

        return true;
}

static void debug_print(struct sadc_c *sc)
{
        DMERR("Device: %s",                  sc->dev->name);
        DMERR("Disk size: %Lu bytes",        sc->disk_size);
        DMERR("Band size: %Lu bytes",        sc->band_size);
        DMERR("Band size: %d pbas",          sc->band_size_pbas);
        DMERR("Total number of bands: %d",   sc->num_bands);
        DMERR("Number of cache bands: %d",   sc->num_cache_bands);
        DMERR("Cache size: %Lu bytes",       sc->cache_size);
        DMERR("Number of data bands: %d",    sc->num_data_bands);
        DMERR("Usable disk size: %Lu bytes", sc->usable_size);
        DMERR("Number of usable pbas: %d",   sc->num_usable_pbas);
        DMERR("Wasted disk size: %Lu bytes", sc->wasted_size);
}

static void calc_params(struct sadc_c *sc)
{
        sc->band_size       = sc->band_size_tracks * sc->track_size;
        sc->band_size_pbas  = sc->band_size / PBA_SIZE;
        sc->num_bands       = sc->disk_size / sc->band_size;
        sc->num_cache_bands = sc->num_bands * sc->cache_percent / 100;
        sc->cache_size      = sc->num_cache_bands * sc->band_size;

        /* Make |num_data_bands| a multiple of |num_cache_bands| so that all
         * cache bands are equally loaded. */
        sc->num_data_bands = (sc->num_bands / sc->num_cache_bands - 1) *
                sc->num_cache_bands;

        sc->cache_assoc     = sc->num_data_bands / sc->num_cache_bands;
        sc->usable_size     = sc->num_data_bands * sc->band_size;
        sc->wasted_size     = sc->disk_size - sc->cache_size - sc->usable_size;
        sc->num_valid_pbas  = (sc->usable_size + sc->cache_size) / PBA_SIZE;
        sc->num_usable_pbas = sc->usable_size / PBA_SIZE;

        WARN_ON(sc->usable_size % PBA_SIZE);
}

static void reset_cache_band(struct sadc_c *sc, struct cache_band *cb)
{
        cb->current_pba = cb->begin_pba;
        bitmap_zero(cb->map, sc->cache_assoc);
}

static int reset_disk(struct sadc_c *sc)
{
        int i;

        DMERR("Resetting disk...");

        if (!mutex_trylock(&sc->c_lock)) {
                DMERR("Cannot reset -- GC in progres...");
                return -EIO;
        }

        for (i = 0; i < sc->num_cache_bands; i++)
                reset_cache_band(sc, &sc->cache_bands[i]);
        memset(sc->pba_map, -1, sizeof(int32_t) * sc->num_usable_pbas);

        mutex_unlock(&sc->c_lock);

        return 0;
}

static bool alloc_structs(struct sadc_c *sc)
{
        int32_t i, size, pba;

        size = sizeof(int32_t) * sc->num_usable_pbas;
        sc->pba_map = vmalloc(size);
        if (!sc->pba_map)
                return false;
        memset(sc->pba_map, -1, size);
        sc->rmw_data_band = -1;

        size = sizeof(struct cache_band) * sc->num_cache_bands;
        sc->cache_bands = vmalloc(size);
        if (!sc->cache_bands)
                return false;

        /* The cache region starts where the data region ends. */
        pba = sc->num_usable_pbas;
        size = BITS_TO_LONGS(sc->cache_assoc) * sizeof(long);
        for (i = 0; i < sc->num_cache_bands; ++i) {
                unsigned long *bitmap = kmalloc(size, GFP_KERNEL);
                if (!bitmap)
                        return false;
                sc->cache_bands[i].map = bitmap;

                sc->cache_bands[i].begin_pba = pba;
                sc->cache_bands[i].begin_data_band = i;
                reset_cache_band(sc, &sc->cache_bands[i]);
                pba += sc->band_size_pbas;
        }
        return true;
}

static void sadc_dtr(struct dm_target *ti);

static int sadc_ctr(struct dm_target *ti, unsigned int argc, char **argv)
{
        struct sadc_c *sc;
        int32_t ret;

        DMERR("Constructing...");

        sc = kzalloc(sizeof(*sc), GFP_KERNEL);
        if (!sc) {
                ti->error = "dm-sadc: Cannot allocate sadc context.";
                return -ENOMEM;
        }
        ti->private = sc;

        if (!get_args(ti, sc, argc, argv)) {
                kzfree(sc);
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

        sc->page_pool = mempool_create_page_pool(MIN_POOL_PAGES, 0);
        if (!sc->page_pool) {
                ti->error = "Cannot allocate page mempool";
                goto bad;
        }

        sc->bs = bioset_create(MIN_IOS, 0);
        if (!sc->bs) {
                ti->error = "Cannot allocate bioset.";
                goto bad;
        }

        sc->queue = alloc_workqueue("sadcd",
                                    WQ_NON_REENTRANT | WQ_MEM_RECLAIM, 1);
        if (!sc->queue) {
                ti->error = "Cannot allocate work queue.";
                goto bad;
        }

        ret = -EINVAL;
        if (dm_get_device(ti, argv[0], dm_table_get_mode(ti->table), &sc->dev)) {
                ti->error = "dm-sadc: Device lookup failed.";
                return -1;
        }

        mutex_init(&sc->c_lock);

        debug_print(sc);

        /* TODO: Reconsider proper values for these. */
        ti->num_flush_bios = 1;
        ti->num_discard_bios = 1;
        ti->num_write_same_bios = 1;
        return 0;

bad:
        sadc_dtr(ti);
        return ret;
}

static void sadc_dtr(struct dm_target *ti)
{
        int i;
        struct sadc_c *sc = (struct sadc_c *) ti->private;

        DMERR("Destructing...");

        ti->private = NULL;
        if (!sc)
                return;

        if (sc->queue)
                destroy_workqueue(sc->queue);
        if (sc->page_pool)
                mempool_destroy(sc->page_pool);
        if (sc->io_pool)
                mempool_destroy(sc->io_pool);
        if (sc->dev)
                dm_put_device(ti, sc->dev);

        for (i = 0; i < sc->num_cache_bands; ++i)
                if (sc->cache_bands[i].map)
                        kfree(sc->cache_bands[i].map);

        if (sc->pba_map)
                vfree(sc->pba_map);
        if (sc->cache_bands)
                vfree(sc->cache_bands);
        kzfree(sc);
}

/* Returns the band on which |lba| resides. */
static int32_t band(struct sadc_c *sc, sector_t lba)
{
        return lba_to_pba(lba) / sc->band_size_pbas;
}

/* Returns the cache band for the |band|. */
static struct cache_band *cache_band(struct sadc_c *sc, int32_t band)
{
        int i = band % sc->num_cache_bands;

        WARN_ON(!(0 <= i && i < sc->num_cache_bands));

        return &sc->cache_bands[i];
}

/* Returns available space in pbas in cache band |cb|.  */
static int32_t space_in_cache_band(struct sadc_c *sc, struct cache_band *cb)
{
        return sc->band_size_pbas - (cb->current_pba - cb->begin_pba);
}

/* Returns PBA to which |lba| was mapped. */
static sector_t lookup_lba(struct sadc_c *sc, sector_t lba)
{
        int32_t mpba, pba = lba_to_pba(lba);

        WARN_ON(!(0 <= pba && pba < sc->num_usable_pbas));

        mpba = sc->pba_map[pba];
        if (mpba == -1)
                return lba;

        WARN_ON(!(sc->num_usable_pbas <= mpba && mpba < sc->num_valid_pbas));

        return pba_to_lba(mpba) + lba % LBAS_IN_PBA;
}

/*
 * Maps |lba| to an lba in the cache band.  Also sets the bit for the data band
 * to which |lba| belongs.
 */
static sector_t map_lba(struct sadc_c *sc, sector_t lba)
{
        int32_t mpba, pba = lba_to_pba(lba), b = band(sc, lba);
        struct cache_band *cb = cache_band(sc, b);
        int i = (b - cb->begin_data_band) / sc->num_cache_bands;

        WARN_ON(!space_in_cache_band(sc, cb));
        WARN_ON(!(0 <= pba && pba < sc->num_usable_pbas));
        WARN_ON(!(0 <= b && b < sc->num_data_bands));
        WARN_ON(!(0 <= i && i < sc->cache_assoc));

        sc->pba_map[pba] = cb->current_pba++;
        i = (b - cb->begin_data_band) / sc->num_cache_bands;
        set_bit(i, cb->map);
        mpba = sc->pba_map[pba];

        WARN_ON(!(sc->num_usable_pbas <= mpba && mpba < sc->num_valid_pbas));

        return pba_to_lba(mpba);
}

#define bio_pbas bio_segments

/* Returns whether a |bio| is contiguously mapped. */
static bool contiguous(struct sadc_c *sc, struct bio *bio)
{
        sector_t base = bio->bi_sector;
        int i;

        for (i = 0; i < bio_pbas(bio) - 1; ++i, base += LBAS_IN_PBA) {
                sector_t curr = lookup_lba(sc, base + i * LBAS_IN_PBA);
                sector_t next = lookup_lba(sc, base + (i+1) * LBAS_IN_PBA);
                if (curr + LBAS_IN_PBA != next)
                        return false;
        }
        return true;
}

/* Given a bio and a band, returns the size of bio in that band in pbas. */
static int32_t size_in_band(struct sadc_c *sc, struct bio *bio, int32_t band)
{
        int32_t bb = band * sc->band_size_pbas;
        int32_t be = bb + sc->band_size_pbas;

        int32_t b = max(bb, lba_to_pba(bio->bi_sector));
        int32_t e = min(be, lba_to_pba(bio_end_sector(bio)));

        return e > b ? e - b : 0;
}

/*
 * Returns whether |bio| crosses bands.  Given the sizes at the top of this file
 * and the fact that the kernel does not allow bios larger than 512 KB (I need
 * to confirm this), we assume that a bio can cross at most two bands.
 */
static bool does_not_cross_bands(struct sadc_c *sc, struct bio *bio)
{
        int32_t nb = band(sc, bio->bi_sector) + 1;

        return bio_end_sector(bio) <= pba_to_lba(nb * sc->band_size_pbas);
}

/* Returns whether |bio| is 4 KB aligned.  */
static bool aligned(struct bio *bio)
{
        return !(bio->bi_sector & 0x7) && !(bio->bi_size & 0xfff);
}

/* Returns whether writing |bio| to the cache band will cause GC.  */
static bool does_not_require_gc(struct sadc_c *sc, struct bio *bio)
{
        int32_t b = band(sc, bio->bi_sector);
        struct cache_band *cb = cache_band(sc, b);
        int32_t s = size_in_band(sc, bio, b);

        if (space_in_cache_band(sc, cb) < s)
                return false;

        s = bio_pbas(bio) - s;
        if (!s)
                return true;

        WARN_ON(s > sc->band_size_pbas);

        cb = cache_band(sc, b+1);
        return space_in_cache_band(sc, cb) >= s;
}


/* TODO: Consider name change. */
/* Returns true if |bio| can complete just by doing a remap. */
static bool fast(struct sadc_c *sc, struct bio *bio)
{
        if (bio_data_dir(bio) == READ)
                return contiguous(sc, bio);

        return aligned(bio) &&
                does_not_cross_bands(sc, bio) &&
                does_not_require_gc(sc, bio);
}

static void remap(struct sadc_c *sc, struct bio *bio)
{
        bio->bi_bdev = sc->dev->bdev;

        if (bio_data_dir(bio) == READ)
                bio->bi_sector = lookup_lba(sc, bio->bi_sector);
        else
                bio->bi_sector = map_lba(sc, bio->bi_sector);

        DBG(bio, 0);
}

static void init_bio(struct io *io, struct bio *bio,
                     sector_t sector, int idx, int vcnt)
{
        struct sadc_c *sc = io->sc;

        bio->bi_private = io;
        bio->bi_end_io = endio;
        bio->bi_idx = idx;
        bio->bi_vcnt = vcnt;
        bio->bi_sector = sector;
        bio->bi_size = vcnt * PAGE_SIZE;
        bio->bi_bdev = sc->dev->bdev;
}

static struct bio *clone_bio(struct io *io, struct bio *bio,
                             sector_t sector, int idx, int vcnt)
{
        struct sadc_c *sc = io->sc;
        struct bio *clone;

        clone = bio_clone_bioset(bio, GFP_NOIO, sc->bs);
        if (!clone)
                return NULL;

        init_bio(io, clone, sector, idx, vcnt);
        atomic_inc(&io->pending);

        return clone;
}

static struct bio *alloc_bio(struct io *io, sector_t sector, int vcnt)
{
        struct sadc_c *sc = io->sc;
        struct bio *bio;
        struct page *page;

        bio = bio_alloc_bioset(GFP_NOIO, vcnt, sc->bs);
        if (!bio)
                return NULL;

        init_bio(io, bio, sector, 0, 0);

        page = mempool_alloc(sc->page_pool, GFP_NOIO | __GFP_HIGHMEM);
        if (!page)
                goto bad;
        if (!bio_add_page(bio, page, PAGE_SIZE, 0))
                goto bad;

        atomic_inc(&io->pending);

        return bio;

bad:
        if (page)
                mempool_free(page, sc->page_pool);
        if (bio)
                bio_put(bio);
        return NULL;
}

static void release_bio(struct bio *bio)
{
        struct io *io = bio->bi_private;

        atomic_dec(&io->pending);
        bio_put(bio);
}

/* This can split a bio to at most 2. */
static int split_write_bio(struct sadc_c *sc, struct io *io, struct bio **bios)
{
        struct bio *bio = io->bio;
        int32_t b = band(sc, bio->bi_sector);
        int vcnt = size_in_band(sc, bio, b);
        sector_t sector = map_lba(sc, bio->bi_sector);
        int idx = 0;

        bios[0] = clone_bio(io, bio, sector, idx, vcnt);
        if (!bios[0])
                return io->error = -ENOMEM;

        vcnt = bio_pbas(bio) - vcnt;
        if (!vcnt)
                return 1;

        sector = map_lba(sc, bio->bi_sector + pba_to_lba(vcnt));
        idx += vcnt;
        bios[1] = clone_bio(io, bio, sector, idx, vcnt);
        if (!bios[1]) {
                release_bio(bios[0]);
                return io->error = -ENOMEM;
        }
        return 2;
}

static int split_read_bio(struct sadc_c *sc, struct io *io, struct bio **bios)
{
        struct bio *bio = io->bio;
        sector_t base = bio->bi_sector;
        sector_t sector = lookup_lba(sc, base);
        int i, n = 0, idx = 0;

        for (i = 1; i < bio_pbas(bio); ++i) {
                sector_t prev = lookup_lba(sc, base + (i-1) * LBAS_IN_PBA);
                sector_t curr = lookup_lba(sc, base + i * LBAS_IN_PBA);
                if (prev + LBAS_IN_PBA == curr)
                        continue;
                bios[n] = clone_bio(io, bio, sector, idx, i - idx);
                if (!bios[n])
                        goto bad;
                sector = curr, idx = i, n++;
        }
        bios[n] = clone_bio(io, bio, sector, idx, i - idx);
        if (bios[n])
                return n + 1;

bad:
        while (n--)
                release_bio(bios[n]);
        return io->error = -ENOMEM;
}

typedef int (*split_f)(struct sadc_c *sc, struct io *io, struct bio **bios);

static void complete_delayed_io(struct sadc_c *sc, struct io *io, split_f split)
{
        struct bio *bios[MAX_SEGMENTS_IN_BIO];
        int i, n;

        n = split(sc, io, bios);
        if (n < 0) {
                release_io(io);
                return;
        }

        for (i = 0; i < n; ++i)
                generic_make_request(bios[i]);
}

/*
 * Worker thread entry point.
 *
 * There are different kinds of writes each of which needs special
 * handling.  Starting from the simplest, these are the following:
 *
 * - Aligned, does not cross bands, does not require GC.  This case is
 *   handled in |sadc_map| because a simple remap is enough for this.
 *
 * - Aligned, crosses bands, none of the sections require GC.  We will
 *   handle this case below, via |do_split_io|.
 *
 * - Aligned, does not cross bands, requires GC.  This requires GC of a
 *   single cache band.
 *
 * - Aligned, crosses bands, one or both sections require GC.  This
 *   requires GC of at least one, at most two cache bands.
 *
 * - There are the same variations for the unaligned writes, which we do
 *   not consider right now.  We will consider them if we see them in
 *   practice.
 */

static void do_start_gc(struct sadc_c *sc)
{
        struct io *io = sc->pending_io;
        struct bio *bio = io->bio;
        int32_t b = band(sc, bio->bi_sector), s = size_in_band(sc, bio, b);
        struct cache_band *cb = cache_band(sc, b);

        WARN_ON(does_not_require_gc(sc, bio));

        sc->state = STATE_START_CACHE_BAND_GC;

        if (space_in_cache_band(sc, cb) < s) {
                DMERR("Setting cache band %d", b % sc->num_cache_bands);
                sc->gc_cache_band = cb;
        } else {
                DMERR("Setting cache band %d", (b+1) % sc->num_cache_bands);
                sc->gc_cache_band = cache_band(sc, b+1);
        }
}

static inline int band_to_bit(struct sadc_c *sc, struct cache_band *cb,
                              int32_t band)
{
        return (band - cb->begin_data_band) / sc->num_cache_bands;
}

static inline int bit_to_band(struct sadc_c *sc, struct cache_band *cb, int bit)
{
        return bit * sc->num_cache_bands + cb->begin_data_band;
}

static void do_start_cache_band_gc(struct sadc_c *sc)
{
        struct cache_band *cb = sc->gc_cache_band;
        int i;

        WARN_ON(!cb || !bitmap_weight(cb->map, sc->cache_assoc));
        sc->state = STATE_READ_DATA_BAND;

        i = find_first_bit(cb->map, sc->cache_assoc);
        sc->rmw_data_band = bit_to_band(sc, cb, i);
        DMERR("Setting RMW band to %d", sc->rmw_data_band);
}

static int do_read_data_band(struct sadc_c *sc)
{
        struct io *io;

        struct bio *nbios[MAX_SEGMENTS_IN_BIO];
        int32_t i, pba = sc->rmw_data_band * sc->band_size_pbas;

        WARN_ON(sc->rmw_data_band == -1);
        sc->state = STATE_MODIFY_DATA_BAND;

        io = alloc_io(sc, NULL);
        if (!io)
                return -ENOMEM;
        for (i = 0; i < sc->band_size_pbas; ++i) {
                nbios[i] = alloc_bio(io, pba_to_lba(pba + i), 1);
                if (!nbios[i])
                        goto bad;
        }

        /*
         * Start requests once all of the allocations succeed.  Also make copies
         * of bios since these bios are going to act as an RMW buffer.
         * Increment the refcounts so that bios do not disappear once |endio|
         * calls |bio_put| on them.  The following modify and write operations
         * will act on these bios.
         */
        for (i = 0; i < sc->band_size_pbas; ++i) {
                sc->bios[i] = nbios[i];
                bio_get(nbios[i]);
                generic_make_request(nbios[i]);
        }
        return -EINPROGRESS;

bad:
        while (i--)
                release_bio(nbios[i]);
        release_io(io);

        return -ENOMEM;
}

static int do_modify_data_band(struct sadc_c *sc, int error)
{
        struct io *io;
        struct bio *nbios[MAX_SEGMENTS_IN_BIO];
        int32_t i, j, pba = sc->rmw_data_band * sc->band_size_pbas;

        if (error)
                return error;

        WARN_ON(sc->rmw_data_band == -1);
        sc->state = STATE_WRITE_DATA_BAND;

        io = alloc_io(sc, NULL);
        if (!io)
                return -ENOMEM;

        for (i = j = 0; i < sc->band_size_pbas; ++i) {
                sector_t sector = lookup_lba(sc, sc->bios[i]->bi_sector);
                if (sector == pba_to_lba(pba + i))
                        continue;
                nbios[j] = clone_bio(io, sc->bios[i], sector, 0, 1);
                if (!nbios[j])
                        goto bad;
                ++j;
        }

        /*
         * There should definitely be something to modify, or we should not be
         * doing RMW on this band.  Otherwise, we will return -EINPROGRESS, but
         * no request will be launched and |gc| will not be called.
         */
        WARN_ON(!j);

        for (i = 0; i < j; ++i)
                generic_make_request(nbios[i]);

        return -EINPROGRESS;

bad:
        while (j--)
                release_bio(nbios[j]);
        release_io(io);

        return -ENOMEM;
}

static int do_write_data_band(struct sadc_c *sc, int error)
{
        struct io *io;
        int i;

        if (error)
                return error;

        WARN_ON(sc->rmw_data_band == -1);
        sc->state = STATE_RMW_COMPLETE;

        io = alloc_io(sc, NULL);
        if (!io)
                return -ENOMEM;

        for (i = 0; i < sc->band_size_pbas; ++i) {
                sc->bios[i]->bi_private = io;
                sc->bios[i]->bi_rw = WRITE;
                atomic_inc(&io->pending);
                generic_make_request(sc->bios[i]);
        }
        return -EINPROGRESS;
}

static int do_rmw_complete(struct sadc_c *sc, int error)
{
        struct cache_band *cb = sc->gc_cache_band;
        int i;

        if (error)
                return error;

        WARN_ON(sc->rmw_data_band == -1);

        i = band_to_bit(sc, cb, sc->rmw_data_band);
        clear_bit(i, cb->map);
        sc->rmw_data_band = -1;

        if (bitmap_weight(cb->map, sc->cache_assoc))
                sc->state = STATE_START_CACHE_BAND_GC;
        else
                sc->state = STATE_GC_COMPLETE;
        return 0;
}

static void queue_io(struct io *io);

static void do_gc_complete(struct sadc_c *sc)
{
        struct io *io = sc->pending_io;
        struct bio *bio = io->bio;

        reset_cache_band(sc, sc->gc_cache_band);
        sc->gc_cache_band = NULL;

        if (does_not_require_gc(sc, bio)) {
                sc->state = STATE_NONE;
                queue_io(sc->pending_io);
                sc->pending_io = NULL;

                mutex_unlock(&sc->c_lock);
        } else {
                sc->state = STATE_START_GC;
        }
}

static void do_fail_gc(struct sadc_c *sc, int error)
{
        struct io *io = sc->pending_io;

        sc->state = STATE_NONE;
        sc->pending_io = NULL;
        sc->gc_cache_band = NULL;

        mutex_unlock(&sc->c_lock);

        io->error = error;
        DMERR("Failing GC.");
        release_io(io);
}

static void gc(struct sadc_c *sc, int error)
{
        WARN_ON(sc->state == STATE_NONE);
        do {
                int state = sc->state;
                sc->state = STATE_NONE;

                switch (state) {
                case STATE_START_GC:
                        DMERR("STATE_START_GC");
                        WARN_ON(error);
                        do_start_gc(sc);
                        break;
                case STATE_START_CACHE_BAND_GC:
                        DMERR("STATE_START_CACHE_BAND_GC");
                        WARN_ON(error);
                        do_start_cache_band_gc(sc);
                        break;
                case STATE_READ_DATA_BAND:
                        DMERR("STATE_READ_DATA_BAND");
                        WARN_ON(error);
                        error = do_read_data_band(sc);
                        break;
                case STATE_MODIFY_DATA_BAND:
                        DMERR("STATE_MODIFY_DATA_BAND");
                        error = do_modify_data_band(sc, error);
                        break;
                case STATE_WRITE_DATA_BAND:
                        DMERR("STATE_WRITE_DATA_BAND");
                        error = do_write_data_band(sc, error);
                        break;
                case STATE_RMW_COMPLETE:
                        DMERR("STATE_RMW_COMPLETE");
                        error = do_rmw_complete(sc, error);
                        break;
                case STATE_GC_COMPLETE:
                        DMERR("STATE_GC_COMPLETE");
                        WARN_ON(error);
                        do_gc_complete(sc);
                        break;
                default:
                        WARN_ON(1);
                        break;
                }
        } while (!error && sc->state != STATE_NONE);

        if (sc->state != STATE_NONE && error != -EINPROGRESS)
                do_fail_gc(sc, error);
}

static void start_gc(struct sadc_c *sc, struct io *io)
{
        DMERR("Starting gc for bio");
        DBG(io->bio, 0);

        mutex_lock(&sc->c_lock);

        sc->pending_io = io;
        sc->state = STATE_START_GC;
        gc(sc, 0);
}

static void sadcd(struct work_struct *work)
{
        struct io *io = container_of(work, struct io, work);
        struct bio *bio = io->bio;
        struct sadc_c *sc = io->sc;

        WARN_ON(!aligned(bio));

        if (bio_data_dir(bio) == READ)
                complete_delayed_io(sc, io, split_read_bio);
        else if (does_not_require_gc(sc, bio))
                complete_delayed_io(sc, io, split_write_bio);
        else
                start_gc(sc, io);
}

static void queue_io(struct io *io)
{
        struct sadc_c *sc = io->sc;

        INIT_WORK(&io->work, sadcd);
        queue_work(sc->queue, &io->work);
}

static int sadc_map(struct dm_target *ti, struct bio *bio)
{
        struct sadc_c *sc = ti->private;
        struct io *io;

        DBG(bio, -1);

        if (fast(sc, bio)) {
                remap(sc, bio);
                return DM_MAPIO_REMAPPED;
        }

        io = alloc_io(sc, bio);
        queue_io(io);
        return DM_MAPIO_SUBMITTED;
}

static int sadc_ioctl(struct dm_target *ti, unsigned int cmd, unsigned long arg)
{
        struct sadc_c *sc = ti->private;

        if (cmd == RESET_DISK)
                return reset_disk(sc);
        return -EINVAL;
}

static void sadc_status(struct dm_target *ti, status_type_t type,
                           unsigned status_flags, char *result, unsigned maxlen)
{
        struct sadc_c *sc = (struct sadc_c *) ti->private;

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

static struct target_type sadc_target = {
        .name   = "sadc",
        .version = {1, 0, 0},
        .module = THIS_MODULE,
        .ctr    = sadc_ctr,
        .dtr    = sadc_dtr,
        .map    = sadc_map,
        .status = sadc_status,
        .ioctl  = sadc_ioctl,
};

static int __init dm_sadc_init(void)
{
        int r;

        _io_pool = KMEM_CACHE(io, 0);
        if (!_io_pool)
                return -ENOMEM;

        r = dm_register_target(&sadc_target);
        if (r < 0) {
                DMERR("register failed %d", r);
                kmem_cache_destroy(_io_pool);
        }

        return r;
}

static void __exit dm_sadc_exit(void)
{
        dm_unregister_target(&sadc_target);
}

module_init(dm_sadc_init)
module_exit(dm_sadc_exit)

MODULE_AUTHOR("Abutalib Aghayev <aghayev@ccs.neu.edu>");
MODULE_DESCRIPTION(DM_NAME " set-associative disk cache STL emulator target");
MODULE_LICENSE("GPL");
