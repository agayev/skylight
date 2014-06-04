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
 * TODO: Consider turning some WARN_ONs to debug mode only.
 * TODO: Consider using bio_split, bio_trim etc.
 * TODO: Reconsider lba/pba boundaries.
 * TODO: Revisit cache_band, band, etc.
 * TODO: Post and pre-conditions for all functions.
 * TODO: Consider every functions signature.
 * TODO: Consider names.
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

typedef sector_t lba_t;
typedef int32_t pba_t;

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
        int32_t nr_cache_bands;

        int64_t usable_size;  /* Usable size, excluding the cache region. */
        int64_t wasted_size;  /* Wasted due to alignment. */

        int32_t track_size;
        int64_t band_size;
        int32_t band_size_tracks;
        int32_t band_size_pbas;

        /* Number of valid pbas.  Includes the cache region. */
        int32_t nr_valid_pbas;

        /* Number of usable pbas.  Excludes the cache region. */
        int32_t nr_data_pbas;

        /* Maps a pba to its location in the cache region.  Contains -1 for
         * unmapped pbas. */
        pba_t *pba_map;

        struct cache_band *cache_bands;

        int32_t nr_bands;    /* Total number of available bands. */

        /* Number of data bands.  These make up the usable region of the
         * disk. */
        int32_t nr_data_bands;

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
        struct cache_band *gc_band;

        /* Data band currently undergoing RMW. */
        int32_t rmw_band;

        /*
         * This are the bios that act as an RMW buffer.  We read the data band
         * to these, and then execute modify by cloning those of these that need
         * to be modified and reading into them and finally, write them back.
         */
        struct bio *bios[MAX_SEGMENTS_IN_BIO];
};

struct cache_band {
        pba_t begin_pba;    /* Where the cache band begins. */
        pba_t current_pba;  /* Where the next write will go. */
        int32_t nr;         /* Cache band number. */

        /*
         * For every data band that has a block on this cache band, the bit in
         * the data band's position is turned on.
         */
        unsigned long *map;
};

/*
 * This represents a single I/O operation, either read or write.  The |bio|
 * associated with it may result in multiple bios being generated, all of which
 * should complete before this one can be considered complete.  Every generated
 * bio increments |pending| and every completed bio decrements it.  When
 * |pending| reaches zero, we deallocate |io| and signal the completion of |bio|
 * by calling |bio_endio| on it.
 *
 * If |bio| is NULL, then it is a GC generated I/O operation.  Again, it usually
 * results in multiple bios which increment |pending| upon launch and decrement
 * it upon completion.  However, the non-NULL |bio| case, when all of the bios
 * are complete, |endio| returns to GC state machine by invoking |gc|.
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

static inline bool data_pba(struct sadc_c *sc, pba_t pba)
{
        return 0 <= pba && pba < sc->nr_data_pbas;
}

static inline bool cache_pba(struct sadc_c *sc, pba_t pba)
{
        return sc->nr_data_pbas <= pba && pba < sc->nr_valid_pbas;
}

#define lba_to_pba(lba) ((pba_t) (lba / LBAS_IN_PBA))
#define pba_to_lba(pba) (((lba_t) pba) * LBAS_IN_PBA)

static inline bool data_band(struct sadc_c *sc, int32_t band)
{
        return 0 <= band && band < sc->nr_data_bands;
}

/*
 * Since we work with 4 KB blocks, number of pbas is equivalent to the number of
 * segments.
 */
#define bio_pbas bio_segments

#define bio_begin_lba(bio) ((bio)->bi_sector)
#define bio_end_lba bio_end_sector

#define bio_begin_pba(bio) (lba_to_pba(bio_begin_lba(bio)))
#define bio_end_pba(bio) (lba_to_pba(bio_end_lba(bio)))

static inline pba_t band_begin_pba(struct sadc_c *sc, int32_t band)
{
        WARN_ON(!data_band(sc, band));

        return band * sc->band_size_pbas;
}

static inline pba_t band_end_pba(struct sadc_c *sc, int32_t band)
{
        return band_begin_pba(sc, band) + sc->band_size_pbas;
}

static inline void debug_bio(struct sadc_c *sc, struct bio *bio, const char *f)
{
        pr_debug("%10s: %c offset: %d size: %u\n",
                 f,
                 (bio_data_dir(bio) == READ ? 'R' : 'W'),
                 bio_begin_pba(bio),
                 bio->bi_size / PBA_SIZE);
}

/* Returns the band on which |pba| resides. */
static inline int32_t band(struct sadc_c *sc, pba_t pba)
{
        WARN_ON(!data_pba(sc, pba));

        return pba / sc->band_size_pbas;
}

static inline int32_t bio_band(struct sadc_c *sc, struct bio *bio)
{
        return band(sc, bio_begin_pba(bio));
}

/* Returns the cache band for the |band|. */
static inline struct cache_band *cache_band(struct sadc_c *sc, int32_t band)
{
        WARN_ON(!data_band(sc, band));

        return &sc->cache_bands[band % sc->nr_cache_bands];
}

/* Returns available space in pbas in cache band |cb|.  */
static inline int32_t free_pbas_in_cache_band(struct sadc_c *sc,
                                              struct cache_band *cb)
{
        return sc->band_size_pbas - (cb->current_pba - cb->begin_pba);
}

static void gc(struct sadc_c *sc, int error);

static void endio(struct bio *clone, int error)
{
        struct io *io = clone->bi_private;
        struct sadc_c *sc = io->sc;

        if (unlikely(!bio_flagged(clone, BIO_UPTODATE) && !error))
                io->error = -EIO;

        debug_bio(sc, clone, __func__);

        bio_put(clone);
        if (atomic_dec_and_test(&io->pending)) {
                bool gc_io = (io->bio == NULL);
                release_io(io);
                if (gc_io)
                        gc(sc, error);
        }
}

static void reset_cache_band(struct sadc_c *sc, struct cache_band *cb)
{
        cb->current_pba = cb->begin_pba;
        bitmap_zero(cb->map, sc->cache_assoc);
}

static int reset_disk(struct sadc_c *sc)
{
        int i;

        DMINFO("Resetting disk...");

        if (!mutex_trylock(&sc->c_lock)) {
                DMINFO("Cannot reset -- GC in progres...");
                return -EIO;
        }

        for (i = 0; i < sc->nr_cache_bands; ++i)
                reset_cache_band(sc, &sc->cache_bands[i]);
        memset(sc->pba_map, -1, sizeof(pba_t) * sc->nr_data_pbas);

        mutex_unlock(&sc->c_lock);

        return 0;
}

/* Returns bands position in |cb|'s bitmap. */
static inline int band_to_pos(struct sadc_c *sc, struct cache_band *cb,
                              int32_t band)
{
        return (band - cb->nr) / sc->nr_cache_bands;
}

/* Given the band's |pos|ition in |cb|, returns the band. */
static inline int pos_to_band(struct sadc_c *sc, struct cache_band *cb, int pos)
{
        return pos * sc->nr_cache_bands + cb->nr;
}

/* Returns PBA to which |pba| was mapped. */
static pba_t lookup_pba(struct sadc_c *sc, pba_t pba)
{
        WARN_ON(!data_pba(sc, pba));

        if (sc->pba_map[pba] == -1)
                return pba;
        return sc->pba_map[pba];
}

/* Returns LBA to which |lba| was mapped. */
static lba_t lookup_lba(struct sadc_c *sc, lba_t lba)
{
        return pba_to_lba(lookup_pba(sc, lba_to_pba(lba))) + lba % LBAS_IN_PBA;
}

static pba_t map_pba(struct sadc_c *sc, pba_t pba)
{
        struct cache_band *cb;
        int32_t b;

        WARN_ON(!data_pba(sc, pba));

        b = band(sc, pba);
        cb = cache_band(sc, b);

        WARN_ON(!free_pbas_in_cache_band(sc, cb));

        sc->pba_map[pba] = cb->current_pba++;
        set_bit(band_to_pos(sc, cb, b), cb->map);

        return sc->pba_map[pba];
}

static lba_t map_lba(struct sadc_c *sc, lba_t lba)
{
        return pba_to_lba(map_pba(sc, lba_to_pba(lba)));
}

static void clear_pba_map_for_rmw_band(struct sadc_c *sc)
{
        pba_t pba = band_begin_pba(sc, sc->rmw_band);
        pba_t end_pba = band_end_pba(sc, sc->rmw_band);

        WARN_ON(sc->rmw_band == -1);

        for ( ; pba < end_pba; ++pba)
                sc->pba_map[pba] = -1;
}

/* Returns whether a |bio| is contiguously mapped. */
static bool contiguous(struct sadc_c *sc, struct bio *bio)
{
        pba_t pba = bio_begin_pba(bio);
        pba_t end_pba = bio_end_pba(bio);

        for ( ; pba < end_pba-1; ++pba)
                if (lookup_pba(sc, pba)+1 != lookup_pba(sc, pba+1))
                        return false;
        return true;
}

/*
 * Given a bio and a band, returns the number of pbas the bio has in that band.
 */
static int32_t pbas_in_band(struct sadc_c *sc, struct bio *bio, int32_t band)
{
        pba_t begin_pba = max(band_begin_pba(sc, band), bio_begin_pba(bio));
        pba_t end_pba = min(band_end_pba(sc, band), bio_end_pba(bio));

        return max(end_pba - begin_pba, 0);
}

/*
 * Returns whether |bio| crosses bands.  Given the sizes at the top of this file
 * and the fact that the kernel does not allow bios larger than 512 KB (I need
 * to confirm this), we assume that a bio can cross at most two bands.
 */
static bool does_not_cross_bands(struct sadc_c *sc, struct bio *bio)
{
        int32_t b = bio_band(sc, bio) + 1;

        if (b >= sc->nr_data_bands)
                return true;
        return bio_end_pba(bio) <= band_begin_pba(sc, b);
}

/* Returns whether |bio| is 4 KB aligned.  */
static bool aligned(struct bio *bio)
{
        return !(bio_begin_lba(bio) & 0x7) && !(bio->bi_size & 0xfff);
}

/* Returns whether writing |bio| to the cache band will cause GC.  */
static bool does_not_require_gc(struct sadc_c *sc, struct bio *bio)
{
        int32_t b = bio_band(sc, bio);
        struct cache_band *cb = cache_band(sc, b);
        int32_t nr_pbas = pbas_in_band(sc, bio, b);

        if (free_pbas_in_cache_band(sc, cb) < nr_pbas)
                return false;

        nr_pbas = bio_pbas(bio) - nr_pbas;
        if (!nr_pbas)
                return true;

        cb = cache_band(sc, ++b);
        return free_pbas_in_cache_band(sc, cb) >= nr_pbas;
}

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

        if (bio_data_dir(bio) == READ) {
                bio_begin_lba(bio) = lookup_lba(sc, bio_begin_lba(bio));
        } else {
                /*
                 * Since the write is contiguous, it is enough to update the lba
                 * of the bio for it to proceed.  In the case of bios with
                 * multiple segments, we need to update our internal map as
                 * well, therefore we execute map_pba for the remaining pbas for
                 * the side effect of updating |sc->pba_map|.
                 */
                pba_t pba = bio_begin_pba(bio);
                pba_t end_pba = bio_end_pba(bio);

                bio_begin_lba(bio) = map_lba(sc, bio_begin_lba(bio));

                for (++pba; pba < end_pba; ++pba)
                        map_pba(sc, pba);
        }

        debug_bio(sc, bio, __func__);
}

static void init_bio(struct io *io, struct bio *bio,
                     lba_t lba, int idx, int pba_cnt)
{
        struct sadc_c *sc = io->sc;

        bio->bi_private = io;
        bio->bi_end_io = endio;
        bio->bi_idx = idx;
        bio->bi_sector = lba;
        bio->bi_size = pba_cnt * PBA_SIZE;
        bio->bi_bdev = sc->dev->bdev;
}

static struct bio *clone_bio(struct io *io, struct bio *bio,
                             lba_t lba, int idx, int nr_pbas)
{
        struct sadc_c *sc = io->sc;
        struct bio *clone;

        clone = bio_clone_bioset(bio, GFP_NOIO, sc->bs);
        if (unlikely(!clone)) {
                DMERR("Cannot clone a bio.");
                return NULL;
        }

        init_bio(io, clone, lba, idx, nr_pbas);
        atomic_inc(&io->pending);

        return clone;
}

static struct bio *alloc_bio(struct io *io, lba_t lba, int nr_pbas)
{
        struct sadc_c *sc = io->sc;
        struct bio *bio;
        struct page *page;

        bio = bio_alloc_bioset(GFP_NOIO, nr_pbas, sc->bs);
        if (unlikely(!bio)) {
                DMERR("Cannot allocate a new bio.");
                return NULL;
        }

        init_bio(io, bio, lba, 0, 0);

        page = mempool_alloc(sc->page_pool, GFP_NOIO);
        if (unlikely(!page)) {
                DMERR("Cannot allocate a page for a new bio.");
                goto bad;
        }

        if (unlikely(!bio_add_page(bio, page, PAGE_SIZE, 0))) {
                DMERR("Cannot add a page to a bio.");
                goto bad;
        }

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
        int32_t b = bio_band(sc, bio);
        int32_t nr_pbas = pbas_in_band(sc, bio, b);
        lba_t lba = map_lba(sc, bio_begin_lba(bio));
        int32_t idx = 0;

        bios[0] = clone_bio(io, bio, lba, idx, nr_pbas);
        if (!bios[0])
                return io->error = -ENOMEM;

        nr_pbas = bio_pbas(bio) - nr_pbas;
        if (!nr_pbas)
                return 1;

        lba = map_lba(sc, pba_to_lba(bio_begin_pba(bio) + nr_pbas));
        idx += nr_pbas;
        bios[1] = clone_bio(io, bio, lba, idx, nr_pbas);
        if (!bios[1]) {
                release_bio(bios[0]);
                return io->error = -ENOMEM;
        }
        return 2;
}

static int split_read_bio(struct sadc_c *sc, struct io *io, struct bio **bios)
{
        struct bio *bio = io->bio;
        pba_t pba = bio_begin_pba(bio);
        pba_t end_pba = bio_end_pba(bio);
        lba_t lba = lookup_lba(sc, bio_begin_lba(bio));
        int i = 0, n = 0, idx = 0;

        for (++i, ++pba; pba < end_pba; ++pba, ++i) {
                pba_t curr = lookup_pba(sc, pba);
                pba_t prev = lookup_pba(sc, pba-1);
                if (prev + 1 == curr)
                        continue;
                bios[n] = clone_bio(io, bio, lba, idx, i - idx);
                if (!bios[n])
                        goto bad;
                lba = pba_to_lba(curr), idx = i, ++n;
        }
        bios[n] = clone_bio(io, bio, lba, idx, i - idx);
        if (bios[n])
                return n + 1;

bad:
        while (n--)
                release_bio(bios[n]);
        return io->error = -ENOMEM;
}

typedef int (*split_t)(struct sadc_c *sc, struct io *io, struct bio **bios);

static void complete_delayed_io(struct sadc_c *sc, struct io *io, split_t split)
{
        struct bio *bios[MAX_SEGMENTS_IN_BIO];
        int i, n;

        n = split(sc, io, bios);
        if (n < 0) {
                release_io(io);
                return;
        }

        WARN_ON(bio_data_dir(io->bio) == READ && n == 1);

        if (bio_data_dir(io->bio) == READ)
                WARN_ON(n == 1);

        for (i = 0; i < n; ++i)
                generic_make_request(bios[i]);
}

/*
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
        int32_t b = bio_band(sc, bio);
        int32_t nr_pbas = pbas_in_band(sc, bio, b);
        struct cache_band *cb = cache_band(sc, b);

        WARN_ON(does_not_require_gc(sc, bio));

        sc->state = STATE_START_CACHE_BAND_GC;

        if (free_pbas_in_cache_band(sc, cb) < nr_pbas)
                sc->gc_band = cb;
        else
                sc->gc_band = cache_band(sc, ++b);

        pr_debug("sc->gc_band -> %d\n", b % sc->nr_cache_bands);
}

static void do_start_cache_band_gc(struct sadc_c *sc)
{
        struct cache_band *cb = sc->gc_band;
        int i;

        WARN_ON(!cb || !bitmap_weight(cb->map, sc->cache_assoc));
        sc->state = STATE_READ_DATA_BAND;

        i = find_first_bit(cb->map, sc->cache_assoc);
        sc->rmw_band = pos_to_band(sc, cb, i);
        pr_debug("sc->rmw_band -> %d\n", sc->rmw_band);
}

static int do_read_data_band(struct sadc_c *sc)
{
        struct io *io;
        struct bio *nbios[MAX_SEGMENTS_IN_BIO];
        pba_t pba = band_begin_pba(sc, sc->rmw_band);
        int32_t i;

        WARN_ON(sc->rmw_band == -1);
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
        pba_t pba = band_begin_pba(sc, sc->rmw_band);
        int32_t i, j;

        if (error)
                return error;

        WARN_ON(sc->rmw_band == -1);
        sc->state = STATE_WRITE_DATA_BAND;

        io = alloc_io(sc, NULL);
        if (!io)
                return -ENOMEM;

        for (i = j = 0; i < sc->band_size_pbas; ++i) {
                lba_t lba = lookup_lba(sc, bio_begin_lba(sc->bios[i]));
                if (lba == pba_to_lba(pba + i))
                        continue;
                nbios[j] = clone_bio(io, sc->bios[i], lba, 0, 1);
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

        WARN_ON(sc->rmw_band == -1);
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
        struct cache_band *cb = sc->gc_band;

        if (error)
                return error;

        WARN_ON(sc->rmw_band == -1);

        clear_bit(band_to_pos(sc, cb, sc->rmw_band), cb->map);
        clear_pba_map_for_rmw_band(sc);
        sc->rmw_band = -1;

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

        reset_cache_band(sc, sc->gc_band);
        sc->gc_band = NULL;

        if (does_not_require_gc(sc, bio)) {
                sc->state = STATE_NONE;
                queue_io(sc->pending_io);
                sc->pending_io = NULL;

                mutex_unlock(&sc->c_lock);
        } else {
                sc->state = STATE_START_GC;
        }
}

static void do_fail_gc(struct sadc_c *sc)
{
        struct io *io = sc->pending_io;

        sc->state = STATE_NONE;
        sc->pending_io = NULL;
        sc->gc_band = NULL;

        mutex_unlock(&sc->c_lock);

        io->error = -EIO;
        pr_debug("GC failed.\n");
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
                        pr_debug("STATE_START_GC\n");
                        WARN_ON(error);
                        do_start_gc(sc);
                        break;
                case STATE_START_CACHE_BAND_GC:
                        pr_debug("STATE_START_CACHE_BAND_GC\n");
                        WARN_ON(error);
                        do_start_cache_band_gc(sc);
                        break;
                case STATE_READ_DATA_BAND:
                        pr_debug("STATE_READ_DATA_BAND\n");
                        WARN_ON(error);
                        error = do_read_data_band(sc);
                        break;
                case STATE_MODIFY_DATA_BAND:
                        pr_debug("STATE_MODIFY_DATA_BAND\n");
                        error = do_modify_data_band(sc, error);
                        break;
                case STATE_WRITE_DATA_BAND:
                        pr_debug("STATE_WRITE_DATA_BAND\n");
                        error = do_write_data_band(sc, error);
                        break;
                case STATE_RMW_COMPLETE:
                        pr_debug("STATE_RMW_COMPLETE\n");
                        error = do_rmw_complete(sc, error);
                        break;
                case STATE_GC_COMPLETE:
                        pr_debug("STATE_GC_COMPLETE\n");
                        WARN_ON(error);
                        do_gc_complete(sc);
                        break;
                default:
                        WARN_ON(1);
                        break;
                }
        } while (!error && sc->state != STATE_NONE);

        if (sc->state != STATE_NONE && error != -EINPROGRESS)
                do_fail_gc(sc);
}

static void start_gc(struct sadc_c *sc, struct io *io)
{
        debug_bio(sc, io->bio, __func__);

        mutex_lock(&sc->c_lock);

        sc->pending_io = io;
        sc->state = STATE_START_GC;
        gc(sc, 0);
}

/*
 * Worker thread entry point.
 */
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

static bool alloc_structs(struct sadc_c *sc)
{
        int32_t i, size, pba;

        size = sizeof(int32_t) * sc->nr_data_pbas;
        sc->pba_map = vmalloc(size);
        if (!sc->pba_map)
                return false;

        memset(sc->pba_map, -1, size);
        sc->rmw_band = -1;

        size = sizeof(struct cache_band) * sc->nr_cache_bands;
        sc->cache_bands = vmalloc(size);
        if (!sc->cache_bands)
                return false;

        /* The cache region starts where the data region ends. */
        pba = sc->nr_data_pbas;
        size = BITS_TO_LONGS(sc->cache_assoc) * sizeof(long);
        for (i = 0; i < sc->nr_cache_bands; ++i) {
                unsigned long *bitmap = kmalloc(size, GFP_KERNEL);
                if (!bitmap)
                        return false;

                sc->cache_bands[i].map = bitmap;
                sc->cache_bands[i].begin_pba = pba;
                sc->cache_bands[i].nr = i;
                reset_cache_band(sc, &sc->cache_bands[i]);
                pba += sc->band_size_pbas;
        }
        return true;
}

static bool get_args(struct dm_target *ti, struct sadc_c *sc,
                     int argc, char **argv)
{
        unsigned long long tmp;
        char d;

        if (argc != 5) {
                ti->error = "dm-sadc: Invalid argument count.";
                return false;
        }

        if (sscanf(argv[1], "%llu%c", &tmp, &d) != 1 || tmp & 0xfff ||
            (tmp < 4 * 1024 || tmp > 2 * 1024 * 1024)) {
                ti->error = "dm-sadc: Invalid track size.";
                return false;
        }
        sc->track_size = tmp;

        if (sscanf(argv[2], "%llu%c", &tmp, &d) != 1 || tmp < 1 || tmp > 200) {
                ti->error = "dm-sadc: Invalid band size.";
                return false;
        }
        sc->band_size_tracks = tmp;

        if (sscanf(argv[3], "%llu%c", &tmp, &d) != 1 || tmp < 1 || tmp > 50) {
                ti->error = "dm-sadc: Invalid cache percent.";
                return false;
        }
        sc->cache_percent = tmp;

        if (sscanf(argv[4], "%llu%c", &tmp, &d) != 1 ||
            tmp < MIN_DISK_SIZE || tmp > MAX_DISK_SIZE) {
                ti->error = "dm-sadc: Invalid disk size.";
                return false;
        }
        sc->disk_size = tmp;

        return true;
}

static void calc_params(struct sadc_c *sc)
{
        sc->band_size      = sc->band_size_tracks * sc->track_size;
        sc->band_size_pbas = sc->band_size / PBA_SIZE;
        sc->nr_bands       = sc->disk_size / sc->band_size;
        sc->nr_cache_bands = sc->nr_bands * sc->cache_percent / 100;
        sc->cache_size     = sc->nr_cache_bands * sc->band_size;

        /* Make |nr_data_bands| a multiple of |nr_cache_bands| so that all cache
         * bands are equally loaded. */
        sc->nr_data_bands  = (sc->nr_bands / sc->nr_cache_bands - 1) *
                sc->nr_cache_bands;

        sc->cache_assoc    = sc->nr_data_bands / sc->nr_cache_bands;
        sc->usable_size    = sc->nr_data_bands * sc->band_size;
        sc->wasted_size    = sc->disk_size - sc->cache_size - sc->usable_size;
        sc->nr_valid_pbas  = (sc->usable_size + sc->cache_size) / PBA_SIZE;
        sc->nr_data_pbas   = sc->usable_size / PBA_SIZE;

        WARN_ON(sc->usable_size % PBA_SIZE);
}

static void print_params(struct sadc_c *sc)
{
        DMINFO("Disk size: %Lu bytes",        sc->disk_size);
        DMINFO("Band size: %Lu bytes",        sc->band_size);
        DMINFO("Band size: %d pbas",          sc->band_size_pbas);
        DMINFO("Total number of bands: %d",   sc->nr_bands);
        DMINFO("Number of cache bands: %d",   sc->nr_cache_bands);
        DMINFO("Cache size: %Lu bytes",       sc->cache_size);
        DMINFO("Number of data bands: %d",    sc->nr_data_bands);
        DMINFO("Usable disk size: %Lu bytes", sc->usable_size);
        DMINFO("Number of usable pbas: %d",   sc->nr_data_pbas);
        DMINFO("Wasted disk size: %Lu bytes", sc->wasted_size);
}

static void sadc_dtr(struct dm_target *ti);

static int sadc_ctr(struct dm_target *ti, unsigned int argc, char **argv)
{
        struct sadc_c *sc;
        int32_t ret;

        DMINFO("Constructing...");

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
        print_params(sc);

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

        DMINFO("Destructing...");

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

        for (i = 0; i < sc->nr_cache_bands; ++i)
                if (sc->cache_bands[i].map)
                        kfree(sc->cache_bands[i].map);

        if (sc->pba_map)
                vfree(sc->pba_map);
        if (sc->cache_bands)
                vfree(sc->cache_bands);
        kzfree(sc);
}

static int sadc_map(struct dm_target *ti, struct bio *bio)
{
        struct sadc_c *sc = ti->private;
        struct io *io;

        debug_bio(sc, bio, __func__);

        if (fast(sc, bio)) {
                remap(sc, bio);
                return DM_MAPIO_REMAPPED;
        }

        io = alloc_io(sc, bio);
        if (!io)
                return -EIO;

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
