#include <linux/bio.h>
#include <linux/bitops.h>
#include <linux/device-mapper.h>
#include <linux/init.h>
#include <linux/mempool.h>
#include <linux/module.h>

#define DM_MSG_PREFIX "shingle"

/*
 * Sizes of all names ending with _size or _SIZE is in bytes.
 *
 * From Seagate:
 *
 * Consider the following scale of sizes:
 *  - tracks in the range of 0.5 to 2 MiB
 *  - bands in the range of 20 to 200 tracks
 *  - drives in the range of 1 to 10 TB
 *  - a 1% cache space could be a single, very large band or many smaller bands
 *  - the 99% data space as tens of thousands of bands
 */

#define TOTAL_SIZE (76LL << 10)

#define LBA_SIZE 512
#define PBA_SIZE 4096
#define SIZE_RATIO (PBA_SIZE / LBA_SIZE)

#define LBA_TO_PBA(lba) ((int32_t) (lba / SIZE_RATIO))
#define PBA_TO_LBA(pba) (((sector_t) pba) * SIZE_RATIO)

#define LBAS_IN_PAGE (PAGE_SIZE / LBA_SIZE)

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
 * TODO: We can reduce memory usage by reducing this number, explore how
 * realistic 32 is, since in practice the number seems to be much smaller.
 */
#define MAX_SEGMENTS_IN_BIO 32

/*
 * A bio may be split into chunks if the underlying sectors are scattered
 * between data band and cache bands.  This struct represents a chunk resulting
 * from splitting a bio.
 */
struct chunk {
        int idx;
        int vcnt;
        sector_t sector;
        bool requires_gc;
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
        struct shingle_c *sc;
        struct bio *bio;
        struct work_struct work;
        int error;
        struct chunk chunks[MAX_SEGMENTS_IN_BIO];
        int num_chunks;
        atomic_t pending;
};

#define MIN_IOS 16
#define MIN_POOL_PAGES 32

static struct kmem_cache *_io_pool;

static struct io *alloc_io(struct shingle_c *sc, struct bio *bio)
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

        /* TODO: Change bounds back after testing. */
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

        /* The cache region starts where the data region ends. */
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
                                    WQ_NON_REENTRANT | WQ_MEM_RECLAIM, 1);
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

        /* TODO: Reconsider proper values for these. */
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

/* static int full_cache_band(struct shingle_c *sc, struct cache_band *cb) */
/* { */
/*         return cb->current_pba == cb->begin_pba + sc->band_size_pbas; */
/* } */

/* static void gc_cache_band(struct shingle_c *sc, struct cache_band *cb) */
/* { */
/*         BUG(); */
/*         /\* int i; *\/ */
/*         /\* for_each_set_bit(i, cb->data_band_bitmap, sc->cache_assoc) { *\/ */
/*         /\*         int32_t db = i * sc->num_cache_bands + cb->begin_data_band; *\/ */
/*         /\*         DMERR("GC: data band %d has data in the cache buffer.", db); *\/ */
/*         /\*         read_modify_write_data_band(sc, db); *\/ */
/*         /\* } *\/ */
/*         /\* reset_cache_band(sc, cb); *\/ */
/* } */

/*
 * For now we only handle READ and WRITE requests that are 4KB and aligned and
 * we assume there are no more than MAX_SEGMENTS_IN_BIO segments in a bio.
 */
static int bad_bio(struct shingle_c *sc, struct bio *bio)
{
        return (bio->bi_sector & 0x7) || (bio->bi_size & 0xfff) ||
                        (LBA_TO_PBA(bio->bi_sector) >= sc->num_usable_pbas) ||
                        (bio->bi_vcnt > MAX_SEGMENTS_IN_BIO);
}

/*
 * Translates |lba| to its location in the cache region.  Returns |lba| if it
 * was not mapped.
 */
static sector_t lookup_lba(struct shingle_c *sc, sector_t lba)
{
        int32_t mapped_pba, pba = LBA_TO_PBA(lba);

        /* |pba| should be in the data region. */
        BUG_ON(pba < 0 || pba >= sc->num_usable_pbas);

        mapped_pba = sc->pba_map[pba];

        /* |mapped_pba| should be in the cache region. */
        BUG_ON(~mapped_pba && (mapped_pba < sc->num_usable_pbas ||
                               mapped_pba >= sc->num_valid_pbas));

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
        int32_t data_band, pos;
        int32_t mapped_pba, pba = LBA_TO_PBA(lba);

        BUG_ON(cb->current_pba == cb->begin_pba + sc->band_size_pbas);
        mapped_pba = cb->current_pba++;

        /* |pba| should be in the data region. */
        BUG_ON(pba < 0 || pba >= sc->num_usable_pbas);
        sc->pba_map[pba] = mapped_pba;

        data_band = pba / sc->band_size_pbas;
        BUG_ON(data_band < 0 || data_band >= sc->num_data_bands);

        pos = (data_band - cb->begin_data_band) / sc->num_cache_bands;
        BUG_ON(pos < 0 || pos >= sc->cache_assoc);
        set_bit(pos, cb->data_band_bitmap);

        return PBA_TO_LBA(mapped_pba);
}

static void endio(struct bio *clone, int error)
{
        struct io *io = clone->bi_private;

        if (unlikely(!bio_flagged(clone, BIO_UPTODATE) && !error))
                io->error = -EIO;

        /* TODO: Remove once everything works. */
        DMERR("%s %c %d %u",
              (io->error ? "!!" : "OK"),
              (bio_data_dir(clone) == READ ? 'R' : 'W'),
              LBA_TO_PBA(clone->bi_sector),
              clone->bi_vcnt);

        bio_put(clone);
        if (atomic_dec_and_test(&io->pending))
                release_io(io);
}

static void clone_init(struct io *io, struct bio *clone, struct chunk *sp)
{
        struct shingle_c *sc = io->sc;

        atomic_inc(&io->pending);
        clone->bi_private = io;
        clone->bi_end_io = endio;
        clone->bi_bdev = sc->dev->bdev;

        clone->bi_idx = sp->idx;
        clone->bi_vcnt = sp->vcnt;
        clone->bi_sector = sp->sector;
}

static struct cache_band *get_cache_band(struct shingle_c *sc, int data_band)
{
        int i = data_band % sc->num_cache_bands;

        BUG_ON(data_band >= sc->num_data_bands);

        return &sc->cache_bands[i];
}

/*
 * Helper function for |find_write_chunks|.
 */
static void fill_chunk(struct shingle_c *sc, struct io *io, int si,
                       int data_band, int vcnt)
{
        struct bio *bio = io->bio;
        struct chunk *sp = &io->chunks[si];
        struct cache_band *cb = get_cache_band(sc, data_band);
        int32_t pbas_left = sc->band_size_pbas -
                            (cb->current_pba - cb->begin_pba);

        BUG_ON(si < 0);
        sp->idx = si ? io->chunks[si-1].vcnt : 0;
        sp->vcnt = vcnt;
        sp->requires_gc = vcnt > pbas_left;

        if (sp->requires_gc)
                sp->sector = PBA_TO_LBA(cb->begin_pba);
        else
                sp->sector = map_lba(sc, cb,
                                     bio->bi_sector + sp->idx * LBAS_IN_PAGE);
}

/*
 * When we receive a write at this layer, no matter how large the original write
 * size was, the kernel splits it into bios of at most 512 KB.  I haven't
 * verified it yet, but I remember reading it in a book.
 *
 * The point is, given the numbers at the top of this file regarding band and
 * track sizes, minimum size for a band is 10 MB, which means a write can cross
 * at most two bands, therefore a write bio may be split in at most one point.
 *
 * If |io->bio| does not cross bands, then we set the required mapping for it in
 * |io->chunks[0]| and return 1.  Otherwise, we also set |io->chunks[1]| and
 * return 2.
 */
static int find_write_chunks(struct shingle_c *sc, struct io *io)
{
        struct bio *bio = io->bio;
        int32_t pba, data_band, size_pbas = bio->bi_size / PBA_SIZE;
        int32_t pbas_left_in_band, chunk_size_pbas;

        pba = LBA_TO_PBA(bio->bi_sector);
        BUG_ON(pba < 0 || pba >= sc->num_usable_pbas);

        /* The band where staring pba is located. */
        data_band = pba / sc->band_size_pbas;
        BUG_ON(data_band >= sc->num_data_bands);

        pbas_left_in_band = sc->band_size_pbas - (pba % sc->band_size_pbas);
        chunk_size_pbas = min(pbas_left_in_band, size_pbas);

        fill_chunk(sc, io, 0, data_band, chunk_size_pbas);

        chunk_size_pbas = size_pbas - chunk_size_pbas;
        if (!chunk_size_pbas)
                return io->num_chunks = 1;

        fill_chunk(sc, io, 1, ++data_band, chunk_size_pbas);
        return io->num_chunks = 2;
}

/*
 * Although a bio may be contiguous in LBA-space, it may not be so in PBA-space
 * since some of the LBAs may be located in the cache band.  This function
 * splits a bio into contiguous chunks fills up |io->chunks| array and returns
 * the number of chunks.  It returns 1 for a bio that is contiguous in
 * PBA-space.
 *
 * The splitting logic differs for a read bio and a write bio.   For a read, the
 * split points are those where contiguity is broken due to sectors mapped to
 * cache band.  See |find_write_chunks| for the write splitting logic.
 */
static int find_read_chunks(struct shingle_c *sc, struct io *io)
{
        struct bio *bio = io->bio;
        struct chunk *chunks = io->chunks;
        sector_t prev_lba;
        int i, j;

        BUG_ON(bio_data_dir(bio) != READ);

        chunks[0].idx = 0;
        chunks[0].sector = lookup_lba(sc, bio->bi_sector);
        prev_lba = bio->bi_sector;

        i = 0;
        for (j = 1; j < bio->bi_vcnt; ++j) {
                int32_t curr_lba = prev_lba + LBAS_IN_PAGE;
                int32_t curr_mapped_lba = lookup_lba(sc, curr_lba);
                int32_t prev_mapped_lba = lookup_lba(sc, prev_lba);

                if (prev_mapped_lba + LBAS_IN_PAGE != curr_mapped_lba) {
                        chunks[i].vcnt = j - chunks[i].idx;
                        ++i;
                        chunks[i].idx = j;
                        chunks[i].sector = curr_mapped_lba;
                }
                prev_lba += LBAS_IN_PAGE;
        }
        chunks[i].vcnt = bio->bi_vcnt - chunks[i].idx;
        return io->num_chunks = i + 1;
}

static bool requires_gc(struct io *io);

static void do_non_contig_io(struct io *io)
{
        struct shingle_c *sc = io->sc;
        struct bio *bio = io->bio;
        struct bio *bios[MAX_SEGMENTS_IN_BIO];
        int i;

        BUG_ON(io->num_chunks <= 1 || requires_gc(io));

        for (i = 0; i < io->num_chunks; ++i) {
                bios[i] = bio_clone_bioset(bio, GFP_NOIO, sc->bs);
                if (!bios[i])
                        goto bad;
        }
        for (i = 0; i < io->num_chunks; ++i) {
                clone_init(io, bios[i], &io->chunks[i]);
                generic_make_request(bios[i]);
        }
        return;

bad:
        while (i--)
                bio_put(bios[i]);
        io->error = -ENOMEM;
        release_io(io);
}

static void start_gc(struct io *io)
{
        BUG();
}

/*
 * This is the entry point of the worker threads.
 */
static void shingled(struct work_struct *work)
{
        struct io *io = container_of(work, struct io, work);

        if (requires_gc(io))
                start_gc(io);
        else
                do_non_contig_io(io);
}

static void queue_io(struct io *io)
{
        struct shingle_c *sc = io->sc;

        INIT_WORK(&io->work, shingled);
        queue_work(sc->queue, &io->work);
}

static bool requires_gc(struct io *io)
{
        if (bio_data_dir(io->bio) == READ)
                return false;

        BUG_ON(io->num_chunks > 2);
        return io->chunks[0].requires_gc || io->chunks[1].requires_gc;
}

/*
 * I/O is contiguous, if for reads it does not need to be split, and for writes,
 * it does not need to be split as well as no gc required.
 */
static bool contig_io(struct shingle_c *sc, struct io *io)
{
        struct bio *bio = io->bio;

        if (bio_data_dir(bio) == READ)
                return find_read_chunks(sc, io) == 1;

        return find_write_chunks(sc, io) == 1 && !requires_gc(io);
}

static void remap_io(struct shingle_c *sc, struct io *io)
{
        struct bio *bio = io->bio;

        bio->bi_sector = io->chunks[0].sector;
        bio->bi_bdev = sc->dev->bdev;

        /* TODO: Remove once everything works. */
        DMERR("OK %c %d %u", (bio_data_dir(bio) == READ ? 'R' : 'W'),
              LBA_TO_PBA(bio->bi_sector), bio->bi_vcnt);
}

static int shingle_map(struct dm_target *ti, struct bio *bio)
{
        struct shingle_c *sc = ti->private;
        struct io *io;

        BUG_ON(bad_bio(sc, bio));

        io = alloc_io(sc, bio);
        if (!io)
                return -EIO;

        if (contig_io(sc, io)) {
                remap_io(sc, io);
                mempool_free(io, sc->io_pool);
                return DM_MAPIO_REMAPPED;
        }
        queue_io(io);
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
