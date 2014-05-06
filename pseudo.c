#include <assert.h>
#include <err.h>
#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define SECTOR_SIZE 4096

#define BITSPERWORD 32
#define SHIFT 5
#define MASK 0x1F

struct cache_band {
  int32_t start_si;      /* Index of the sector where the cache band starts. */
  int32_t current_si;    /* Index of the sector where the next write will go. */

  /* There are usually multiple data bands assigned to each cache band.
   * |start_dbi| holds the index of the starting data band assigned to this
   * cache band.  The index of the next data band assigned to this cache band is
   * |start_dbi + sc->num_cache_bands|.  This information is used for
   * |bitmap|. */
  int32_t start_dbi;

  /* Although multiple data bands are assigned to this cache band, not all of
   * them may have sectors on it when full.  |bitmap| keeps track of data bands
   * having sectors on this cache band.  It is used during garbage collection.
   *
   * Assume there are 6 data bands and 2 cache bands.  Then data bands 0, 2, 4
   * are assigned to cache band 0 and data bands 1, 3, 5 are assigned to cache
   * band 1 (using modulo arithmetic).  Consider cache band 1.  If it contains
   * sectors only for data band 5, then |bitmap| will be "001", if it contains
   * data for data bands 1 and 3, then |bitmap| will be "110".  That is, given a
   * data band index, it is first converted to a position using the formula
   * |position = (dbi - start_dbi) / sc->num_cache_bands_|, which is then
   * set. */
  int32_t *bitmap;
} cache_band;

struct shingle_c {
  char *disk;
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
  int32_t num_cache_bands;    /* Number of cache bands. */
  int32_t num_data_bands;     /* Number of data bands. */

  int32_t cache_associativity; /* Number of data bands associated with each
                                * cache band. */
} shingle_c;

/* Exported API. */
void write(struct shingle_c *sc, int32_t sector, char *buf);
void read(struct shingle_c *sc, int32_t sector, char *buf);

char* readable(double size) {
  static char buf[1024];
  int i = 0;
  const char* units[] = {"B", "kB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB"};
  while (size > 1024) {
    size /= 1024;
    i++;
  }
  sprintf(buf, "%.*f %s", i, size, units[i]);
  return buf;
}

/* Sets bit in position |i| in |bitmap|. */
void set(int32_t i, int32_t *bitmap) {
  bitmap[i >> SHIFT] |= (1 << (i & MASK));
}

/* Clears bit in position |i| in |bitmap|. */
void clear(int32_t i, int32_t *bitmap) {
  bitmap[i >> SHIFT] &= ~(1 << (i & MASK));
}

/* Tests bit in position |i| in |bitmap|. */
int test(int32_t i, int32_t *bitmap) {
  return bitmap[i >> SHIFT] & (1 << (i & MASK));
}

void read_from_disk(struct shingle_c *sc, int32_t sector, char *buf) {
  assert(sc && buf && sector >= 0 && sector < sc->num_valid_sectors);
  *buf = sc->disk[sector];
  printf("r %d %c\n", sector, *buf);
}

void write_to_disk(struct shingle_c *sc, int32_t sector, char *buf) {
  assert(sc && buf && sector >= 0 && sector < sc->num_valid_sectors);
  sc->disk[sector] = *buf;
  printf("w %d %c\n", sector, *buf);
}

/* Tests if cache band |cb| full. */
int full_cache_band(struct shingle_c *sc, struct cache_band *cb) {
  assert(sc && cb);
  return cb->current_si == cb->start_si + sc->band_size_in_sectors;
}

/* Resets the cache band |cb|. */
void reset_cache_band(struct shingle_c *sc, struct cache_band *cb) {
  assert(sc && cb);
  cb->current_si = cb->start_si;
  int32_t bitmap_size = (sc->cache_associativity / BITSPERWORD + 1) *
                        sizeof(int32_t);
  memset(cb->bitmap, 0, bitmap_size);
}

/* Initialize the context and construct all cache bands. */
void construct(struct shingle_c *sc) {
  sc->band_size_in_bytes = sc->band_size_in_tracks * sc->track_size_in_bytes;
  sc->band_size_in_sectors = sc->band_size_in_bytes / SECTOR_SIZE;
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
                           sc->cache_size_in_bytes) / SECTOR_SIZE;
  sc->num_usable_sectors = sc->usable_size_in_bytes / SECTOR_SIZE;
  assert(sc->usable_size_in_bytes % SECTOR_SIZE == 0);

  int32_t disk_size = sizeof(char) * sc->num_usable_sectors;
  sc->disk = malloc(disk_size);
  memset(sc->disk, 'f', disk_size);
  
  int32_t sector_map_size = sizeof(int32_t) * sc->num_usable_sectors;
  sc->sector_map = malloc(sector_map_size);
  memset(sc->sector_map, -1, sector_map_size);
  
  sc->cache_bands = malloc(sizeof(struct cache_band) * sc->num_cache_bands);
  sc->rmw_buffer = malloc(sc->band_size_in_bytes);

  int32_t i, start_si = sc->num_usable_sectors;
  for (i = 0; i < sc->num_cache_bands; ++i) {
    int size = (sc->cache_associativity / BITSPERWORD + 1) * sizeof(int32_t);
    sc->cache_bands[i].bitmap = malloc(size);
    sc->cache_bands[i].start_si = start_si;
    reset_cache_band(sc, &sc->cache_bands[i]);
    start_si += sc->band_size_in_sectors;
  }
}

void destruct(struct shingle_c *sc) {
  int i;
  for (i = 0; i < sc->num_cache_bands; ++i)
    free(sc->cache_bands[i].bitmap);
  free(sc->disk);
  free(sc->cache_bands);
  free(sc->sector_map);
  free(sc->rmw_buffer);
  free(sc);
}

void shingle_debug_print(struct shingle_c *sc)
{
  printf("Total disk size: [%s] %"PRId64" bytes\n",
         readable(sc->total_size_in_bytes),
         sc->total_size_in_bytes);
  printf("Band size: [%s] %"PRId64" bytes\n",
         readable(sc->band_size_in_bytes),
         sc->band_size_in_bytes);
  printf("Band size: %d sectors\n", sc->band_size_in_sectors);
  printf("Total number of bands: %d\n", sc->num_bands);
  printf("Number of cache bands: %d\n", sc->num_cache_bands);
  printf("Cache size: [%s] %"PRId64" bytes\n",
         readable(sc->cache_size_in_bytes),
         sc->cache_size_in_bytes);
  printf("Number of data bands: %d\n", sc->num_data_bands);
  printf("Usable disk size: [%s] %"PRId64" bytes\n",
         readable(sc->usable_size_in_bytes),
         sc->usable_size_in_bytes);
  printf("Wasted disk size: [%s] %"PRId64" bytes\n",
         readable(sc->wasted_size_in_bytes),
         sc->wasted_size_in_bytes);
}

void shingle_get_args(struct shingle_c *sc, int argc, char **argv)
{
  unsigned long long tmp;
  char dummy;
  
  if (argc != 5)
    errx(1, "Invalid argument count.\nUsage: %s total_size_in_bytes" \
         " track_size_in_bytes band_size_in_tracks cache_percent\n", argv[0]);
  
  if (sscanf(argv[1], "%llu%c", &tmp, &dummy) != 1)
    errx(1, "Invalid total size.");
  sc->total_size_in_bytes = tmp;
  
  if (sscanf(argv[2], "%llu%c", &tmp, &dummy) != 1 || tmp & 0xfff ||
      /* TEMPORARY, CHANGE BACK LOWER BOUND TO 512 FOR THE MODULE. */
      (tmp < 4 * 1024 || tmp > 2 * 1024 * 1024))
    errx(1, "Invalid track size.");
  sc->track_size_in_bytes = tmp;

  /* TEMPORARY, CHANGE BACK LOWER BOUND TO 20 FOR THE MODULE */
  if (sscanf(argv[3], "%llu%c", &tmp, &dummy) != 1 || tmp < 1 || tmp > 200)
    errx(1, "Invalid band size.");
  sc->band_size_in_tracks = tmp;

  /* TEMPORARY, CHANGE BACK UPPER BOUND TO 20 FOR THE MODULE */
  if (sscanf(argv[4], "%llu%c", &tmp, &dummy) != 1 || tmp < 1 || tmp > 50)
    errx(1, "Invalid cache percent.");
  sc->cache_percent = tmp;
}

void write_to_cache_band(struct shingle_c *sc, struct cache_band *cb,
                                int32_t sector, char *buf) {
  assert(sc && cb &&
         sector >= 0 && sector < sc->num_usable_sectors &&
         !full_cache_band(sc, cb));
  write_to_disk(sc, cb->current_si, buf);
  sc->sector_map[sector] = cb->current_si++;
  int32_t dbi = sector / sc->band_size_in_sectors;
  assert(dbi >= 0 && dbi < sc->num_data_bands);
  int32_t dbp = (dbi - cb->start_dbi) / sc->num_cache_bands;
  set(dbp, cb->bitmap);
}

void rmw_data_band(struct shingle_c *sc, int32_t data_band, char *buf) {
  int begin_sector = data_band * sc->band_size_in_sectors;
  int end_sector = begin_sector + sc->band_size_in_sectors;
  
  /* CHANGE BACK TO BUF += SECTOR_SIZE */
  /* This can be replaced by a single read in a DM target. */
  int next_sector = begin_sector;
  char *p = buf;
  for ( ; next_sector < end_sector; ++next_sector, ++p)
    read_from_disk(sc, next_sector, p);

  next_sector = begin_sector;
  p = buf;
  for ( ; next_sector < end_sector; ++next_sector, ++p) {
    if (~sc->sector_map[next_sector]) {
      read_from_disk(sc, sc->sector_map[next_sector], p);
      sc->sector_map[next_sector] = -1;
    }
  }

  /* This can be replaced by a single write in a DM target. */
  next_sector = begin_sector;
  p = buf;
  for ( ; next_sector < end_sector; ++next_sector, ++buf)
    write_to_disk(sc, next_sector, buf);
}

void gc(struct shingle_c *sc, struct cache_band *cb) {
  assert(sc && cb && full_cache_band(sc, cb));
  int i;
  for (i = 0; i < sc->num_cache_bands; ++i) {
    if (test(i, cb->bitmap)) {
      int32_t dbi = i * sc->num_cache_bands + cb->start_dbi;
      printf("GC: data band %d has data in the cache buffer\n", dbi);
      rmw_data_band(sc, dbi, sc->rmw_buffer);
    }
  }
  reset_cache_band(sc, cb);
}

struct cache_band *get_cache_band(struct shingle_c *sc, int32_t sector) {
  assert(sc && sector >= 0 && sector < sc->num_usable_sectors);
  int32_t cbi = (sector / sc->band_size_in_sectors) % sc->num_cache_bands;
  return &sc->cache_bands[cbi];
}

void write(struct shingle_c *sc, int32_t sector, char *buf) {
  struct cache_band *cb = get_cache_band(sc, sector);
  assert(cb);
  if (full_cache_band(sc, cb))
    gc(sc, cb);
  write_to_cache_band(sc, cb, sector, buf);
}

void read(struct shingle_c *sc, int32_t sector, char *buf) {
  int32_t xlated = sc->sector_map[sector];
  read_from_disk(sc, ~xlated ? xlated : sector, buf);
}

void cmdloop(struct shingle_c* sc, char *buf) {
  assert(sc && buf);
  
  char line[128];
  for (;;) {
    printf("> ");
    fflush(stdout);
    
    if (fgets(line, sizeof(line), stdin) == NULL)
      break;
    
    char cmd, byte; int sector;
    int n = sscanf(line, " %c%d %c", &cmd, &sector, &byte);
    if (cmd == 'w' && n == 3) {
      buf[0] = byte;
      write(sc, sector, buf);
    } else if (cmd == 'r' && n == 2) {
      read(sc, sector, buf);
    } else {
      printf("Invalid command.  Use: (w byte sector_index|r sector_index)\n");
      continue;
    }
  }
}

int main(int argc, char *argv[]) {
  struct shingle_c *sc = malloc(sizeof(*sc));
  shingle_get_args(sc, argc, argv);
  construct(sc);
  shingle_debug_print(sc);

  char buf[SECTOR_SIZE];
  cmdloop(sc, buf);
  
  destruct(sc);
}
