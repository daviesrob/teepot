/*
 * Copyright (c) 2014 Genome Research Ltd.
 * Author: Rob Davies <rmd+github@sanger.ac.uk>
 *
 * This file is part of teepot.
 *
 * Teepot is free software: you can redistribute it and/or modify it under
 * the terms of the GNU General Public License as published by the Free Software
 * Foundation; either version 3 of the License, or (at your option) any later
 * version.
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU General Public License for more
 * details.
 * You should have received a copy of the GNU General Public License along with
 * this program. If not, see <http://www.gnu.org/licenses>
 */

#ifdef HAVE_CONFIG_H
#  include <teepot_config.h>
#endif
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <poll.h>
#include <signal.h>
#include <assert.h>
#include <unistd.h>
#include <math.h>
#if !defined(HAVE_CLOCK_GETTIME) && defined(HAVE_GETTIMEOFDAY)
# include <sys/time.h>
#else
# include <time.h>
#endif
#ifdef HAVE_PTHREADS
# include <pthread.h>
#endif

/* Options */
typedef struct {
  size_t      max;      /* Maximum amount of data to keep in memory */
  off_t       file_max; /* Maximum size of each temp file */
  double      wait_time; /* Time to wait before spilling data */
  const char *tmp_dir;  /* Where to write spilled data */
  const char *in_name;  /* Name of input file */
  size_t      tmp_dir_len; /* strlen(tmp_dir) */
  int         use_threads; /* Use threads for outputs */
} Opts;

#ifndef DEFAULT_MAX
#  define DEFAULT_MAX      (1024*1024*100)
#endif
#ifndef DEFAULT_FILE_MAX
#  define DEFAULT_FILE_MAX (DEFAULT_MAX * 10)
#endif
#ifndef DEFAULT_TMP_DIR
#  define DEFAULT_TMP_DIR  "/tmp"
#endif
#ifndef DEFAULT_WAIT_TIME
#  define DEFAULT_WAIT_TIME 1.0
#endif
#ifndef MAX_WAIT_TIME
/* Must be < 2^31 / 1000 to prevent poll timeout from overflowing.
   Use a nice round number below this, is about 23 days. */
#  define MAX_WAIT_TIME 2000000.0
#endif

/* File to spill data to */
typedef struct SpillFile {
  off_t   offset;     /* Current temp file offset */
  size_t  ref_count;  /* Number of Chunks that reference this file */
  int     fd;         /* File descriptor */
  int     is_tmp;     /* 0: fd is the input file; >0 fd is a temporary file */
} SpillFile;

/* A chunk of input */
typedef enum {    /* Spillage state */
  in_core,        /* Normal state: data present in memory */
  spilling,       /* Data is currently being written to a spill file */
  spilled,        /* Data has been spilled */
  rereading       /* Data is being read back from the spill file */
} ChunkState;

typedef struct Chunk {
  unsigned char *data;          /* The data */
  size_t         len;           /* Length of data */
  unsigned int   ref_count;     /* Number of outputs that refer to this */
  unsigned int   nwriters;      /* Number of outputs currently writing this */
  off_t          spill_offset;  /* Offset into spill file */
  SpillFile     *spill;         /* Spill file */
  struct Chunk  *next;          /* Next Chunk in the list */
  ChunkState     state;         /* If spilling or rereading */
} Chunk;

/* Chunk size to use */
#define CHUNK_SZ (1024*1024)

/* The input */
typedef struct {
  off_t    pos;      /* Total bytes read so far */
  int      fd;       /* File descriptor */
  int      reg;      /* Input is a regular file */
} Input;

/* An output */

typedef struct Output {
  char  *name;         /* Output file name */
  int    fd;           /* Output file descriptor */
  int    is_reg;       /* Output is regular */
  Chunk *curr_chunk;   /* Current Chunk being written */
  size_t offset;       /* Offset into current Chunk */
  off_t  written;      /* Amount written so far */
  double write_time;   /* When the last write happened */
  struct Output *prev; /* Previous output (non-regular only) */
  struct Output *next; /* Next  output (non-regular only) */
#ifdef HAVE_PTHREADS
  pthread_t thread;    /* Thread ID for this output */
  int       res;       /* Thread return code, negative = error, positive = ok */
#endif
} Output;

static Output any_output;
#define ANY_OUTPUT &any_output

typedef struct ChunkList {
  Chunk *head;    /* List head */
  Chunk *spilled; /* Last chunk to be spilled to a file */
  Chunk *tail;    /* List tail */
  Output *closed;  /* Closed outputs (threaded mode only) */
  int    read_eof; /* Reading finished (threaded mode only) */
  int    waiting_outputs; /* No. of outputs waiting on conditionals */
  int    waiting_input;   /* Input waiting on a conditional */
#ifdef HAVE_PTHREADS
  pthread_mutex_t lock;         /* Global lock for everything */
  pthread_cond_t  new_data;     /* Signalled when new data arrives */
  pthread_cond_t  data_written; /* Signalled when data is written out */
  pthread_cond_t  state_change; /* Signalled when any chunk->state changes */
#endif
} ChunkList;

/* Spillage control */
typedef struct {
  SpillFile *spill;          /* The current file to spill to */
  Output    *pipe_list_head; /* Linked list of non-regular outputs */
  Output    *pipe_list_tail; /* Linked list of non-regular outputs */
  Output    *blocking_output; /* Output being waited on if deferring spillage */
  off_t      max_pipe;       /* Most data written to a pipe */
  off_t      max_all;        /* Most data written to anything */
  off_t      total_spilled;  /* Total bytes spilled (for accounting) */
  off_t      curr_spilled;   /* Current amount spilled */
  off_t      max_spilled;    /* Maximum amount spilled */
  size_t     alloced;        /* Amount of space currently allocated */
  size_t     max_alloced;    /* Maximum space allocated */
  int        open_spill_files; /* Count of open spill files */
  int        max_spill_files;  /* Max number of spill files opened */
  int        spill_file_count; /* Spill file count */
  int        npipes;           /* Number of pipes still open */
} SpillControl;

#ifdef HAVE_PTHREADS
typedef struct {
  Opts         *options;
  Output       *output;
  SpillControl *spillage;
  ChunkList    *chunks;
  int           index;
} ThreadParams;

#define LOCK_MUTEX(M) do {						\
    if (pthread_mutex_lock(M) != 0) abort();				\
  } while (0)

#define UNLOCK_MUTEX(M) do {			\
    if (pthread_mutex_unlock(M)) abort();	\
  } while (0)

#define DESTROY_MUTEX(M) do {						\
    int dm_res = pthread_mutex_destroy(M);				\
    if (dm_res != 0) {							\
      fprintf(stderr, "Failed to destroy mutex: %s\n", strerror(dm_res)); \
      abort();								\
    }									\
  } while (0)

#define COND_BROADCAST(C) do {						\
    int cb_res = pthread_cond_broadcast(C);				\
    if (cb_res != 0) {							\
      fprintf(stderr, "pthread_cond_broadcast : %s\n", strerror(cb_res)); \
      abort();								\
    }									\
  } while (0)

#define COND_SIGNAL(C) do {						\
    int cb_res = pthread_cond_signal(C);				\
    if (cb_res != 0) {							\
      fprintf(stderr, "pthread_cond_signal : %s\n", strerror(cb_res));	\
      abort();								\
    }									\
  } while (0)

#else

#define LOCK_MUTEX(M) do { } while (0)
#define UNLOCK_MUTEX(M) do { } while (0)
#define DESTROY_MUTEX(M) do { } while (0)
#define COND_BROADCAST(C) do { } while (0)
#define COND_SIGNAL(C) do { } while (0)

#endif

static unsigned int verbosity = 0; /* How verbose to be. */

/*
 * Get the time.
 *
 * Returns the current time, as a double for easy comparison.
 *         -1 on failure
 */

double get_time() {
#ifdef HAVE_CLOCK_GETTIME

  struct timespec tp = { 0, 0 };

  if (0 != clock_gettime(CLOCK_REALTIME, &tp)) {
    perror("clock_gettime");
    return -1.0;
  }

  return (double) tp.tv_sec + (double) tp.tv_nsec * 1.0E-9;

#else /* HAVE_CLOCK_GETTIME */
# ifdef HAVE_GETTIMEOFDAY

  struct timeval tv = { 0, 0 };
  
  if (0 != gettimeofday(&tv, NULL)) {
    perror("gettimeofday");
    return -1.0;
  }

  return (double) tv.tv_sec + (double) tv.tv_usec * 1.0E-6;

# else /* HAVE_GETTIMEOFDAY */

  time_t t = time(NULL);

  if (t == (time_t) -1) {
    perror("time");
    return -1.0;
  }

  return (double) t;

# endif /* HAVE_GETTIMEOFDAY */
#endif /* HAVE_CLOCK_GETTIME */
}

static inline void calc_timeout(struct timespec *timeout,
				double now, double wait_time) {
  double t, f;
  t = now + wait_time;
  f = floor(t);
  timeout->tv_sec  = (time_t) f;
  timeout->tv_nsec = (t - f) * 1e9;
}

/*
 * Convert a number into a more human-readable form using a suitable SI prefix.
 *
 * bytes is the number to convert
 *
 * Returns a pointer to a buffer containing the output string.
 * NB: The buffer is statically allocated.
 */

static char * human_size(double bytes) {
  static char buffer[64];

  int multiplier;

  for (multiplier = 0; bytes >= 1000 && multiplier < 8; multiplier++) {
    bytes /= 1000;
  }

  if (multiplier > 0) {
    snprintf(buffer, sizeof(buffer), "%.1f %c",
	     bytes, " kMGTPEZY"[multiplier]);
  } else {
    snprintf(buffer, sizeof(buffer), "%.0f", bytes);
  }

  return buffer;
}

/*
 * Check if a file descriptor points to a regular file, i.e. not poll-able
 *
 * name is the filename (for error reporting)
 * fd   is the file descriptor
 *
 * Returns 1 if regular
 *         0 if not
 *        -1 on error
 */

static int file_is_regular(const char *name, int fd) {
  struct stat sbuf;

  if (0 != fstat(fd, &sbuf)) {
    fprintf(stderr, "Couldn't stat %s : %s\n",
	    name, strerror(errno));
    return -1;
  }

  return S_ISREG(sbuf.st_mode) ? 1 : 0;
}

/*
 * Set a file descriptor to non-blocking mode
 *
 * *name is the name of the file (for error reporting)
 * fd    is the descriptor
 *
 * Returns  0 on success
 *         -1 on failure
 */

static int setnonblock(const char *name, int fd) {
  int val;

  if ((val = fcntl(fd, F_GETFL)) == -1) {
    fprintf(stderr, "Couldn't get file descriptor flags for %s : %s",
	    name, strerror(errno));
    return -1;
  }
  
  if (fcntl(fd, F_SETFL, val | O_NONBLOCK)) {
    fprintf(stderr, "Couldn't set %s to non-blocking mode : %s",
	    name, strerror(errno));
    return -1;
  }
  return 0;
}

/*
 * Set a file descriptor to blocking mode
 *
 * *name is the name of the file (for error reporting)
 * fd    is the descriptor
 *
 * Returns  0 on success
 *         -1 on failure
 */

static int unsetnonblock(const char *name, int fd) {
  int val;

  if ((val = fcntl(fd, F_GETFL)) == -1) {
    fprintf(stderr, "Couldn't get file descriptor flags for %s : %s",
	    name, strerror(errno));
    return -1;
  }
  
  if (fcntl(fd, F_SETFL, val & ~O_NONBLOCK)) {
    fprintf(stderr, "Couldn't set %s to blocking mode : %s",
	    name, strerror(errno));
    return -1;
  }
  return 0;
}

/*
 * Push an output onto the pipes linked list, for monitoring the slowest output
 *
 * output   is the Output to add.
 * spillage is the SpillControl information
 */

static void pipe_list_push(Output *output, SpillControl *spillage) {
  if (NULL != spillage->pipe_list_tail) {
    spillage->pipe_list_tail->next = output;
  }
  output->prev = spillage->pipe_list_tail;
  output->next = NULL;
  spillage->pipe_list_tail = output;
  if (NULL == spillage->pipe_list_head) spillage->pipe_list_head = output;
}

/*
 * Remove an output from the pipes linked list
 *
 * output   is the Output to remove
 * spillage is the SpillControl information
 */

static void pipe_list_remove(Output *output, SpillControl *spillage) {
  if (NULL == output->prev) {
    assert(spillage->pipe_list_head == output);
    spillage->pipe_list_head = output->next;
  } else {
    assert(output->prev->next == output);
    output->prev->next = output->next;
  }
  if (NULL == output->next) {
    assert(spillage->pipe_list_tail == output);
    spillage->pipe_list_tail = output->prev;
  } else {
    assert(output->next->prev == output);
    output->next->prev = output->prev;
  }
}

/*
 * Insert an output into the pipes linked list.
 *
 * output   is the Output to insert
 * after    is the item that the inserted one should follow.  If NULL
 *          the inserted item is put at the head of the list.
 * spillage is the SpillControl information
 */

static void pipe_list_insert(Output *output, Output *after,
			     SpillControl *spillage) {
  if (NULL == after) {
    output->prev = NULL;
    output->next = spillage->pipe_list_head;
    if (NULL != output->next) {
      output->next->prev = output;
    } else {
      spillage->pipe_list_tail = output;
    }
    spillage->pipe_list_head = output;
  } else {
    output->prev = after;
    output->next = after->next;
    if (NULL != output->next) {
      output->next->prev = output;
    } else {
      spillage->pipe_list_tail = output;
    }
    after->next = output;
  }
}

/*
 * Open the input file and set up the Input struct
 *
 * *options  is the Opt struct with the input file name
 * *in       is the Input struct to fill in
 * *spillage is the SpillControl information.
 * Returns  0 on success
 *         -1 on failure
 */
int open_input(Opts *options, Input *in, SpillControl *spillage) {
  if (NULL == options->in_name || 0 == strcmp(options->in_name, "-")) {
    /* Read from stdin */
    options->in_name = "stdin";
    in->fd = fileno(stdin);
  } else {
    /* Read from named file */
    in->fd = open(options->in_name, O_RDONLY);
    if (in->fd < 0) {
      fprintf(stderr, "Couldn't open %s : %s\n",
	      options->in_name, strerror(errno));
      return -1;
    }
  }

  /* Check if input is a regular file */
  in->reg = file_is_regular(options->in_name, in->fd);
  if (in->reg < 0) return -1;
  
  if (!in->reg) {
    if (options->use_threads) {
      /* Set input pipes to blocking mode (multi-thread mode only) */
      if (0 != unsetnonblock(options->in_name, in->fd)) return -1;
    } else {
      /* Set input pipes to non-blocking mode (single-thread mode only) */
      if (0 != setnonblock(options->in_name, in->fd)) return -1;
    }
    /* Can't use input for spilling */
    spillage->spill = NULL;
  } else {
    /* Use input file for spillage */
    spillage->spill = malloc(sizeof(SpillFile));
    if (NULL == spillage->spill) {
      perror("open_input");
      return -1;
    }
    spillage->spill->offset    = 0;
    spillage->spill->ref_count = 1;
    spillage->spill->fd        = in->fd;
    spillage->spill->is_tmp    = 0;
  }

  in->pos = 0;

  if (verbosity > 1) {
    fprintf(stderr, "%.3f Opened input (%s) on fd %d\n",
	    get_time(), options->in_name, in->fd);
  }

  return 0;
}

/*
 * Open the output files, filling the Output struct for each one.
 * Also put the index of each file in *outputs in *regular of *pipes as
 * appropriate.
 * 
 * n         is the number of files.
 * **names   is an array of n file names.  '-' means stdout.
 * *outputs  is an array of n Output structs, to be filled in
 * *regular  is an array of indexes into *outputs of the regular files
 * *pipes    is an array of indexes into *outputs of poll-able files
 * *nregular is the number of entries put into *regular
 * *npipes   is the number of entries put into *pipes
 *
 * Returns  0 on success
 *         -1 on failure
 */

static int open_outputs(int n, char **names, Output *outputs,
			int *regular, int *pipes, int *nregular, int *npipes) {
  int i;

  *nregular = *npipes = 0;

  for (i = 0; i < n; i++) {
    if (0 == strcmp(names[i], "-")) {  /* Send to stdout */
      outputs[i].fd = fileno(stdout);
      outputs[i].name = "stdout";
    } else {                           /* Open the given file */
      outputs[i].fd = open(names[i], O_WRONLY|O_CREAT|O_TRUNC, 0666);
      if (outputs[i].fd < 0) {
	fprintf(stderr, "Couldn't open %s for writing : %s\n",
		names[i], strerror(errno));
	return -1;
      }
      outputs[i].name = names[i];
    }

    /* Check if the file is regular or not */
    outputs[i].is_reg = file_is_regular(names[i], outputs[i].fd);
    if (outputs[i].is_reg < 0) return -1;

    /* Add it to the regular[] or pipes[] array as appropriate */
    if (outputs[i].is_reg) {
      regular[(*nregular)++] = i;
    } else {
      pipes[(*npipes)++] = i;
      if (0 != setnonblock(names[i], outputs[i].fd)) return -1;
    }

    /* Initialize the pointer to the data to output */
    outputs[i].curr_chunk = NULL;
    outputs[i].offset = 0;

    if (verbosity > 1) {
      fprintf(stderr, "%.3f Opened output #%d (%s) on fd %d.\n",
	      get_time(), i, outputs[i].name, outputs[i].fd);
    }
  }
  return 0;
}

/*
 * Open a new temporary file.
 *
 * *options is the Opts struct with the name of the temporary directory.
 * *current is the last SpillFile, used if we run out of file descriptors
 *
 * Returns a pointer to the new SpillFile struct on success
 *         NULL on failure
 */

static SpillFile * make_tmp_file(Opts *options, SpillFile *current) {
  SpillFile *spill = malloc(sizeof(SpillFile));
  char    *name = malloc(options->tmp_dir_len + 15);
  static int warned = 0;

  if (NULL == spill || NULL == name) {
    perror("make_tmp_file");
    return NULL;
  }

  /* Open a new temporary file */
  snprintf(name, options->tmp_dir_len + 14, "%s/tpXXXXXX", options->tmp_dir);
  spill->fd = mkstemp(name);
  if (spill->fd < 0) {
    free(spill);
    free(name);
    if ((EMFILE == errno || ENFILE == errno) && NULL != current) {
      /* Out of file descriptors, just keep going with the current one */
      if (!warned) {
	perror("Warning: Couldn't open new temporary file");
	warned = 1;
      }
      return current;
    }
    perror("Opening temporary file");
    return NULL;
  }

  /* Unlink the temp file so it will be deleted when closed */
  if (0 != unlink(name)) {
    fprintf(stderr, "Couldn't unlink %s : %s\n", name, strerror(errno));
    return NULL;
  }
  free(name);

  spill->offset = 0;
  spill->ref_count = 0;
  spill->is_tmp = 1;
  return spill;
}

/*
 * Release a temporary file
 * Decerement the reference count, and if it hits zero close the file.
 *
 * *spill    is the SpillFile to release
 * *spillage is the SpillControl information, for the current spill file
 *
 * Returns  0 on success
 *         -1 on failure
 */

int release_tmp(SpillFile *spill, SpillControl *spillage,
		ChunkList *chunks) {
  LOCK_MUTEX(&chunks->lock);
  assert(spill->ref_count > 0);
  if (!spill->is_tmp || --spill->ref_count > 0) {
    UNLOCK_MUTEX(&chunks->lock);
    return 0;  /* Still in use */
  }

  if (spillage->spill == spill) spillage->spill = NULL;
  spillage->curr_spilled -= spill->offset;
  spillage->open_spill_files--;
  UNLOCK_MUTEX(&chunks->lock);

  if (0 != close(spill->fd)) {
    perror("Closing temporary file");
    return -1;
  }

  if (verbosity > 1) {
    fprintf(stderr,
	    "%.3f Released spill file #%d; %d spill files now open.\n",
	    get_time(), spill->is_tmp, spillage->open_spill_files);
  }

  free(spill);

  return 0;
}

/*
 * Write data to a file descriptor.  Continues until everything has been
 * written.
 *
 * fd is the file descriptor
 * data is the data to write
 * len is the number of bytes to write.
 *
 * Returns  0 on success
 *         -1 on failure
 */

static int write_all(int fd, unsigned char *data, size_t len) {
  size_t written = 0;
  ssize_t bytes;

  while (written < len) {
    do {
      bytes = write(fd, data + written, len - written);
    } while (bytes < 0 && EINTR == errno);
    if (bytes < 0) return -1;
    written += bytes;
  }

  return 0;
}

/*
 * Read data from a file descriptor.  Continues until everything asked for
 * has been read, or it hits end of file.
 *
 * fd     is the file descriptor
 * data   is the buffer to put the data in
 * len    is the number of bytes to read
 * offset is the offset into fd to start reading from
 *
 * Returns  0 on success
 *         -1 on failure
 */


static int pread_all(int fd, unsigned char *data, size_t len, off_t offset) {
  size_t got = 0;
  ssize_t bytes = 0;
  
  while (got < len) {
    do {
      bytes = pread(fd, data + got, len - got, offset + got);
    } while (bytes < 0 && EINTR == errno);
    if (bytes < 0) {
      perror("Reading temporary file");
      abort();
      return -1;
    }
    if (bytes == 0) {
      fprintf(stderr, "Unexpected end of file reading temporary file\n");
      return -1;
    }
    got += bytes;
  }

  return 0;
}

/* Spill data to a temporary file (or just forget it if the input is a
 * regular file).
 *
 * *options is the Opts struct with the temporary directory
 * *chunks is the list of chunks to search for a candidate to spill
 * *spillage is the SpillControl information
 *
 * Returns  0 on success
 *         -1 on failure
 */

static int spill_data(Opts *options, ChunkList *chunks,
		      SpillControl *spillage) {
  SpillFile *spill = NULL;
  Chunk *candidate;

  /* Look for a chunk that can be spilled */
  
  LOCK_MUTEX(&chunks->lock);
  candidate = NULL != chunks->spilled ? chunks->spilled : chunks->head;

  while (NULL != candidate
	 && (candidate->nwriters != 0
	     || candidate->ref_count == 0
	     || candidate->state != in_core
	     || NULL == candidate->data)) {
    candidate = candidate->next;
  }

  if (NULL == candidate) {
    UNLOCK_MUTEX(&chunks->lock);
    return 0;  /* Nothing suitable */
  }


  if (NULL != spillage->spill && !spillage->spill->is_tmp) {
    /* Input is regular, just need to forget the data read earlier */
    free(candidate->data);
    candidate->data = NULL;
    candidate->state = spilled;
    chunks->spilled = candidate;
    spillage->alloced -= CHUNK_SZ;
    UNLOCK_MUTEX(&chunks->lock);
    return 0;
  }

  candidate->state = spilling;

  /* Otherwise have to store it somewhere */
  if (NULL == spillage->spill || spillage->spill->offset >= options->file_max) {
    UNLOCK_MUTEX(&chunks->lock);
    spill = make_tmp_file(options, spillage->spill);
    if (NULL == spill) goto fail;
    LOCK_MUTEX(&chunks->lock);

    spillage->spill_file_count++;
    if (spillage->spill_file_count > 0) {
      spill->is_tmp = spillage->spill_file_count;
    }
    spillage->open_spill_files++;
    if (spillage->open_spill_files > spillage->max_spill_files) {
      spillage->max_spill_files = spillage->open_spill_files;
    }

    if (verbosity > 1) {
      fprintf(stderr,
	      "%.3f Opened spill file #%d on fd %d; %d spill files now open.\n",
	      get_time(), spillage->spill_file_count, spill->fd,
	      spillage->open_spill_files);
    }
  } else {
    spill = spillage->spill;
  }
  spill->ref_count++;
  UNLOCK_MUTEX(&chunks->lock);

  if (0 != write_all(spill->fd, candidate->data, candidate->len)) {
    perror("Writing to temporary file");
    goto fail;
  }

  if (verbosity > 2) {
    fprintf(stderr, "%.3f Spilled %zd bytes (%sB) to spill file #%d\n",
	    get_time(), candidate->len, human_size(candidate->len),
	    spill->is_tmp);
  }

  LOCK_MUTEX(&chunks->lock);
  candidate->spill = spill;
  candidate->spill_offset = spill->offset;
  free(candidate->data);
  candidate->data = NULL;

  spill->offset += candidate->len;
  
  spillage->alloced -= CHUNK_SZ;

  spillage->total_spilled += candidate->len;
  spillage->curr_spilled  += candidate->len;
  if (spillage->curr_spilled > spillage->max_spilled) {
    spillage->max_spilled = spillage->curr_spilled;
  }

  spillage->spill = spill;

  chunks->spilled = candidate;
  candidate->state = spilled;
  COND_BROADCAST(&chunks->state_change);
  UNLOCK_MUTEX(&chunks->lock);

  return 0;

 fail:
  /* Ensure any threads waiting on chunks->spill_complete get restarted */
  LOCK_MUTEX(&chunks->lock);
  if (NULL != spill) --spill->ref_count;
  candidate->state = in_core;
  COND_BROADCAST(&chunks->state_change);
  UNLOCK_MUTEX(&chunks->lock);

  return -1;
}

/*
 * Reread spilled data.
 *
 * *chunk is the Chunk struct that got spilled
 * *spillage is the SpillControl information
 *
 * Returns  0 on success
 *         -1 on failure
 */

static int reread_data(ChunkList *chunks, Chunk *chunk,
		       SpillControl *spillage) {
  unsigned char *data = NULL;
  size_t len;
  int is_tmp;
  SpillFile *spill;
  off_t spill_offset;
#ifdef HAVE_PTHREADS
  int res;
#endif

  /* Check chunk state.  In multi-threaded mode it's possible that another
     thread might already be trying to get the data back.  If that is the
     case wait for a state change and check again. */

  LOCK_MUTEX(&chunks->lock);
  do {
    switch (chunk->state) {
    case in_core: /* No need to do anything */
      assert(NULL != chunk->data);
      UNLOCK_MUTEX(&chunks->lock);
      return 0;

#ifdef HAVE_PTHREADS
    case rereading: /* Wait for other thread to finish re-reading data */
    case spilling:  /* Wait for spillage to finish */
      res = pthread_cond_wait(&chunks->state_change, &chunks->lock);
      if (res != 0 && res != EINTR) {
	fprintf(stderr, "pthread_cond_wait : %s\n", strerror(res));
	abort();
      }
      break;
#endif

    case spilled:
      assert(NULL == chunk->data);
      break;

    default:
      abort(); /* Should never happen */
    }
  } while (chunk->state != spilled);

  chunk->state = rereading;
  spill = chunk->spill;
  len = chunk->len;
  is_tmp = spill->is_tmp;
  spill_offset = chunk->spill_offset;
  assert(chunk->len > 0 && chunk->len <= CHUNK_SZ);
  UNLOCK_MUTEX(&chunks->lock);

  data = malloc(len);
  if (NULL == data) {
    perror("reread_data");
    goto fail;
  }
  if (0 != pread_all(spill->fd, data, len, spill_offset)) goto fail;

  LOCK_MUTEX(&chunks->lock);
  spillage->alloced += CHUNK_SZ;
  if (spillage->alloced > spillage->max_alloced) {
    spillage->max_alloced = spillage->alloced;
  }

  chunk->data  = data;
  chunk->state = in_core;

  COND_BROADCAST(&chunks->state_change);
  UNLOCK_MUTEX(&chunks->lock);

  if (verbosity > 2) {
    if (is_tmp) {
      fprintf(stderr, "%.3f Re-read %zd bytes (%sB) from spill file #%d\n",
	      get_time(), len, human_size(len), is_tmp);
    } else {
      fprintf(stderr, "%.3f Re-read %zd bytes (%sB) from input file\n",
	      get_time(), len, human_size(len));
    }
  }
  
  return 0;

 fail:
  /* Ensure any threads waiting on chunks->state_change get restarted */
  free(data);
  LOCK_MUTEX(&chunks->lock);
  chunk->state = spilled;
  COND_BROADCAST(&chunks->state_change);
  UNLOCK_MUTEX(&chunks->lock);

  return -1;
}

/*
 * Allocate a new Chunk, and update the linked list and tail pointers to
 * point to it.
 *
 * *chunks is the ChunkList struct with the pointer to the list tail
 * nrefs   is the initial reference count for the new chunk
 *
 * Returns  0 on success
 *         -1 on failure
 */

static int new_chunk(ChunkList *chunks, int nrefs) {
  /* Allocate a new Chunk */
  Chunk *new_tail = calloc(1, sizeof(Chunk));
  if (NULL == new_tail) {
    perror("new_chunk");
    goto fail;
  }

  /* Initialize and make it the new tail */
  new_tail->ref_count = nrefs;
  new_tail->data = malloc(CHUNK_SZ);
  if (NULL == new_tail->data) {
    perror("new_chunk");
    goto fail;
  }


  LOCK_MUTEX(&chunks->lock);
  if (NULL != chunks->tail) {
    chunks->tail->next = new_tail;
  }
  chunks->tail = new_tail;
  UNLOCK_MUTEX(&chunks->lock);

  return 0;

 fail:
  if (new_tail) {
    free(new_tail->data);
    free(new_tail);
  }
  return -1;
}

/*
 * Decerement the reference count for a chunk, and free it if it gets to
 * zero.
 *
 * *chunks  is the ChunkList struct, so we can update head and spilled.
 * *chunk   is the chunk to free
 * *options is the Opts struct, for the maximum memory limit
 * *spillage is the SpillControl information
 *
 * Returns  0 on success
 *         -1 on failure
 */

static int release_chunk(ChunkList *chunks, Chunk *chunk,
			 Opts *options, SpillControl *spillage) {
  /* chuncks should be locked when calling this */
  assert(chunk->ref_count > 0);

  if (--chunk->ref_count > 0) { /* Still in use */
    if (spillage->alloced >= options->max
	&& chunk->nwriters == 0
	&& chunk->state == in_core
	&& NULL != chunk->spill
	&& NULL != chunk->data) {
      /* Re-spill data if still under memory pressure */
      free(chunk->data);
      chunk->data = NULL;
      chunk->state = spilled;
      spillage->alloced -= CHUNK_SZ;
    }
    UNLOCK_MUTEX(&chunks->lock);
    return 0;
  }

  /* No more readers for the chunk, so free it. */
  /* Only one thread can get ref_count == 0, at which point nothing else
     should be using it. */
  assert(chunk == chunks->head); /* Should be head of list */
  chunks->head = chunk->next;
  if (chunk == chunks->spilled) chunks->spilled = NULL;
  if (NULL != chunk->data) {
    free(chunk->data);
    spillage->alloced -= CHUNK_SZ;
  }
  UNLOCK_MUTEX(&chunks->lock);

  if (NULL != chunk->spill) {
    if (0 != release_tmp(chunk->spill, spillage, chunks)) return -1;
  }
  free(chunk);

  return 0;
}

/*
 * Release all chunks from a starting point.  This is used when closing files
 * so any chunks that are no longer needed get removed.
 *
 * *chunks  is the ChunkList struct, so we can update head and spilled.
 * *chunk   is the starting chunk
 * *options is the Opts struct, for the maximum memory limit
 * *spillage is the SpillControl information
 *
 * Returns  0 on success
 *         -1 on failure
 */

static int release_chunks(ChunkList *chunks, Chunk *chunk,
			  Opts *options, SpillControl *spillage) {
  Chunk *next_chunk;
  while (NULL != chunk) {
    LOCK_MUTEX(&chunks->lock); /* release_chunk unlocks */
    next_chunk = chunk->next;
    if (0 != release_chunk(chunks, chunk, options, spillage)) return -1;
    chunk = next_chunk;
  }
  return 0;
}

/* 
 * Work out what to do if the reader has run out of space.
 * Returns 2 if the reader should block with a timeout
 *         1 if the reader should block with no timeout
 *         0 if the reader should spill data
 *        -1 if an error occurred
 */

static inline int wait_or_spill(Opts *options, SpillControl *spillage) {
  double now;
  if (options->wait_time <= 0 || spillage->pipe_list_head == NULL) return 0;
  if (spillage->pipe_list_head->is_reg) {
    /* Slowest output is a regular file in multi-thread mode,
       don't want to spill anything for this so always wait */
    spillage->blocking_output = spillage->pipe_list_head;
    return 1;
  }

  /* Slowest output is a pipe.  Check to see how long ago it was when it
     last wrote something */
  now = get_time();
  if (now < 0) return -1;
  if (now < spillage->pipe_list_head->write_time) {
    /* Someone fiddled with the clock? */
    spillage->pipe_list_head->write_time = now;
  }
  if (now - spillage->pipe_list_head->write_time < options->wait_time){
    /* Not waited long enough, so get reader to wait for a while and
       try again after the time limit should have expired. */
    spillage->blocking_output = spillage->pipe_list_head;
    return 2;
  }

  /* Waited long enough, get the reader to start spilling data */
  return 0;
}

/* 
 * Read some data into the tail Chunk.  If it's full, make a new chunk
 * first and read into that.  If the end of the input file is read,
 * *read_eof is set to true.
 *
 * *options  is the Opts struct
 * *in       is the Input struct for the input file
 * *spillage is the spillage control struct
 * *chunks   is the ChunkList struct with the list head and tail pointers
 * *read_eof is the end-of-file flag
 * nrefs     is the reference count to set on new Chunks.
 *
 * Returns  2 if nothing written and should wait with time-out
 *          1 if nothing written and should wait
 *          0 on success
 *         -1 in failure
 */

static ssize_t do_read(Opts *options, Input *in, SpillControl *spillage,
		       ChunkList *chunks, int *read_eof, int nrefs) {
  ssize_t bytes;

  LOCK_MUTEX(&chunks->lock);
  spillage->blocking_output = NULL;
  if (options->use_threads) {
    /* Check that the outputs are keeping up if in threaded mode.
       If not, wait until an output writes something. */
    if (in->pos - spillage->max_all > CHUNK_SZ * 20
	|| (spillage->npipes > 0
	    && in->pos - spillage->max_pipe > CHUNK_SZ * 20)) {
      spillage->blocking_output = ANY_OUTPUT;
      UNLOCK_MUTEX(&chunks->lock);
      return 1;
    }
  }
  UNLOCK_MUTEX(&chunks->lock);

  assert(NULL != chunks->tail->data);

  if (chunks->tail->len == CHUNK_SZ) {

    LOCK_MUTEX(&chunks->lock);
    if (spillage->alloced >= options->max) {
      /* Run out of memory, check if the reader should wait or start
	 spilling data */
      int r = wait_or_spill(options, spillage);
      UNLOCK_MUTEX(&chunks->lock);
      if (r) return r; /* Decided to wait */
      if (0 != spill_data(options, chunks, spillage)) return -1;
    } else {
      UNLOCK_MUTEX(&chunks->lock);
    }

    /* Need to start a new Chunk */
    if (0 != new_chunk(chunks, nrefs)) return -1;

    LOCK_MUTEX(&chunks->lock);
    if (NULL != spillage->spill && !spillage->spill->is_tmp) {
      /* Set starting offset for spillage purposes */
      chunks->tail->spill_offset = in->pos;
      chunks->tail->spill = spillage->spill;
    }

    spillage->alloced += CHUNK_SZ;
    if (spillage->alloced > spillage->max_alloced) {
      spillage->max_alloced = spillage->alloced;
    }
    UNLOCK_MUTEX(&chunks->lock);
  }

  /* Read some data */ 
  do {
    bytes = read(in->fd, chunks->tail->data + chunks->tail->len,
		 CHUNK_SZ - chunks->tail->len);
  } while (bytes < 0 && errno == EINTR);
  
  if (bytes < 0) { /* Error */
    if ((errno == EAGAIN || errno == EWOULDBLOCK)
	&& !options->use_threads) return 0; /* Blocking is OK */
    fprintf(stderr, "Error reading %s : %s\n",
	    options->in_name, strerror(errno));
    return -1;
  }

  if (bytes == 0) { /* EOF */
    *read_eof = 1;
    if (verbosity > 2) {
      fprintf(stderr, "%.3f Got EOF on input\n", get_time());
    }
    return 0;
  }

  /* Got some data, update length and in->pos */
  in->pos += bytes;
  LOCK_MUTEX(&chunks->lock);
  chunks->tail->len += bytes;
  UNLOCK_MUTEX(&chunks->lock);

  if (verbosity > 2) {
    fprintf(stderr, "%.3f Read %zd bytes from input; %lld (%sB) so far.\n",
	    get_time(), bytes, (long long) in->pos, human_size(in->pos));
  }

  return 0;
}

/*
 * Write some data.
 *
 * *output  is the Output struct for the file to write
 * *chunks  is the linked list of Chunks
 * *options is the Opts struct
 * *spillage is the SpillControl information
 * index    is the index of the output in the outputs array
 *
 * Returns the number of bytes written (>= 0) on success
 *         -1 on failure (not EPIPE)
 *         -2 on EPIPE
 */

static ssize_t do_write(Output *output, ChunkList *chunks,
			Opts *options, SpillControl *spillage, int index) {
  ssize_t bytes = 0;
  Chunk *curr_chunk = output->curr_chunk, *next_chunk;
  size_t curr_chunk_len;
  unsigned char *data;

  LOCK_MUTEX(&chunks->lock);
  next_chunk     = curr_chunk->next;
  curr_chunk_len = curr_chunk->len;
  data           = curr_chunk->data;
  UNLOCK_MUTEX(&chunks->lock);

  while (next_chunk != NULL || output->offset < curr_chunk_len) {
    /* While there's something to write ... */

    assert(NULL != data);

    if (output->offset < curr_chunk_len) {
      /* Data available in the current Chunk */
      ssize_t b;

      /* Send it */
      do {
	b = write(output->fd, data + output->offset,
		  curr_chunk_len - output->offset);
      } while (b < 0 && EINTR == errno);

      if (b < 0) { /* Error */
	if (EAGAIN == errno || EWOULDBLOCK == errno) break; /* Blocking is OK */
	if (EPIPE == errno) return -2;  /* Got EPIPE, file should be closed */
	fprintf(stderr, "Error writing to %s : %s\n",
		output->name, strerror(errno));
	return -1;
      }

      if (b == 0) break;  /* Wrote nothing, try again later */

      /* Update amount written */
      LOCK_MUTEX(&chunks->lock);
      {
	int do_broadcast = 0;
	output->written += b;
	if (output->written > spillage->max_all) {
	  spillage->max_all = output->written;
	  do_broadcast = 1;
	}
	if (!output->is_reg && output->written > spillage->max_pipe) {
	  spillage->max_pipe = output->written;
	  do_broadcast = 1;
	}
	if (do_broadcast && chunks->waiting_input) {
	  COND_SIGNAL(&chunks->data_written);
	}
      }
      UNLOCK_MUTEX(&chunks->lock);
      output->offset += b;
      bytes += b;

      /* Record time and update linked list */
      if (!output->is_reg || options->use_threads) {
	LOCK_MUTEX(&chunks->lock);
	output->write_time = get_time();
	if (output->write_time < 0) {
	  UNLOCK_MUTEX(&chunks->lock);
	  return -1;
	}
	
	while (NULL != output->next
	       && output->next->written < output->written) {
	  Output *n = output->next;
	  assert(n->prev == output);
	  pipe_list_remove(output, spillage);
	  pipe_list_insert(output, n, spillage);
	}

	if (output == spillage->blocking_output
	    || spillage->blocking_output == ANY_OUTPUT) {
	  spillage->blocking_output = spillage->pipe_list_head;
	  if (chunks->waiting_input) COND_SIGNAL(&chunks->data_written);
	}
	UNLOCK_MUTEX(&chunks->lock);
      }
    }

    assert(output->offset <= curr_chunk_len);

    /* Check if at end of current Chunk */
    if (output->offset == curr_chunk_len) {
      /* Stop sending if no more Chunks yet */
      if (NULL == next_chunk) break;

      /* Otherwise, move on to the next Chunk */
      output->curr_chunk = next_chunk;
      output->offset = 0;

      LOCK_MUTEX(&chunks->lock);  /* release_chunk unlocks */
      --curr_chunk->nwriters;
      if (0 != release_chunk(chunks, curr_chunk, options, spillage)) {
	return -1;
      }

      curr_chunk = next_chunk;

      LOCK_MUTEX(&chunks->lock);
      curr_chunk->nwriters++;
      UNLOCK_MUTEX(&chunks->lock);

      /* Re-read spilled data if needed */
      if (0 != reread_data(chunks, curr_chunk, spillage)) return -1;

      LOCK_MUTEX(&chunks->lock);
      next_chunk     = curr_chunk->next;
      curr_chunk_len = curr_chunk->len;
      data           = curr_chunk->data;
      UNLOCK_MUTEX(&chunks->lock);
    }
  }

  if (verbosity > 2 && bytes > 0) {
    fprintf(stderr,
	    "%.3f Wrote %zd bytes to output #%d (%s); %lld (%sB) so far.\n",
	    get_time(), bytes, index, output->name,
	    (long long) output->written, human_size(output->written));
  }

  return bytes;
}

/*
 * Remove entries corresponding to closed files from an index array.
 * The input array will be either the pipes or regular array.  Any entries
 * with the value -1 correspond to closed files and need to be removed.
 * This walks along the array, shuffling items down where necessary to
 * remove ones with negative values.
 *
 * *index is the input array.
 * nitems is the original number of items in the array.
 *
 * Returns the number of items left after any removals.
 */

int cull_index_array(int *index, int nitems) {
  int i, j;

  /* Shuffle down if necessary */
  for (i = 0, j = 0; i < nitems; i++) {
    if (index[i] < 0) continue;
    if (j < i) index[j] = index[i];
    j++;
  }

  /* Wipe out any stale data after the new array end */
  for (i = j; i < nitems; i++) index[i] = -1;

  return j; /* New length */
}

/*
 * Write statistics about how much data was read and written to each output,
 * and how much data was spilled to temporary files.
 *
 * *options  is the Opts struct
 * *in       is the Input struct for the input file
 * noutputs  is the number of output files
 * outputs   is the array of Output structs
 * *spillage is the SpillControl information
 */

static void write_stats(Opts *options, Input *in,
			int noutputs, Output *outputs,
			SpillControl *spillage) {
  int i;

  double now = get_time();
  fprintf(stderr, "%.3f Read %lld bytes (%sB) from input (%s)\n",
	  now, (long long) in->pos, human_size(in->pos), options->in_name);
  for (i = 0; i < noutputs; i++) {
    fprintf(stderr, "%.3f Wrote %lld bytes (%sB) to output #%d (%s)\n",
	    now, (long long) outputs[i].written,
	    human_size(outputs[i].written),
	    i, outputs[i].name);
  }
  fprintf(stderr, "%.3f Maximum buffer used = %zd bytes (%sB)\n",
	  now, spillage->max_alloced, human_size(spillage->max_alloced));
  if (!in->reg) {
    fprintf(stderr, "%.3f Spilled %lld bytes (%sB) to temporary files\n",
	    now, (long long) spillage->total_spilled,
	    human_size(spillage->total_spilled));
    if (spillage->total_spilled > 0) {
      fprintf(stderr, "%.3f Maximum spilled at one time = %lld bytes (%sB)\n",
	      now, (long long) spillage->max_spilled,
	      human_size(spillage->max_spilled));
      fprintf(stderr, "%.3f Total temporary files opened = %d\n",
	      now, spillage->spill_file_count);
      fprintf(stderr,
	      "%.3f Maximum temporary files in use at one time = %d\n",
	      now, spillage->max_spill_files);
    }
  }
}

/*
 * Do the copies from input to all the outputs.
 *
 * *options  is the Opts struct
 * *in       is the Input struct for the input file.
 * noutputs  is the number of entries in the outputs array
 * outputs   is the array of Output structs.
 * nregular  is the number of entries in the regular array
 * regular   is an array of indexes into outputs of the regular output files
 * npipes    is the number of entries in the pipes array
 * pipes     is an array of indexes into outputs of the poll-able output files
 * *spillage is the SpillControl information
 *
 * Returns  0 on success
 *         -1 on failure
 */

static int do_copy(Opts *options, Input *in,
		   int noutputs, Output *outputs,
		   int nregular, int *regular,
		   int npipes, int *pipes,
		   SpillControl *spillage) {
  ChunkList chunks = {
    NULL, NULL, NULL, NULL, 0, 0, 0,
#ifdef HAVE_PTHREADS
    PTHREAD_MUTEX_INITIALIZER, PTHREAD_COND_INITIALIZER,
    PTHREAD_COND_INITIALIZER, PTHREAD_COND_INITIALIZER
#endif
  }; /* Linked list of Chunks */ 
  struct pollfd *polls; /* structs for poll(2) */
  int   *poll_idx;    /* indexes in outputs corresponding to entries in polls */
  int   *closing_pipes; /* Pipes that need to be closed */
  int   *closing_reg;   /* Regular files that need to be closed */
  int i, keeping_up = npipes, read_eof = 0, nclosed = 0;

  if (0 != new_chunk(&chunks, noutputs)) return -1; /* Initial Chunk */
  chunks.head = chunks.tail;
  chunks.head->nwriters  = noutputs;
  polls         = malloc((noutputs + 1) * sizeof(struct pollfd));
  poll_idx      = malloc((noutputs + 1) * sizeof(int));
  closing_pipes = malloc((npipes + 1)   * sizeof(int));
  closing_reg   = malloc((nregular + 1) * sizeof(int));
  if (NULL == chunks.head || NULL == polls || NULL == poll_idx
      || NULL == closing_pipes || NULL == closing_reg) {
    perror("do_copy");
    return -1;
  }

  /* Point all outputs to the initial Chunk and build linked list of pipes */
  for (i = 0; i < noutputs; i++) {
    outputs[i].curr_chunk = chunks.head;
    if (!outputs[i].is_reg) pipe_list_push(&outputs[i], spillage);
  }

  do {  /* Main loop */
    int npolls = 0, pipe_close = 0, reg_close = 0;
    /* Are we waiting for a slow output? */
    int blocked = (NULL != spillage->blocking_output
		   && spillage->alloced >= options->max);
    int timeout = -1;
    int should_read;

    if (blocked) {
      /* Work out how long to wait in poll */
      double now = get_time();
      double left;
      if (now < 0) return -1;
      if (now < spillage->blocking_output->write_time) {
	/* Someone fiddled with the clock? */
	spillage->blocking_output->write_time = now;
      }
      left = options->wait_time - (now - spillage->blocking_output->write_time);
      timeout = left > 0 ? (int)(left * 1000) : 0;
      if (timeout == 0) { /* Remove the block */
	blocked = 0;
	spillage->blocking_output = NULL;
      }
    }

    /* Only try to read if not at EOF; either there are no 
       pipes or at least one pipe has nothing left to write;
       and we aren't waiting for a slow output in order to prevent spillage */
    should_read = (!read_eof
		   && (npipes == 0 || keeping_up > 0)
		   && !blocked);

    if (should_read) {
      if (in->reg) {
	/* If reading a regular file, do it now */
	if (do_read(options, in, spillage, &chunks, &read_eof,
		    noutputs - nclosed) < 0) {
	  return -1;
	}
      } else {
	/* Otherwise add it to the poll list */
	polls[npolls].fd = in->fd;
	poll_idx[npolls] = -1;
	polls[npolls].events = POLLIN;
	polls[npolls++].revents = 0;
      }
    }

    keeping_up = 0;  /* Number of pipes that are keeping up */

    /* Add all the pipe outputs that have something to write to the poll list */
    for (i = 0; i < npipes; i++) {
      if (outputs[pipes[i]].curr_chunk != chunks.tail
	  || outputs[pipes[i]].offset < chunks.tail->len) {
	/* Something to write */
	polls[npolls].fd = outputs[pipes[i]].fd;
	poll_idx[npolls] = i;
	polls[npolls].events = POLLOUT|POLLERR|POLLHUP;
	polls[npolls++].revents = 0;

	if (outputs[pipes[i]].curr_chunk == chunks.tail
	    || outputs[pipes[i]].curr_chunk->next == chunks.tail) {
	  /* Encourage more reading if near to the end of the stored data.
	     This should allow a small buffer to build up, which can
	     help to avoid stalls in pipelines */
	  keeping_up++;
	}
      } else {
	/* Keeping up or finished */
	if (read_eof) {
	  closing_pipes[pipe_close++] = i;
	  timeout = 0; /* Ensure pipe gets closed promptly */
	} else {
	  keeping_up++;
	}
      }
    }
    
    if (npolls > 0) {  /* Need to do some polling */
      int ready;
      do {
	ready = poll(polls, npolls, timeout);
      } while (ready < 0 && EINTR == errno);

      if (ready < 0) {
	perror("poll failed in do_copy");
	return -1;
      }

      for (i = 0; i < npolls && ready > 0; i++) {
	if (polls[i].revents) {  /* Got some events */
	  
	  --ready;
	  if (poll_idx[i] < 0) {  /* Input, try to read from it. */
	    if (do_read(options, in, spillage, &chunks,
			&read_eof, noutputs - nclosed) < 0) {
	      return -1;
	    }

	  } else {  /* Output, try to write to it. */
	    Output *output = &outputs[pipes[poll_idx[i]]];
	    ssize_t res = do_write(output, &chunks, options,
				   spillage, pipes[poll_idx[i]]);

	    if (-2 == res) { /* Got EPIPE, add to closing_pipes list */
	      closing_pipes[pipe_close++] = poll_idx[i];
	      continue;
	    } else if (res < 0) { /* other write error, give up */
	      return -1;
	    }

	    if (output->curr_chunk == chunks.tail
		&& output->offset == chunks.tail->len) {
	      /* All the data so far has been written to this output */
	      if (read_eof) {
		/* If finished reading, add to closing_pipes */
		closing_pipes[pipe_close++] = poll_idx[i];
	      } else {
		/* otherwise, add to keeping_up count, to
		   encourage more reading */
		keeping_up++;
	      }
	    } else if (!read_eof
		       && (output->curr_chunk == chunks.tail
			   || output->curr_chunk->next == chunks.tail)) {
	      /* Encourage more reading if near to the end of the stored data.
		 This should allow a small buffer to build up, which can
		 help to avoid stalls in pipelines */
	      keeping_up++;
	    }
	  }
	}
      }
    } /* End of polling section */

    /* Deal with regular output files */

    for (i = 0; i < nregular; i++) {
      /* Try to write */
      if (do_write(&outputs[regular[i]], &chunks,
		   options, spillage, regular[i]) < 0) {
	return -1;
      }

      if (read_eof
	  && outputs[regular[i]].curr_chunk == chunks.tail
	  && outputs[regular[i]].offset == chunks.tail->len) {
	/* If all data written and finished reading, add to closing_reg list */
	closing_reg[reg_close++] = i;
      }
    }

    /* Close any regular files that have finished */

    for (i = 0; i < reg_close; i++) {
      int reg_idx = closing_reg[i];
      int to_close;

      assert(reg_idx >= 0 && reg_idx < nregular);

      to_close = regular[reg_idx];
      assert(to_close >= 0 && to_close < noutputs);
      assert(outputs[to_close].fd >= 0);

      if (0 != close(outputs[to_close].fd)) {
	fprintf(stderr, "Error closing %s : %s\n",
		outputs[to_close].name, strerror(errno));
	return -1;
      }
      if (verbosity > 1) {
	fprintf(stderr, "%.3f Closed output #%d (%s)\n",
		get_time(), to_close, outputs[to_close].name);
      }
      outputs[to_close].fd = -1;
      regular[reg_idx] = -1; /* Mark for removal */

      if (0 != release_chunks(&chunks, outputs[to_close].curr_chunk,
			      options, spillage)) {
	return -1;
      }
      nclosed++;
    }

    /* Remove entries for closed outputs from the regular array */
    if (reg_close) nregular = cull_index_array(regular, nregular);

    /* Close any poll-able files that have finished */

    for (i = 0; i < pipe_close; i++) {
      int pipe_idx = closing_pipes[i];
      int to_close;

      assert(pipe_idx >= 0 && pipe_idx < npipes);

      to_close = pipes[closing_pipes[i]];
      assert(to_close >= 0 && to_close < noutputs);
      assert(outputs[to_close].fd >= 0);

      if (0 != close(outputs[to_close].fd)) {
	fprintf(stderr, "Error closing %s : %s\n",
		outputs[to_close].name, strerror(errno));
	return -1;
      }
      if (verbosity > 1) {
	fprintf(stderr, "%.3f Closed output #%d (%s)\n",
		get_time(), to_close, outputs[to_close].name);
      }
      outputs[to_close].fd = -1;
      pipes[pipe_idx] = -1; /* Mark for removal */

      /* Remove from spillage linked list */
      pipe_list_remove(&outputs[to_close], spillage);

      /* Release any data referenced by this output */
      if (0 != release_chunks(&chunks, outputs[to_close].curr_chunk,
			      options, spillage)) {
	return -1;
      }
      nclosed++;
    }

    /* Remove entries for closed outputs from the pipes array */
    if (pipe_close) npipes = cull_index_array(pipes, npipes);

  } while (nclosed < noutputs);

  if (verbosity > 0) {
    write_stats(options, in, noutputs, outputs, spillage);
  }

  return 0;
}

/*
 * Show the usage message
 *
 * prog is the program name
 */

void show_usage(char *prog) {
  fprintf(stderr, "Usage: %s [-m <limit>] [-f <limit>] [-t temp_dir]\n", prog);
}

/*
 * Parse a size option.  This is a number followed optionally by one of
 * the letters [TtGgMmKk].  The number is scaled as suggested by the suffix
 * letter.
 *
 * *in     is the input string
 * *sz_out is the output size in bytes
 *
 * Returns  0 on success
 *         -1 if the input did not start with a number or the suffix was invalid
 */

int parse_size_opt(char *in, size_t *sz_out) {
  unsigned long ul;
  char *endptr = in;
  size_t sz;

  ul = strtoul(in, &endptr, 10);
  if (endptr == in) return -1;
  sz = ul;

  switch(*endptr) {
  case 'T': case 't':
    sz *= 1024;
    if (sz < ul) return -1;
  case 'G': case 'g':
    sz *= 1024;
    if (sz < ul) return -1;
  case 'M': case 'm':
    sz *= 1024;
    if (sz < ul) return -1;
  case 'K': case 'k':
    sz *= 1024;
    if (sz < ul) return -1;
  case '\0':
    break;
  default:
    return -1;
  }

  *sz_out = sz;
  return 0;
}

/* Parse command-line options
 *
 * argc        is the option count
 * argv        is the array of option strings
 * *options    is the Opts strict to be filled out
 * *after_opts is the index into argv that follows the option strings
 *
 * Returns  0 on success
 *         -1 on failure
 */

int get_options(int argc, char** argv, Opts *options, int *after_opts) {
  int opt;

  options->max       = DEFAULT_MAX;
  options->file_max  = DEFAULT_FILE_MAX;
  options->tmp_dir   = DEFAULT_TMP_DIR;
  options->wait_time = DEFAULT_WAIT_TIME;
  options->in_name   = NULL;
#ifdef HAVE_PTHREADS
  options->use_threads = 1;
#else
  options->use_threads = 0;
#endif

  while ((opt = getopt(argc, argv, "m:f:pPt:i:vw:")) != -1) {
    switch (opt) {

    case 'i':
      options->in_name = optarg;
      break;

    case 'f': {
      size_t sz = 0;
      if (0 != parse_size_opt(optarg, &sz)) {
	fprintf(stderr, "Couldn't understand file size limit '%s'\n", optarg);
	return -1;
      }
      if (0 == sz) {
	fprintf(stderr, "File size limit should be > 0\n");
	return -1;
      }
      options->file_max = sz;
      break;
    }

    case 'm': {
      size_t sz = 0;
      if (0 != parse_size_opt(optarg, &sz)) {
	fprintf(stderr, "Couldn't understand memory limit '%s'\n", optarg);
	return -1;
      }
      if (0 == sz) {
	fprintf(stderr, "Memory limit should be > 0\n");
	return -1;
      }
      options->max = sz;
      break;
    }

    case 'p':
#ifdef HAVE_PTHREADS
      options->use_threads = 1;
#endif
      break;

    case 'P':
      options->use_threads = 0;
      break;

    case 't':
      options->tmp_dir = optarg;
      break;

    case 'v':
      verbosity++;
      break;

    case 'w': {
      char *end = NULL;
      options->wait_time = strtod(optarg, &end);
      if (end == optarg) {
	fprintf(stderr,
		"Couldn't understand wait time (-w) argument '%s'\n", optarg);
	return -1;
      }
      if (options->wait_time > MAX_WAIT_TIME) {
	fprintf(stderr, "Wait time must be less than %f\n", MAX_WAIT_TIME);
	return -1;
      }
      break;
    }

    default:
      show_usage(argv[0]);
      return -1;
    }
  }

  options->tmp_dir_len = strlen(options->tmp_dir);

  *after_opts = optind;
  return 0;
}

#ifdef HAVE_PTHREADS

/*
 * Runs a writer thread.  This waits for data to arrive and then writes it
 * out.  It waits on a conditional whenever it runs out of data.  At the
 * end, it tidies up, puts the output on the chunks->closed linked list
 * and leaves an exit status in output->res.
 *
 * This function is run via pthread_create()
 *
 * *v is a ThreadParams struct containing pointers to the Opts, Output,
 *    SpillControl and ChunkList structs needed by the writer.  It also
 *    includes an index number for the thread for use in messages.
 *
 * Returns NULL
 */

void * run_write_thread(void *v) {
  ThreadParams *p = (ThreadParams *) v;
  Opts *options          = p->options;
  Output *output         = p->output;
  SpillControl *spillage = p->spillage;
  ChunkList *chunks      = p->chunks;
  int index              = p->index;
  int is_reg;
  int res;
  ssize_t bytes = 0;

  free(p);

  if (0 == strcmp(output->name, "-")) {
    output->fd = fileno(stdout);
    output->name = "stdout";
  } else {
    output->fd = open(output->name, O_WRONLY|O_CREAT|O_TRUNC, 0666);
    if (output->fd < 0) {
      fprintf(stderr, "Couldn't open %s for writing : %s\n",
	      output->name, strerror(errno));
      output->res = -1;
      goto out;
    }
  }
  is_reg = file_is_regular(output->name, output->fd);
  if (is_reg < 0) {
    output->res = -1;
    goto out;
  }

  LOCK_MUTEX(&chunks->lock);
  output->is_reg = is_reg;
  if (is_reg) spillage->npipes--;

  /* Wait for some data */
  while (NULL == chunks->head && !chunks->read_eof) {
    chunks->waiting_outputs++;
    res = pthread_cond_wait(&chunks->new_data, &chunks->lock);
    chunks->waiting_outputs--;
    if (res != 0 && res != EINTR) {
      fprintf(stderr, "pthread_cond_wait : %s\n", strerror(res));
      abort();
    }
  }
  output->curr_chunk = chunks->head;
  UNLOCK_MUTEX(&chunks->lock);

  if (verbosity > 1) {
    fprintf(stderr, "%.3f Opened output #%d (%s) on fd %d.\n",
	    get_time(), index, output->name, output->fd);
  }

  for (;;) {
    int at_end;
    
    LOCK_MUTEX(&chunks->lock);
    at_end = (output->curr_chunk->next == NULL
	      && output->offset >= output->curr_chunk->len);

    if (at_end) {
      if (chunks->read_eof) { /* Finished */
	UNLOCK_MUTEX(&chunks->lock);
	break;
      } else {        /* Wait for more data */
	chunks->waiting_outputs++;
	res = pthread_cond_wait(&chunks->new_data, &chunks->lock);
	chunks->waiting_outputs--;
	if (res != 0) {
	  fprintf(stderr, "pthread_cond_wait : %s\n", strerror(res));
	  abort();
	}
      }
    }
    UNLOCK_MUTEX(&chunks->lock);

    bytes = do_write(output, chunks, options, spillage, index);
    if (bytes < 0) {
      if (bytes == -1) output->res = -1;
      break;
    }
  }

  if (0 != close(output->fd)) {
    fprintf(stderr, "Error closing %s : %s\n", output->name, strerror(errno));
    output->res = -1;
  }
  if (verbosity > 1) {
    fprintf(stderr, "%.3f Closed output #%d (%s)\n",
	    get_time(), index, output->name);
  }
  output->fd = -1;

 out:
  LOCK_MUTEX(&chunks->lock);
  pipe_list_remove(output, spillage);
  if (!output->is_reg) spillage->npipes--;
  output->next = chunks->closed;
  chunks->closed = output;
  UNLOCK_MUTEX(&chunks->lock);

  return NULL;
}

/*
 * Start the output threads.  First all the Output structs are initialized,
 * then the threads are started using pthread_create.
 *
 * *options  is the Opts struct
 * n         is the number of outputs
 * names     is an array of output file names
 * outputs   is the array of Output structs
 * *spillage is the SpillControl information
 * *chunks   is the ChunkList struct for the chunks linked list.  It also
 *           includes mutexes and conditionals for thread synchronization.
 *
 * Returns  0 on success
 *         -1 on failure
 */

int start_output_threads(Opts *options, int n, char **names, Output *outputs,
			 SpillControl *spillage, ChunkList *chunks) {
  int i, res;
  ThreadParams *p = NULL;

  for (i = 0; i < n; i++) {
    outputs[i].name = names[i];
    outputs[i].res  = 0;
    outputs[i].curr_chunk = NULL;
    outputs[i].offset  = 0;
    outputs[i].written = 0;
    outputs[i].is_reg = 0;
    outputs[i].fd = -1;
    outputs[i].write_time = get_time();
    pipe_list_push(&outputs[i], spillage);
  }

  for (i = 0; i < n; i++) {
    p = malloc(sizeof(ThreadParams));
    if (NULL == p) goto memfail;
    p->options  = options;
    p->output   = &outputs[i];
    p->spillage = spillage;
    p->chunks   = chunks;
    p->index    = i;

    res = pthread_create(&outputs[i].thread, NULL, run_write_thread, p);
    if (res) goto fail;
  }

  return 0;
 memfail:
  res = errno;
 fail:
  fprintf(stderr, "Couldn't start output thread #%d: %s\n", i, strerror(res));
  return -1;
}

/*
 * Run a multi-threaded copy.  This implements the reading thread, which
 * reads from the input file descriptor, storing the data in the linked
 * list of chunks.  If it gets too far ahead, it will either wait on
 * a conditional for the outputs to catch up; or spill data as necessary.
 * It cleans up any output threads that finish early using pthread_join
 * as the read progresses.  When it has finished, it pthread_joins any
 * remaining threads.  The return status of each output thread is checked,
 * the return status of this function is set to -1 if any of the output
 * threads reports a failure.
 *
 * *options  is the Opts struct
 * *in       is the Input struct for the input file.
 * noutputs  is the number of entries in the outputs array
 * outputs   is the array of Output structs.
 * *spillage is the SpillControl information
 * *chunks   is the ChunkList struct for the chunks linked list.  It also
 *           includes mutexes and conditionals for thread synchronization.
 *
 * Returns  0 on success
 *         -1 on failure
 */

int do_thread_copy(Opts *options, Input *in, int noutputs, Output *outputs,
		   SpillControl *spillage, ChunkList *chunks) {
  int read_eof = 0, nclosed = 0;
  int res = 0, r, i;
  ssize_t b;
  unsigned char *joined;

  joined = calloc(noutputs, sizeof(unsigned char));
  if (NULL == joined) {
    perror("do_thread_copy");
    return -1;
  }

  LOCK_MUTEX(&chunks->lock);
  for (;;) {
    Output *closed;

    closed   = chunks->closed;
    chunks->closed = NULL;
    UNLOCK_MUTEX(&chunks->lock);

    while (NULL != closed) {
      /* deal with closed outputs */
      nclosed++;
      if (closed->res != 0) res = -1;
      if (0 != release_chunks(chunks, closed->curr_chunk,
			      options, spillage)) {
	closed->res = -1;
      }
      if (0 != (r = pthread_join(closed->thread, NULL))) {
	fprintf(stderr, "pthread_join : %s\n", strerror(r));
	res = -1;
      }
      joined[closed - outputs] = 1;
      closed = closed->next;
    }
    if (nclosed == noutputs) break;

    b = do_read(options, in, spillage, chunks, &read_eof, noutputs - nclosed);
    if (b < 0) {
      res = -1;
      read_eof = 1;
    }

    LOCK_MUTEX(&chunks->lock);
    if (read_eof) {
      chunks->read_eof = read_eof;
      UNLOCK_MUTEX(&chunks->lock);
      break;
    }

    if (0 == b && chunks->waiting_outputs > 0) {
      COND_BROADCAST(&chunks->new_data);
    }

    if (NULL != spillage->blocking_output) {
      /* Pause while we wait for a slow output to catch up.
	 do_read should have set b to 1 or 2 depending on if we want to
	 wait indefinitely or with a time-out. */
      r = 0;
      if (b == 1) {
	chunks->waiting_input++;
	r = pthread_cond_wait(&chunks->data_written, &chunks->lock);
	chunks->waiting_input--;
      } else {
	double now = get_time();
	if (now >= 0 &&
	    now - spillage->pipe_list_head->write_time < options->wait_time) {
	  struct timespec timeout;
	  calc_timeout(&timeout, now, options->wait_time);
	  chunks->waiting_input++;
	  r = pthread_cond_timedwait(&chunks->data_written,
				     &chunks->lock, &timeout);
	  chunks->waiting_input--;
	}
      }
      if (r != 0 && r != ETIMEDOUT && r != EINTR) {
	fprintf(stderr, "pthread_cond_timedwait : %s\n", strerror(r));
	abort();
      }
    }
  }
  /* Mutex will be unlocked on escaping from the loop above */

  /* Wait for remaining data to flush out */
  LOCK_MUTEX(&chunks->lock);
  COND_BROADCAST(&chunks->new_data);
  UNLOCK_MUTEX(&chunks->lock);
  
  /* Join any remaining threads */
  if (nclosed < noutputs) {
    for (i = 0; i < noutputs; i++) {
      if (joined[i]) continue;
      if (0 != (r = pthread_join(outputs[i].thread, NULL))) {
	fprintf(stderr, "pthread_join : %s\n", strerror(r));
	res = -1;
      }
      if (outputs[i].res < 0) res = -1;
      joined[i] = 1;
      nclosed++;
    }
  }

  /* paranoia check */
  for (i = 0; i < noutputs; i++) assert(joined[i] != 0);

  if (verbosity > 0) {
    write_stats(options, in, noutputs, outputs, spillage);
  }

  return res;
}
#endif

int main(int argc, char** argv) {
  Opts    options;
  int     out_start = argc;
  Input   in = { 0, -1, 0 };
  Output *outputs = calloc((argc - 1), sizeof(Output));
  int    *regular = malloc((argc - 1) * sizeof(int));
  int    *pipes   = malloc((argc - 1) * sizeof(int));
  SpillControl spillage = {
    NULL, NULL, NULL, NULL, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
  };  /* Spillage info. */
#ifdef HAVE_PTHREADS
  ChunkList chunks = { NULL, NULL, NULL, NULL, 0, 0, 0,
		       PTHREAD_MUTEX_INITIALIZER, PTHREAD_COND_INITIALIZER,
		       PTHREAD_COND_INITIALIZER, PTHREAD_COND_INITIALIZER };
#endif
  int     nregular = 0, npipes = 0;
  struct sigaction sig;

  if (NULL == outputs || NULL == regular || NULL == pipes) {
    perror("malloc");
    return EXIT_FAILURE;
  }

  /* Ignore SIGPIPEs, we want write(2) to get EPIPE instead. */
  memset(&sig, 0, sizeof(sig));
  sig.sa_handler = SIG_IGN;
  if (0 != sigaction(SIGPIPE, &sig, NULL)) {
    perror("sigaction");
    return EXIT_FAILURE;
  }

  if (0 != get_options(argc, argv, &options, &out_start)) {
    return EXIT_FAILURE;
  }

  if (out_start >= argc) {
    fprintf(stderr, "No output files specified\n");
    return EXIT_FAILURE;
  }

  if (verbosity > 0) {
    fprintf(stderr, "%.3f Started.\n", get_time());
  }

  spillage.npipes = argc - out_start;

#ifdef HAVE_PTHREADS
  if (options.use_threads) {
    if (0 != new_chunk(&chunks, argc - out_start)) return EXIT_FAILURE;
    chunks.head = chunks.tail;
    chunks.head->nwriters  = argc - out_start;

    /* Open the output files */
    if (start_output_threads(&options, argc - out_start, argv + out_start,
			     outputs, &spillage, &chunks) != 0){
      return EXIT_FAILURE;
    }
  }
#endif

  if (0 != open_input(&options, &in, &spillage)) {
    return EXIT_FAILURE;
  }

#ifdef HAVE_PTHREADS
  if (!options.use_threads) {
#endif
    /* Open the output files now if single-threaded */
    if (open_outputs(argc - out_start, argv + out_start, outputs,
		     regular, pipes, &nregular, &npipes) != 0) {
      return EXIT_FAILURE;
    }
#ifdef HAVE_PTHREADS
  }
#endif

#ifdef HAVE_PTHREADS
  if (options.use_threads) {
    if (do_thread_copy(&options, &in, argc - out_start,
		       outputs, &spillage, &chunks) != 0) {
      return EXIT_FAILURE;
    }
  } else {
#endif
    /* Copy input to all outputs */
    if (do_copy(&options, &in, argc - out_start, outputs,
		nregular, regular, npipes, pipes, &spillage) != 0) {
      return EXIT_FAILURE;
    }
#ifdef HAVE_PTHREADS
  }
#endif

  if (verbosity > 0) {
    fprintf(stderr, "%.3f Finished.\n", get_time());
  }

  return EXIT_SUCCESS;
}
