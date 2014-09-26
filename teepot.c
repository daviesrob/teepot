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
#if !defined(HAVE_CLOCK_GETTIME) && defined(HAVE_GETTIMEOFDAY)
# include <sys/time.h>
#else
# include <time.h>
#endif

/* Options */
typedef struct {
  size_t      max;      /* Maximum amount of data to keep in memory */
  off_t       file_max; /* Maximum size of each temp file */
  double      wait_time; /* Time to wait before spilling data */
  const char *tmp_dir;  /* Where to write spilled data */
  const char *in_name;  /* Name of input file */
  size_t      tmp_dir_len; /* strlen(tmp_dir) */
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
  int     is_tmp;     /* 1: fd is a temporary file; 0: fd is the input file */ 
} SpillFile;

/* A chunk of input */

typedef struct Chunk {
  unsigned char *data;          /* The data */
  size_t         len;           /* Length of data */
  unsigned int   ref_count;     /* Number of outputs that refer to this */
  unsigned int   nwriters;      /* Number of outputs currently writing this */
  off_t          spill_offset;  /* Offset into spill file */
  SpillFile     *spill;         /* Spill file */
  struct Chunk  *next;          /* Next Chunk in the list */
} Chunk;

/* Chunk size to use */
#define CHUNK_SZ (1024*1024)

typedef struct ChunkList {
  Chunk *head;    /* List head */
  Chunk *spilled; /* Last chunk to be spilled to a file */
  Chunk *tail;    /* List tail */
} ChunkList;

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
} Output;

/* Spillage control */
typedef struct {
  SpillFile *spill;          /* The current file to spill to */
  size_t     alloced;        /* Amount of space currently allocated */
  Output    *pipe_list_head; /* Linked list of non-regular outputs */
  Output    *pipe_list_tail; /* Linked list of non-regular outputs */
  Output    *blocking_output; /* Output being waited on if deferring spillage */
} SpillControl;

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
    /* Set input pipes to non-blocking mode */
    if (0 != setnonblock(options->in_name, in->fd)) return -1;
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

int release_tmp(SpillFile *spill, SpillControl *spillage) {
  if (!spill->is_tmp || --spill->ref_count > 0) return 0;  /* Still in use */

  if (0 != close(spill->fd)) {
    perror("Closing temporary file");
    return -1;
  }

  if (spillage->spill == spill) spillage->spill = NULL;

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
      abort();
      perror("Reading temporary file");
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
  SpillFile *spill;
  Chunk *candidate;

  /* Look for a chunk that can be spilled */
  candidate = NULL != chunks->spilled ? chunks->spilled : chunks->head;

  while (NULL != candidate
	 && (candidate->nwriters > 0 || NULL == candidate->data)) {
    candidate = candidate->next;
  }
  if (NULL == candidate) return 0;  /* Nothing suitable */

  if (NULL != spillage->spill && !spillage->spill->is_tmp) {
    /* Input is regular, just need to forget the data read earlier */
    free(candidate->data);
    candidate->data = NULL;
    spillage->alloced -= CHUNK_SZ;
    return 0;
  }

  /* Otherwise have to store it somewhere */
  if (NULL == spillage->spill || spillage->spill->offset >= options->file_max) {
    spill = make_tmp_file(options, spillage->spill);
    if (NULL == spill) return -1;
  } else {
    spill = spillage->spill;
  }

  if (0 != write_all(spill->fd, candidate->data, candidate->len)) {
    perror("Writing to temporary file");
    return -1;
  }

  candidate->spill = spill;
  candidate->spill_offset = spill->offset;
  spill->offset += candidate->len;
  spill->ref_count++;

  free(candidate->data);
  candidate->data = NULL;
  spillage->alloced -= CHUNK_SZ;

  spillage->spill = spill;
  chunks->spilled = candidate;

  return 0;
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

static int reread_data(Chunk *chunk, SpillControl *spillage) {
  assert(chunk->len > 0 && chunk->len <= CHUNK_SZ);
  chunk->data = malloc(chunk->len);
  if (NULL == chunk->data) {
    perror("reread_data");
    return -1;
  }
  if (0 != pread_all(chunk->spill->fd,
		     chunk->data, chunk->len, chunk->spill_offset)) {
    return -1;
  }
  spillage->alloced += CHUNK_SZ;
  return 0;
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
    return -1;
  }

  /* Initialize and make it the new tail */
  new_tail->ref_count = nrefs;
  chunks->tail->next = new_tail;
  chunks->tail = new_tail;

  return 0;
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
  if (--chunk->ref_count > 0) {
    if (spillage->alloced >= options->max
	&& chunk->nwriters == 0
	&& NULL != chunk->spill
	&& NULL != chunk->data) {
      /* Re-spill data if still under memory pressure */
      free(chunk->data);
      chunk->data = NULL;
      spillage->alloced -= CHUNK_SZ;
    }
    return 0;
  }

  /* No more readers for the chunk, so free it */
  assert(chunk == chunks->head); /* Should be head of list */
  chunks->head = chunk->next;
  if (chunk == chunks->spilled) chunks->spilled = NULL;
  if (NULL != chunk->spill) {
    if (0 != release_tmp(chunk->spill, spillage)) return -1;
  }
  if (NULL != chunk->data) {
    free(chunk->data);
    spillage->alloced -= CHUNK_SZ;
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
  for (;NULL != chunk; chunk = chunk->next) {
    if (0 != release_chunk(chunks, chunk, options, spillage)) return -1;
  }
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
 * Returns  0 on success
 *         -1 in failure
 */

static ssize_t do_read(Opts *options, Input *in, SpillControl *spillage,
		       ChunkList *chunks, int *read_eof, int nrefs) {
  ssize_t bytes;

  spillage->blocking_output = NULL;

  if (chunks->tail->len == CHUNK_SZ || NULL == chunks->tail->data) {

    if (spillage->alloced >= options->max) {
      if (options->wait_time > 0 && spillage->pipe_list_head != NULL) {
	double now = get_time();
	if (now < 0) return -1;
	if (now < spillage->pipe_list_head->write_time) {
	  /* Someone fiddled with the clock? */
	  spillage->pipe_list_head->write_time = now;
	}
	if (now - spillage->pipe_list_head->write_time < options->wait_time) {
	  /* Not waited long enough, so return without reading anything */
	  spillage->blocking_output = spillage->pipe_list_head;
	  return 0;
	}
      }

      if (0 != spill_data(options, chunks, spillage)) return -1;
    }

    if (chunks->tail->len == CHUNK_SZ) {
      /* Need to start a new Chunk */
      if (0 != new_chunk(chunks, nrefs)) return -1;
      
      if (NULL != spillage->spill && !spillage->spill->is_tmp) {
	/* Set starting offset for spillage purposes */
	chunks->tail->spill_offset = in->pos;
	chunks->tail->spill = spillage->spill;
      }
    }

    /* Allocate a buffer to put the data in */
    chunks->tail->data = malloc(CHUNK_SZ);
    if (NULL == chunks->tail->data) {
      perror("do_read");
      return -1;
    }
    spillage->alloced += CHUNK_SZ;
  }

  /* Read some data */ 
  do {
    bytes = read(in->fd, chunks->tail->data + chunks->tail->len,
		 CHUNK_SZ - chunks->tail->len);
  } while (bytes < 0 && errno == EINTR);
  
  if (bytes < 0) { /* Error */
    if (errno == EAGAIN || errno == EWOULDBLOCK) return 0; /* Blocking is OK */
    fprintf(stderr, "Error reading %s : %s\n",
	    options->in_name, strerror(errno));
    return -1;
  }

  if (bytes == 0) { /* EOF */
    *read_eof = 1;
    return 0;
  }

  /* Got some data, update length and in->pos */
  in->pos += bytes;
  chunks->tail->len += bytes;

  return 0;
}

/*
 * Write some data.
 *
 * *output  is the Output struct for the file to write
 * *chunks  is the linked list of Chunks
 * *options is the Opts struct
 * *spillage is the SpillControl information
 *
 * Returns the number of bytes written (>= 0) on success
 *         -1 on failure (not EPIPE)
 *         -2 on EPIPE
 */

static ssize_t do_write(Output *output, ChunkList *chunks,
			Opts *options, SpillControl *spillage) {
  ssize_t bytes = 0;
  Chunk *curr_chunk = output->curr_chunk;

  while (curr_chunk->next != NULL || output->offset < curr_chunk->len) {
    /* While there's something to write ... */

    assert(NULL != curr_chunk->data);

    if (output->offset < curr_chunk->len) {
      /* Data available in the current Chunk */
      ssize_t b;

      /* Send it */
      do {
	b = write(output->fd, curr_chunk->data + output->offset,
		  curr_chunk->len - output->offset);
      } while (b < 0 && EINTR == errno);

      if (b < 0) { /* Error */
	if (EAGAIN == errno || EWOULDBLOCK == errno) break; /* Blocking is OK */
	if (EPIPE == errno) return -2;  /* Got EPIPE, file should be closed */
	fprintf(stderr, "Error writing to %s : %s\n",
		output->name, strerror(errno));
	return -1;
      }

      if (b == 0) break;  /* Wrote nothing, try again later */

      /* Update amount read */
      output->written += b;
      output->offset += b;
      bytes += b;

      /* Record time and update linked list */
      if (!output->is_reg) {
	output->write_time = get_time();
	if (output->write_time < 0) {
	  return -1;
	}
	
	while (NULL != output->next
	       && output->next->written < output->written) {
	  Output *n = output->next;
	  assert(n->prev == output);
	  pipe_list_remove(output, spillage);
	  pipe_list_insert(output, n, spillage);
	}

	if (output == spillage->blocking_output) {
	  spillage->blocking_output = spillage->pipe_list_head;
	}
      }
    }

    assert(output->offset <= curr_chunk->len);

    /* Check if at end of current Chunk */
    if (output->offset == curr_chunk->len) {
      /* Stop sending if no more Chunks yet */
      if (NULL == curr_chunk->next) break;

      /* Otherwise, move on to the next Chunk */
      output->curr_chunk = curr_chunk->next;
      output->offset = 0;

      --curr_chunk->nwriters;
      if (0 != release_chunk(chunks, curr_chunk, options, spillage)) {
	return -1;
      }

      curr_chunk = output->curr_chunk;
      curr_chunk->nwriters++;

      if (NULL == curr_chunk->data) {
	/* Need to re-read spilled data */
	if (0 != reread_data(curr_chunk, spillage)) return -1;
      }
    }
  }
  return bytes;
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
  ChunkList chunks = { NULL, NULL, NULL }; /* Linked list of Chunks */ 
  struct pollfd *polls; /* structs for poll(2) */
  int   *poll_idx;    /* indexes in outputs corresponding to entries in polls */
  int   *closing_pipes; /* Pipes that need to be closed */
  int   *closing_reg;   /* Regular files that need to be closed */
  int i, keeping_up = npipes, read_eof = 0, nclosed = 0;

  chunks.head = chunks.tail = calloc(1, sizeof(Chunk));  /* Initial Chunk */
  polls         = malloc((noutputs + 1) * sizeof(struct pollfd));
  poll_idx      = malloc((noutputs + 1) * sizeof(int));
  closing_pipes = malloc((npipes + 1)   * sizeof(int));
  closing_reg   = malloc((nregular + 1) * sizeof(int));
  if (NULL == chunks.head || NULL == polls || NULL == poll_idx
      || NULL == closing_pipes || NULL == closing_reg) {
    perror("do_copy");
    return -1;
  }

  chunks.head->ref_count = noutputs;
  chunks.head->nwriters  = noutputs;

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
	if (0 != do_read(options, in, spillage, &chunks, &read_eof,
			 noutputs - nclosed)) {
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
	    if (0 != do_read(options, in, spillage, &chunks,
			     &read_eof, noutputs - nclosed)) {
	      return -1;
	    }

	  } else {  /* Output, try to write to it. */
	    Output *output = &outputs[pipes[poll_idx[i]]];
	    ssize_t res = do_write(output, &chunks, options, spillage);

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
	    }
	  }
	}
      }
    } /* End of polling section */

    /* Deal with regular output files */

    for (i = 0; i < nregular; i++) {
      /* Try to write */
      if (do_write(&outputs[regular[i]], &chunks, options, spillage) < 0) {
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

    for (i = reg_close - 1; i >= 0; i--) {
      int to_close = regular[closing_reg[i]];

      assert(outputs[to_close].fd >= 0);
      if (0 != close(outputs[to_close].fd)) {
	fprintf(stderr, "Error closing %s : %s\n",
		outputs[to_close].name, strerror(errno));
	return -1;
      }
      outputs[to_close].fd = -1;

      /* Remove the entry from the regular array */
      if (closing_reg[i] < nregular - 1) {
	memmove(&regular[closing_reg[i]], &regular[closing_reg[i] + 1],
		(nregular - closing_reg[i] - 1) * sizeof(regular[0]));
      }

      if (0 != release_chunks(&chunks, outputs[to_close].curr_chunk,
			      options, spillage)) {
	return -1;
      }
      nclosed++;
      nregular--;
    }

    /* Close any poll-able files that have finished */

    for (i = pipe_close - 1; i >= 0; i--) {
      int to_close = pipes[closing_pipes[i]];

      assert(outputs[to_close].fd >= 0);
      if (0 != close(outputs[to_close].fd)) {
	fprintf(stderr, "Error closing %s : %s\n",
		outputs[to_close].name, strerror(errno));
	return -1;
      }
      outputs[to_close].fd = -1;

      /* Remove the entry from the pipes array */
      if (closing_pipes[i] < npipes - 1) {
	memmove(&pipes[closing_pipes[i]], &pipes[closing_pipes[i] + 1],
		(npipes - closing_pipes[i] - 1) * sizeof(pipes[0]));
      }

      /* Remove from spillage linked list */
      pipe_list_remove(&outputs[to_close], spillage);

      /* Release any data referenced by this output */
      if (0 != release_chunks(&chunks, outputs[to_close].curr_chunk,
			      options, spillage)) {
	return -1;
      }
      nclosed++;
      npipes--;
    }
  } while (nclosed < noutputs);
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

  while ((opt = getopt(argc, argv, "m:f:t:i:w:")) != -1) {
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

    case 't':
      options->tmp_dir = optarg;
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

int main(int argc, char** argv) {
  Opts    options;
  int     out_start = argc;
  Input   in = { 0, -1, 0 };
  Output *outputs = malloc((argc - 1) * sizeof(Output));
  int    *regular = malloc((argc - 1) * sizeof(int));
  int    *pipes   = malloc((argc - 1) * sizeof(int));
  SpillControl spillage = { NULL, 0, NULL, NULL, NULL }; /* Spillage info. */
  int     nregular = 0, npipes = 0;
  struct sigaction sig;

  if (NULL == outputs || NULL == regular || NULL == pipes) {
    perror("malloc");
    return EXIT_FAILURE;
  }

  /* Ignore SIGPIPEs, we want write(2) to get EPIPE instead. */
  sig.sa_handler = SIG_IGN;
  if (0 != sigaction(SIGPIPE, &sig, NULL)) {
    perror("sigaction");
    return EXIT_FAILURE;
  }

  if (0 != get_options(argc, argv, &options, &out_start)) {
    return EXIT_FAILURE;
  }

  if (0 != open_input(&options, &in, &spillage)) {
    return EXIT_FAILURE;
  }

  if (out_start >= argc) {
    fprintf(stderr, "No output files specified\n");
    return EXIT_FAILURE;
  }

  /* Open the output files */
  if (open_outputs(argc - out_start, argv + out_start, outputs,
		   regular, pipes, &nregular, &npipes) != 0) {
    return EXIT_FAILURE;
  }
  
  /* Copy input to all outputs */
  if (do_copy(&options, &in, argc - out_start, outputs,
	      nregular, regular, npipes, pipes, &spillage) != 0) {
    return EXIT_FAILURE;
  }

  return EXIT_SUCCESS;
}
