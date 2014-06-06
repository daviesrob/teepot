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

/* Options */
typedef struct {
  size_t      max;      /* Maximum amount of data to keep in memory */
  off_t       file_max; /* Maximum size of each temp file */
  const char *tmp_dir;  /* Where to write spilled data */
  const char *in_name;  /* Name of input file */
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

/* Temporary file */
typedef struct TmpFile {
  struct TmpFile *next;
  off_t           offset;
  size_t          ref_count;
  int             fd;
} TmpFile;

/* A chunk of input */

typedef struct Chunk {
  unsigned char *data;
  size_t   len;
  size_t   ref_count;
  off_t    tmp_offset;
  TmpFile *tmp;
  struct Chunk *next;
} Chunk;

/* Chunk size to use */
#define CHUNK_SZ (1024*1024)

/* The input */
typedef struct {
  int      fd;
  int      reg;
  TmpFile *tmp;
} Input;

/* An output */

typedef struct {
  char  *name;
  int    fd;
  int    is_reg;
  Chunk *curr_chunk;
  size_t offset;
} Output;


/*
 * Check if a file descriptor points to a regular file, i.e. not poll-able
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
 * Open the input file and set up the Input struct
 *
 * *options  is the Opt struct with the input file name
 * *in       is the Input struct to fill in
 *
 * Returns  0 on success
 *         -1 on failure
 */

int open_input(Opts *options, Input *in) {
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
  in->tmp = NULL;

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
    }

    /* Initialize the pointer to the data to output */
    outputs[i].curr_chunk = NULL;
    outputs[i].offset = 0;
  }
  return 0;
}

/* 
 * Read some data into the tail Chunk.  If it's full, make a new chunk
 * first and read into that.  If the end of the input file is read,
 * *read_eof is set to true.
 *
 * *in_name  is the name of the input file
 * in_fd     is the descriptor to read
 * **tail_p  is the current tail Chunk
 * *read_eof is the end-of-file flag
 * nrefs     is the reference count to set on new Chunks.
 *
 * Returns  0 on success
 *         -1 in failure
 */

static ssize_t do_read(Opts *options, Input *in,
		       Chunk **tail_p, int *read_eof, int nrefs) {
  Chunk *tail = *tail_p;
  ssize_t bytes;

  if (tail->len == CHUNK_SZ) {
    /* Need to start a new Chunk */
    Chunk *new_tail = calloc(1, sizeof(Chunk));
    if (NULL == new_tail) {
      perror("do_read");
      return -1;
    }

    /* Initialize and make it the new tail */
    new_tail->ref_count = nrefs;
    tail->next = new_tail;
    tail = *tail_p = new_tail;
  }

  if (NULL == tail->data) {
    /* Allocate a buffer to put the data in */
    tail->data = malloc(CHUNK_SZ);
    if (NULL == tail->data) {
      perror("do_read");
      return -1;
    }
  }

  /* Read some data */ 
  do {
    bytes = read(in->fd, tail->data + tail->len, CHUNK_SZ - tail->len);
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

  /* Got some data, update length */
  tail->len += bytes;

  return 0;
}

/*
 * Write some data.
 *
 * *output is the Output struct for the file to write
 * nclosed is the number of files that have been closed so far
 *
 * Returns the number of bytes written (>= 0) on success
 *         -1 on failure (not EPIPE)
 *         -2 on EPIPE
 */

static ssize_t do_write(Output *output, int nclosed) {
  ssize_t bytes = 0;
  Chunk *curr_chunk = output->curr_chunk;

  while (curr_chunk->next != NULL || output->offset < curr_chunk->len) {
    /* While there's somethign to write ... */

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
      output->offset += b;
      bytes += b;
    }

    assert(output->offset <= curr_chunk->len);

    /* Check if at end of current Chunk */
    if (output->offset == curr_chunk->len) {
      /* Stop sending if no more Chunks yet */
      if (NULL == curr_chunk->next) break;

      /* Otherwise, move on to the next Chunk */
      output->curr_chunk = curr_chunk->next;
      output->offset = 0;

      if (--curr_chunk->ref_count <= nclosed) {
	/* If no more readers for the current Chunk, free it */
	free(curr_chunk->data);
	free(curr_chunk);
      }

      curr_chunk = output->curr_chunk;
    }
  }
  return bytes;
}

/*
 * Do the copies from in_fd to all the outputs.
 *
 * in_name   is the name of the input file.
 * in_fd     is the file descriptor for the input file.
 * in_reg    is a flag showing if the input is a regular file.
 * noutputs  is the number of entries in the outputs array
 * outputs   is the array of Output structs.
 * nregular  is the number of entries in the regular array
 * regular   is an array of indexes into outputs of the regular output files
 * npipes    is the number of entries in the pipes array
 * pipes     is an array of indexes into outputs of the poll-able output files
 *
 * Returns  0 on success
 *         -1 on failure
 */

static int do_copy(Opts *options, Input *in,
		   int noutputs, Output *outputs,
		   int nregular, int *regular,
		   int npipes, int *pipes) {
  Chunk *tail;          /* tail Chunk (the one currently being written to) */
  struct pollfd *polls; /* structs for poll(2) */
  int   *poll_idx;    /* indexes in outputs corresponding to entries in polls */
  int   *closing_pipes; /* Pipes that need to be closed */
  int   *closing_reg;   /* Regular files that need to be closed */
  int i, keeping_up = npipes, read_eof = 0, nclosed = 0;

  tail          = calloc(1, sizeof(Chunk));  /* Initial empty Chunk */
  polls         = malloc((noutputs + 1) * sizeof(struct pollfd));
  poll_idx      = malloc((noutputs + 1) * sizeof(int));
  closing_pipes = malloc((npipes + 1)   * sizeof(int));
  closing_reg   = malloc((nregular + 1) * sizeof(int));
  if (NULL == tail || NULL == polls || NULL == poll_idx
      || NULL == closing_pipes || NULL == closing_reg) {
    perror("do_copy");
    return -1;
  }

  tail->ref_count = noutputs;

  /* Point all outputs to the initial Chunk */
  for (i = 0; i < noutputs; i++) {
    outputs[i].curr_chunk = tail;
  }

  do {  /* Main loop */
    int npolls = 0, pipe_close = 0, reg_close = 0;
    /* Only try to read if not at EOF, and either there are no 
       pipes or at least one pipe has nothing left to write. */
    int should_read = !read_eof && (npipes == 0 || keeping_up > 0);

    if (should_read) {
      if (in->reg) {
	/* If reading a regular file, do it now */
	if (0 != do_read(options, in, &tail, &read_eof, noutputs)) return -1;
      } else {
	/* Otherwise add it to the poll list */
	polls[npolls].fd = in->fd;
	poll_idx[npolls] = -1;
	polls[npolls].events = POLLIN;
	polls[npolls++].revents = 0;
      }
    }

    /* Add all the pipe outputs that have something to write to the poll list */
    for (i = 0; i < npipes; i++) {
      if (outputs[pipes[i]].curr_chunk != tail
	  || outputs[pipes[i]].offset < tail->len
	  || read_eof) { /* always after read_eof so we finish */
	polls[npolls].fd = outputs[pipes[i]].fd;
	poll_idx[npolls] = i;
	polls[npolls].events = POLLOUT|POLLERR|POLLHUP;
	polls[npolls++].revents = 0;
      }
    }
    
    keeping_up = 0;  /* Number of pipes that are keeping up */
    if (npolls > 0) {  /* Need to do some polling */
      int ready;
      do {
	ready = poll(polls, npolls, -1);
      } while (ready < 0 && EINTR == errno);

      if (ready < 0) {
	perror("poll failed in do_copy");
	return -1;
      }

      for (i = 0; i < npolls && ready > 0; i++) {
	if (polls[i].revents) {  /* Got some events */
	  
	  --ready;
	  if (poll_idx[i] < 0) {  /* Input, try to read from it. */
	    if (0 != do_read(options, in, &tail, &read_eof, noutputs)) {
	      return -1;
	    }

	  } else {  /* Output, try to write to it. */
	    Output *output = &outputs[pipes[poll_idx[i]]];
	    ssize_t res = do_write(output, nclosed);

	    if (-2 == res) { /* Got EPIPE, add to closing_pipes list */
	      closing_pipes[pipe_close++] = poll_idx[i];
	      continue;
	    } else if (res < 0) { /* other write error, give up */
	      return -1;
	    }

	    if (output->curr_chunk == tail && output->offset == tail->len) {
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
      if (do_write(&outputs[regular[i]], nclosed) < 0) return -1;

      if (read_eof
	  && outputs[regular[i]].curr_chunk == tail
	  && outputs[regular[i]].offset == tail->len) {
	/* If all data written and finished reading, add to closing_reg list */
	closing_reg[reg_close++] = i;
      }
    }

    /* Close any regular files that have finished */

    for (i = 0; i < reg_close; i++) {
      int to_close = regular[closing_reg[i]];

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
      nclosed++;
      nregular--;
    }

    /* Close any poll-able files that have finished */

    for (i = 0; i < pipe_close; i++) {
      int to_close = pipes[closing_pipes[i]];

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
      nclosed++;
      npipes--;
    }
  } while (!read_eof && nclosed < noutputs);
  return 0;
}

void show_usage(char *prog) {
  fprintf(stderr, "Usage: %s [-m <limit>] [-f <limit>] [-t temp_dir]\n", prog);
}

/*
 * Parse a size option.  This is a number followed optionally by one of
 * the letters [TtGgMmKk].  The number is scaled as suggested by the suffix
 * letter.
 *
 * *in is the input string
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
  return 0;
}

int get_options(int argc, char** argv, Opts *options, int *after_opts) {
  int opt;
  size_t sz;

  options->max      = DEFAULT_MAX;
  options->file_max = DEFAULT_FILE_MAX;
  options->tmp_dir  = DEFAULT_TMP_DIR;
  options->in_name  = NULL;

  while ((opt = getopt(argc, argv, "m:f:t:i:")) != -1) {
    switch (opt) {

    case 'i':
      options->in_name = optarg;
      break;

    case 'f':
      if (0 != parse_size_opt(optarg, &sz)) {
	fprintf(stderr, "Couldn't understand file size limit '%s'\n", optarg);
	return -1;
      }
      options->file_max = sz;
      break;

    case 'm':
      if (0 != parse_size_opt(optarg, &sz)) {
	fprintf(stderr, "Couldn't understand memory limit '%s'\n", optarg);
	return -1;
      }
      options->max = sz;
      break;

    case 't':
      options->tmp_dir = optarg;
      break;

    default:
      show_usage(argv[0]);
      return -1;
    }
  }

  *after_opts = optind;
  return 0;
}

int main(int argc, char** argv) {
  Opts    options;
  int     out_start = argc;
  Input   in = { -1, 0, NULL };
  Output *outputs = malloc((argc - 1) * sizeof(Output));
  int    *regular = malloc((argc - 1) * sizeof(int));
  int    *pipes   = malloc((argc - 1) * sizeof(int));
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

  if (0 != open_input(&options, &in)) {
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
	      nregular, regular, npipes, pipes) != 0) {
    return EXIT_FAILURE;
  }

  return EXIT_SUCCESS;
}
