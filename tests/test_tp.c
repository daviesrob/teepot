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
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <assert.h>
#include <poll.h>

#define MAX_READERS 2

typedef struct {
  int bytes[MAX_READERS];
} TestSegment;

typedef enum {
  IN_PIPE,      /* Make teepot read from a pipe */
  IN_SLOW_PIPE, /* As above, but don't allow the pipe to get too far ahead */
  IN_FILE       /* Read from a file */
} InputType;

typedef struct {
  size_t        in_len;         /* Total length to send */
  InputType     in_type;        /* What to read from */
  unsigned int  nfiles;         /* Number of files to output */
  unsigned int  nsegs;          /* Number of segments in the test schedule */
  unsigned int  npipes;         /* Number of pipes to output to */
  unsigned int  slow_pipe_rate; /* Bytes to write to the slow pipe in one go */
  TestSegment  *segs;           /* Schedule for reading from pipes */
} Test;

int chld_fds[2] = { -1, -1 };   /* File descriptors for SIGCHLD pipe */
int got_chld = 0;               /* Flag got a SIGCHLD */

#define BUFFSZ 65536            /* Size of input / output buffers */

/* Macro to generate test data */
#define TEST_BYTE(X) (((X) >> ((((X) & 3) << 3) + 2)) & 0xff)

/* SIGCHLD handler.
 * This sets the global got_chld flag, and writes a byte to the chld_fds pipe.
 * The other end of the pipe is monitored by the poll loop, so that it does
 * not get stuck when the child process ends.
 */

static void sig_handler(int signal) {
  ssize_t bytes;
  char c;

  switch (signal) {
  case SIGCHLD:
    if (got_chld) return; /* save writing repeatedly to the pipe */
    got_chld = 1;
    break;
  default:
    return;
  }

  if (chld_fds[1] < 0) return;
  c = '*';
  do {
    bytes = write(chld_fds[1], &c, 1);
  } while (bytes < 0 && errno == EINTR);
  if (bytes < 0 && (errno != EAGAIN && errno != EWOULDBLOCK)) {
    close(chld_fds[1]); /* Should get the attention of the other end... */
  }
}

/*
 * Set a file descriptor to non-blocking mode
 *
 * name is the name of the file (for error reporting)
 * fd   is the file descriptor
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
 * Drain the chld_fds pipe.  This ensures poll() won't flag it as readable
 * until a new SIGCHLD arrives.
 *
 * Returns  0 on success
 *         -1 on failure.
 */

static int drain_signal_pipe() {
  char c[256];
  ssize_t bytes;

  for (;;) {
    do {
      bytes = read(chld_fds[0], c, sizeof(c));
    } while (bytes < 0 && EINTR == errno);
    if (bytes < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) break;
    if (bytes < 0) {
      perror("Draining pipe");
      return -1;
    }
    if (0 == bytes) {
      fprintf(stderr, "Unexpected EOF from SIGGCHLD pipe\n");
      return -1;
    }
  }
  return 0;
}

/*
 * Generate test data and write it to a given file descriptor
 *
 * name     is the name of the file to write to (for error reporting)
 * fd       is the file descriptor to write to
 * len      is the length of the simulated file being written
 * *offset  is the offset into the simulated file
 * nonblock flags that gen_data should stop if the write would block
 * epipe_ok flags that the write may return EPIPE
 *
 * Returns the number of bytes written on success
 *         -1 on failure
 */

ssize_t gen_data(char *name, int fd, size_t len, size_t *offset,
		 int nonblock, int epipe_ok) {
  unsigned char buffer[BUFFSZ];
  ssize_t  total = 0;
  int      would_block = 0;
  ssize_t  to_send = len - *offset;

  while (*offset < len) {
    size_t l       = (len - *offset < BUFFSZ) ? len - *offset : BUFFSZ;
    size_t written = 0;
    size_t i, j;

    for (i = 0, j = *offset; i < l; i++, j++) buffer[i] = TEST_BYTE(j);

    while (written < l) {
      ssize_t  bytes;

      do {
	bytes = write(fd, buffer + written, l - written);
      } while (bytes < 0
	       && (EINTR == errno
		   ||(!nonblock && (EAGAIN == errno || EWOULDBLOCK == errno))));
      if (bytes < 0) {
	if (nonblock && (EAGAIN == errno || EWOULDBLOCK == errno)) {
	  would_block = 1;
	  break;
	}
	if (EPIPE == errno && epipe_ok) {
	  /* Pretend we wrote everything */
	  *offset = len;
	  return to_send;
	}
	fprintf(stderr, "Couldn't write to %s : %s\n", name, strerror(errno));
	return -1;
      }
      written += bytes;
    }

    total   += written;
    *offset += written;
    if (would_block) break;
  }

  return total;
}

/*
 * Reads data and checks that is matches the expected pattern
 *
 * fd is the file descriptor to read
 * len is the size of the simulated file
 * *offset is the offset into the simulated file
 * nonblock flags that read_data should stop if it would block
 * *eof_out is set to 1 if the read returned end of file.
 *
 * Returns the number of bytes read on success
 *         -1 on failure
 */

int read_data(int fd, size_t len, size_t *offset, int nonblock, int *eof_out) {
  unsigned char buffer[BUFFSZ];
  size_t  got    = 0;
  size_t  need   = len - *offset;
  ssize_t bytes;
  size_t  i, j;
  
  while (got < need) {
    /* Read some data */
    size_t to_get = (need - got < BUFFSZ) ? need - got : BUFFSZ;
    do {
      bytes = read(fd, buffer, to_get);
    } while (bytes < 0
	     && (EINTR == errno
		 ||(!nonblock && (EAGAIN == errno || EWOULDBLOCK == errno))));
    if (bytes < 0) {
      if (nonblock && (EAGAIN == errno || EWOULDBLOCK == errno)) {
	break;
      }
      perror("Reading data");
      return -1;
    }
    if (0 == bytes) {
      *eof_out = 1; /* Flag end of file */
      break;
    }

    /* Check what was read */
    for (i = 0, j = *offset; i < bytes; i++,j++) {
      if (buffer[i] != TEST_BYTE(j)) {
	fprintf(stderr,
		"Incorrect data read at offset %zd : expected %02x, got %02x\n",
		j, (unsigned int) TEST_BYTE(j), (unsigned int) buffer[i]);
	return -1;
      }
    }
    
    got     += bytes;
    *offset += bytes;
  }
  
  return got;
}

/*
 * Make named pipes for piped output tests.
 *
 * names[] are the names of the fifos that are created.
 *
 * Returns  0 on success
 *         -1 on failure
 */

int make_fifos(char *names[MAX_READERS]) {
  int i;
  char *name;

  /* Ensure names are all NULL to start with so cleaning up will work
     if the process fails part way through */
  for (i = 0; i < MAX_READERS; i++) names[i] = NULL;

  for (i = 0; i < MAX_READERS; i++) {
    /* Get a unique name */
    name = tmpnam(NULL);
    if (NULL == name) {
      perror("Getting unique name");
      return -1;
    }

    /* Copy into names[] array */
    names[i] = strdup(name);
    if (NULL == names[i]) {
      perror("Copying name");
      return -1;
    }

    /* Make the fifo */
    if (0 != mkfifo(name, 0600)) {
      free(names[i]); /* If it failed, remove the name that wasn't used */
      names[i] = NULL;
      if (EEXIST == errno) { /* Try again if the name already existed */
	i--;
	continue;
      }
      fprintf(stderr, "Couldn't make pipe %s : %s\n",
	      name, strerror(errno));
      return -1;
    }
    printf("# Made named pipe %s\n", name);
  }

  return 0;
}

/*
 * Remove files.
 *
 * names is the list of files to remove
 * num   is the number of files in the names array
 */

void remove_files(char **names, unsigned int num) {
  int i;

  for (i = 0; i < num; i++) {
    if (NULL == names[i]) continue;
    if (0 != unlink(names[i])) {
      fprintf(stderr, "Warning: Couldn't remove %s : %s\n",
	      names[i], strerror(errno));
    }
  }
}

/*
 * Create output files for teepot to write to
 *
 * test           is the current Test
 * *out_files_out is an array of output file names
 * *out_fds_out   is an array of output file descriptors
 *
 * Returns  0 on success
 *         -1 on failure
 */

int get_output_files(Test *test, char ***out_files_out, int **out_fds_out) {
  char **out_files = NULL;
  int   *out_fds   = NULL;
  char  *tmpsuff   = "testXXXXXX";
  size_t tmplen    = strlen(P_tmpdir) + strlen(tmpsuff) + 2;
  unsigned int i   = 0;
  
  /* Create the out_files and out_fds arrays */
  out_files = calloc(test->nfiles, sizeof(char *));
  if (NULL == out_files) {
    perror(NULL);
    goto fail;
  }

  out_fds = malloc(test->nfiles * sizeof(int));
  if (NULL == out_fds) {
    perror(NULL);
    goto fail;
  }
  for (i = 0; i < test->nfiles; i++) out_fds[i] = -1;

  for (i = 0; i < test->nfiles; i++) {
    /* Allocate space for the name */
    out_files[i] = malloc(tmplen);
    if (NULL == out_files[i]) {
      perror(NULL);
      goto fail;
    }

    /* Try to make the file */
    snprintf(out_files[i], tmplen, "%s/%s", P_tmpdir, tmpsuff);

    out_fds[i] = mkstemp(out_files[i]);
    if (-1 == out_fds[i]) {
      fprintf(stderr, "Couldn't make temporary file %s : %s\n",
	      out_files[i], strerror(errno));
      free(out_files[i]);  /* Remove unused name on failure */
      out_files[i] = NULL;
      goto fail;
    }
  }

  /* Pass back the array locations */
  *out_files_out = out_files;
  *out_fds_out   = out_fds;
  return 0;

 fail:
  /* Clean up */
  if (NULL != out_files) {
    unsigned int j;

    if (i > 0) remove_files(out_files, i);

    for (j = 0; j < i; j++) {
      free(out_files[j]);
    }

    free(out_files);
  }
  if (NULL != out_fds) free(out_fds);
  return -1;
}

/* 
 * Create a file for teepot to read
 * 
 * test is the current Test
 *
 * Returns the name of the file on success
 *         NULL on failure
 */

char * get_input_file(Test *test) {
  char  *tmpsuff   = "testXXXXXX";
  size_t tmplen    = strlen(P_tmpdir) + strlen(tmpsuff) + 2;
  char  *name      = malloc(tmplen);
  size_t offset    = 0;
  int    fd;

  if (NULL == name) {
    perror(NULL);
    return NULL;
  }

  /* Make a temporary file */
  snprintf(name, tmplen, "%s/%s", P_tmpdir, tmpsuff);
  
  fd = mkstemp(name);
  if (-1 == fd) {
    fprintf(stderr, "Couldn't make temporary file %s : %s\n",
	    name, strerror(errno));
    free(name);
    return NULL;
  }

  /* Fill it with test data */
  if (gen_data(name, fd, test->in_len, &offset, 0, 0) < 0) {
    remove_files(&name, 1);
    free(name);
    return NULL;
  }
  assert(offset == test->in_len);

  /* Close fd as no longer needed */
  if (0 != close(fd)) {
    fprintf(stderr, "Error closing temporary file %s : %s\n",
	    name, strerror(errno));
    remove_files(&name, 1);
    free(name);
    return NULL;
  }

  return name;
}

/* Number of extra slots to allocate for arguments */
#define ARGS_EXTRA 5    /* prog -i <in> ... NULL */

/*
 * Start the test program.
 *
 * prog is the command to run
 * test is the current Test
 * *in_fd is the parent end of the pipe to stdin (for pipe input)
 * in_file is the name of the input file (for file input)
 * fifos is an array containing the locations of the named pipes
 * out_files is an array containing the locations of the output files
 *
 * Returns the pid of the child process on success
 *         -1 on failure
 */

pid_t start_prog(int argc, char **argv, Test *test, int *in_fd,
		 char *in_file, char **fifos, char **out_files) {
  int pipefds[2];
  static char **args = NULL;
  static unsigned int nargs = 0;
  char *prog;
  pid_t pid;
  unsigned int i, arg = 0;

  /* Allocate space for the argument list */
  if (nargs < test->nfiles + test->npipes + ARGS_EXTRA + argc - 1) {
    nargs = test->nfiles + test->npipes + ARGS_EXTRA + argc - 1;
    args = realloc(args, nargs * sizeof(char *));
    if (NULL == args) {
      perror("Couldn't allocate space for argument list");
      return -1;
    }
  }

  /* copy argv */
  for (i = 0; i < argc; i++) {
    args[arg++] = argv[i];
  }
  prog = argv[0];

  if (test->in_type == IN_FILE) {
    /* File input, add -i filename to arguments */
    args[arg++] = "-i";
    args[arg++] = in_file;
  } else {
    /* For pipe input, make the pipe and set both ends nonblocking */
    if (0 != pipe(pipefds)) {
      perror("Couldn't make a pipe");
      return -1;
    }
    if (0 != setnonblock("pipe read end", pipefds[0])) return -1;
    if (0 != setnonblock("pipe write end", pipefds[1])) return -1;
  }

  /* Add output files and named pipes to the argument list */
  for (i = 0; i < test->nfiles; i++) args[arg++] = out_files[i];
  for (i = 0; i < test->npipes; i++) args[arg++] = fifos[i];
  args[arg++] = NULL;
  assert(arg <= nargs);

  printf("# Running: %s", prog);
  for (i = 1; i + 1 < arg; i++) printf(" %s", args[i]);
  printf("\n");

  /* Ensure the chld_fds pipe is empty and reset the got_chld flag*/
  if (0 != drain_signal_pipe()) return -1;
  got_chld = 0;

  /* Start the new process */
  pid = fork();
  if (pid < 0) {
    perror("Couldn't fork");
    return -1;
  }

  if (0 == pid) { /* Child */
    int in = fileno(stdin);
    if (test->in_type == IN_FILE) {
      /* Redirect stdin to /dev/null */
      int fd;
      close(in);
      fd = open("/dev/null", O_RDONLY);
      if (-1 == fd) {
	perror("Opening /dev/null");
	exit(EXIT_FAILURE);
      }
      if (fd != in) {
	if (-1 == dup2(fd, in)) {
	  perror("Redirecting stdin");
	  exit(EXIT_FAILURE);
	}
      }
    } else {
      /* Make stdin read from a pipe */
      close(pipefds[1]);
      if (-1 == dup2(pipefds[0], in)) {
	perror("Redirecting stdin");
	exit(EXIT_FAILURE);
      }
    }
    
    execv(prog, args);
    fprintf(stderr, "Couldn't exec %s : %s\n", prog, strerror(errno));
    exit(EXIT_FAILURE);
  } /* End of child code */

  /* Parent */
  if (test->in_type != IN_FILE) {
    close(pipefds[0]);
    *in_fd = pipefds[1];
  }

  printf("# Child PID = %u\n", (unsigned int) pid);

  return pid;
}

/*
 * Open named pipes for testing pipe outputs
 *
 * num      is the number of pipes to open
 * fifos    is an array holding the locations of the named pipes
 * fifo_fds is an array for the opened file descriptors
 *
 * Returns  0 on success
 *         -1 on failure
 */

int open_fifos(int num, char **fifos, int *fifo_fds) {
  int i;

  for (i = 0; i < num; i++) {
    fifo_fds[i] = open(fifos[i], O_RDONLY|O_NONBLOCK);
    if (fifo_fds[i] < 0) {
      fprintf(stderr, "Couldn't open named pipe %s : %s\n",
	      fifos[i], strerror(errno));
      return -1;
    }
  }
  return 0;
}

/*
 * Close named pipe file descriptors
 *
 * num   is the number of named pipes to close
 * fifos is an array of the locations of the named pipes (for error reporting)
 * fifo_fds is an array of the file descriptors for the named pipes
 *
 * Returns  0 on success
 *         -1 on failure
 */

int close_fifos(int num, char **fifos, int *fifo_fds) {
  int i;
  int res = 0;

  for (i = 0; i < num; i++) {
    if (fifo_fds[i] < 0) continue;  /* Already been closed */
    if (0 != close(fifo_fds[i])) {
      fprintf(stderr, "Error closing named pipe %s : %s\n",
	      fifos[i], strerror(errno));
      res = -1;
    }
    fifo_fds[i] = -1;
  }

  return res;
}

/*
 * Test if at the end of files by trying to read a byte from them.
 *
 * nfds  is the number of files to check
 * fds   is an array of file descriptors
 * names is an array of the file names
 *
 * Returns  0 on success
 *         -1 on failure or if a file was not at EOF
 */

int test_at_eof(int nfds, int *fds, char **names) {
  int i;
  ssize_t bytes;
  char byte;

  for (i = 0; i < nfds; i++) {
    if (fds[i] < 0) continue;   /* closed */
    /* Try reading a byte. */
    do {
      bytes = read(fds[i], &byte, 1);
    } while (bytes < 0
	     && (EINTR == errno || EAGAIN == errno || EWOULDBLOCK == errno));

    if (bytes < 0) { /* Error */
      fprintf(stderr, "(Looking for EOF) : error reading from %s : %s\n",
	      names[i], strerror(errno));
      return -1;
    }

    if (bytes != 0) { /* Got a byte, so not at EOF */
      fprintf(stderr, "Read past expected end of data on %s\n", names[i]);
      return -1;
    }
  }
  return 0;
}

/* Constants for the non-output file descriptors in poll_idx */
#define IDX_CHLDFD -2        /* SIGCHLD pipe */
#define IDX_IN_FD  -1        /* input pipe */

/*
 * Run the poll loop for tests that output to pipes.  For tests where teepot
 * reads from a pipe, this will generate data and feed it into in_fd.  It
 * will poll the output pipes and read data from them according to the
 * schedule given in the Test struct.  Some schedules call for outputs to
 * finish early (simpulating programs like head).  In those cases the
 * appropriate descriptor is closed.  The poll loop exits when either no
 * more outputs are left, or all the data has been read.  If there is
 * more data to send at the end, this is done by calling gen_data to ensure
 * that any files being written are completed.  Finally, all pipes are
 * checked to ensure that they are either closed or at end-of-file.
 *
 * Piped input can either be IN_PIPE or IN_SLOW_PIPE.  IN_PIPE writes as
 * much as it can each time round to simulate programs that generate data
 * quickly.  IN_SLOW_PIPE only writes a few bytes each time, and ensures
 * that the fastest output has caught up before writing any more.  This
 * simulates programs that generate data more slowly than the outputs can
 * be consumed.
 *
 * test  is the current Test
 * in_fd is the file descriptor for the input to teepot (if reading from pipe)
 * fifo_fds is an array of output file descriptors (output to pipes)
 * fifos    is an array of the locations of the named pipes in fifo_fds
 *
 * Returns  0 on success
 *         -1 on failure
 */

int run_poll_loop(Test *test, int in_fd, int *fifo_fds, char **fifos) {
  struct pollfd polls[MAX_READERS + 2];   /* 1 for in_fd, 1 for chld_fds[0] */
  int           poll_idx[MAX_READERS + 2];/* Which file is in each poll slot */
  /* in_fd should be polled */
  int           in_polled = test->in_type == IN_FILE ? 0 : 1;
  /* in_fd is a slow pipe */
  int           in_slow   = test->in_type == IN_SLOW_PIPE ? 1 : 0;
  size_t        fifo_offsets[MAX_READERS];  /* Offsets into the outputs */
  size_t        fifo_totals[MAX_READERS];   /* Target length for each output */
  unsigned int  seg_num;        /* Test segment counter */
  size_t        sent = 0;       /* number of bytes sent to in_fd so far */
  ssize_t       bytes;
  int           i;

  for (i = 0; i < test->npipes; i++) fifo_offsets[i] = fifo_totals[i] = 0;

  for (seg_num = 0; seg_num < test->nsegs; seg_num++) {
    TestSegment *seg = &test->segs[seg_num];  /* current test segment */
    int done = 0;

    /* Set targets for each output */
    for (i = 0; i < test->npipes; i++) {
      if (fifo_fds[i] >= 0 && seg->bytes[i] > 0) {
	fifo_totals[i] += seg->bytes[i];
      }
    }

    while (!done) {
      int npolls = 0, ready;
      size_t max_offset = 0;
      int noutputs = test->nfiles;  /* number of live outputs */

      /* Add SIGCHLD file descriptor to poll.  This is written to by the
	 signal handler.  It ensures that poll returns when the child exits.
	 This prevents a potential deadlock when poll waits forever on in_fd
	 as there is nothing reading from it any more.  */
      poll_idx[npolls] = IDX_CHLDFD;
      polls[npolls].fd = chld_fds[0];
      polls[npolls].events = POLLIN;
      polls[npolls++].revents = 0;

      /* Add outputs to poll.  Also count them and find out which has read
	 the most, for the slow pipe */
      for (i = 0; i < test->npipes; i++) {
	if (fifo_fds[i] < 0) continue;       /* Skip if already closed */
	noutputs++;
	if (max_offset < fifo_offsets[i]) max_offset = fifo_offsets[i];

	if (fifo_offsets[i] < fifo_totals[i] /* Poll if something to receive */
	    || seg->bytes[i] < 0) {          /* or about to close */
	  poll_idx[npolls] = i;
	  polls[npolls].fd        = fifo_fds[i];
	  polls[npolls].events    = POLLIN;
	  polls[npolls++].revents = 0;
	}
      }
      
      /* Add in_fd to poll if necessary */
      if (in_polled) {             /* Input is a pipe */
	if (sent < test->in_len) {    /* There is more data to send */
	  if (!in_slow || max_offset >= sent) {
	    /* Either a fast pipe, or one of the outputs has caught up */
	    poll_idx[npolls] = IDX_IN_FD;
	    polls[npolls].fd = in_fd;
	    polls[npolls].events = POLLOUT|POLLERR|POLLHUP;
	    polls[npolls++].revents = 0;
	  }
	} else {                      /* no more to send, close in_fd */
	  if (0 != close(in_fd)) {
	    perror("Closing pipe");
	    return -1;
	  }
	  in_polled = 0;              /* don't try to poll in_fd any more */
	}
      }
      
      /* Do the poll.  Wait forever unless we got a SIGCHLD, in which case we
	 are just draining any remaining data from the output pipes. */
      do {
	ready = poll(polls, npolls, got_chld ? 100 : -1);
      } while (ready < 0 && EINTR == errno);
      
      if (ready == 0 && got_chld) { /* Shouldn't happen */
	fprintf(stderr, "Child exited, but timed out waiting for data\n");
	return -1;
      }

      for (i = 0; i < npolls && ready > 0; i++) {
	if (0 == polls[i].revents) continue; /* Not ready */
	--ready;
	if (poll_idx[i] == IDX_IN_FD) { /* Can send more data */
	  /* Work out how much to send. For IN_PIPE, as much as possible.
	     For IN_SLOW_PIPE, a few bytes at a time. */
	  size_t l = (in_slow
		      ? (sent + test->slow_pipe_rate < test->in_len
			 ? sent + test->slow_pipe_rate : test->in_len)
		      : test->in_len);
	  assert(in_polled && polls[i].fd == in_fd);

	  /* Send some data */
	  bytes = gen_data("pipe", in_fd, l, &sent, 1, noutputs > 0 ? 0 : 1);
	  if (bytes < 0) {
	    fprintf(stderr, "Error sending data to test process : %s\n",
		    strerror(errno));
	    return -1;
	  }
	} else if (poll_idx[i] >= 0) { /* Can read data */
	  int polled = poll_idx[i];  /* Get the index of the ready output */
	  assert(fifo_fds[polled] == polls[i].fd);

	  if (seg->bytes[polled] < 0) {
	    /* Decided to close instead */
	    if (0 != close(fifo_fds[polled])) {
	      perror("Closing pipe");
	      return -1;
	    }
	    fifo_fds[polled] = -1; /* Mark as closed */
	    noutputs--;
	  } else {
	    /* Try to read */
	    int eof = 0;
	    bytes = read_data(fifo_fds[polled], fifo_totals[polled],
			      &fifo_offsets[polled], 1, &eof);
	    if (bytes < 0) return -1;
	    if (eof) {
	      fprintf(stderr, "Got unexpected EOF on fifo %s\n", fifos[polled]);
	      return -1;
	    }
	  }
	} else { /* Got a SIGCHLD */
	  assert(poll_idx[i] == IDX_CHLDFD);
	  /* Drain the pipe to prevent an endless poll loop */
	  drain_signal_pipe();
	}
      }

      /* Are we there yet? */
      done = 1;
      for (i = 0; i < test->npipes; i++) {
	if ((fifo_fds[i] >= 0 && fifo_offsets[i] < fifo_totals[i])
	    || (seg->bytes[i] < 0 && fifo_fds[i] >= 0)) {
	  done = 0;
	  break;
	}
      }
    } /* End of poll loop */
  } /* End of seg loop */

  if (in_polled) {
    /* Try to push out any remaining data */
    if (sent < test->in_len) {
      size_t left = test->in_len - sent;
      fprintf(stderr, "Pushing ...\n");
      if (gen_data("pipe", in_fd, test->in_len, &sent, 0,
		   test->nfiles == 0 ? 1 : 0) != left) {
	fprintf(stderr, "gen_data returned error\n");
	return -1;
      }
    }

    /* Ensure in_fd has been closed */
    if (0 != close(in_fd)) {
      perror("Closing pipe");
      return -1;
    }
  }

  /* By now, any unclosed pipes should be at EOF, so test for that */

  return test_at_eof(test->npipes, fifo_fds, fifos);
}

/*
 * Wait for a child process to end, and check the exit status.
 * 
 * child is the PID to wait for.
 *
 * Returns  0 on success
 *         -1 if wait() failed or the exit status was non-zero
 *        -99 it wait() returned the wrong PID
 */

int wait_child(pid_t child) {
  pid_t pid;
  int   status;

  pid = wait(&status);
  if (pid < 0) {
    perror("Waiting on child process");
    return -1;
  }
  if (pid != child) {
    fprintf(stderr, "Unexpected return value from wait, expected %d, got %d\n",
	    child, pid);
    return -99;
  }
  if (WIFEXITED(status)) {
    int code = WEXITSTATUS(status);
    if (code != 0) {
      fprintf(stderr, "Child exited with code %d\n", code);
      return -1;
    }
  } else {
    fprintf(stderr, "Child exited via signal %d\n", WTERMSIG(status));
    return -1;
  }

  return 0;
}

/*
 * Check output files to ensure they contain the expected data.
 *
 * test      is the current Test
 * out_fds   is an array of the descriptors for the files
 * out_files is an array containing the output file names
 *
 * Returns  0 if the files were all correct.
 *         -1 if they were not, or an error occurred while checking.
 */

int check_output_files(Test *test, int *out_fds, char **out_files) {
  int i;
  int res = 0;
  
  for (i = 0; i < test->nfiles; i++) {
    int eof = 0;
    size_t offset = 0;

    /* Seek back to the start */
    if (-1 == lseek(out_fds[i], 0, SEEK_SET)) {
      fprintf(stderr, "Couldn't seek on %s : %s\n",
	      out_files[i], strerror(errno));
      res =  -1;
      continue;
    }

    /* Check the contents */
    if (read_data(out_fds[i], test->in_len, &offset, 0, &eof) < 0) {
      res =  -1;
      continue;
    }

    if (eof) {
      fprintf(stderr, "Got EOF on %s after %ld bytes (expected %ld)\n",
	      out_files[i], (long) offset, (long) test->in_len);
      res = -1;
      continue;
    }

    if (offset != test->in_len) {
      fprintf(stderr,
	      "Didn't read expected number of bytes from %s. "
	      "Got %ld expected %ld",
	      out_files[i], (long) offset, (long) test->in_len);
      res = -1;
      continue;
    }
  }

  /* Check that we are now at EOF for all the files, i.e. nothing unexpected
     got tagged on the end */
  if (!res) {
    res = test_at_eof(test->nfiles, out_fds, out_files);
  }

  /* Close all the file descriptors */
  for (i = 0; i < test->nfiles; i++) {
    if (0 != close(out_fds[i])) {
      fprintf(stderr, "Error closing %s : %s\n", out_files[i], strerror(errno));
      res = -1;
    }
  }

  return res;
}

/*
 * Log some information about the test.
 */

void print_test_info(int test_num, Test *test) {
  const char * test_types[3] = { "pipe", "slow pipe", "file" };

  printf("# Test %d : Input %s; %zd bytes; %u files; %u pipes\n",
	 test_num, test_types[test->in_type],
	 test->in_len, test->nfiles, test->npipes);
  if (test->nsegs) {
    unsigned int i, j;
    printf("# Reading schedule:\n");
    for (i = 0; i < test->nsegs; i++) {
      printf("# ");
      for (j = 0; j < test->npipes; j++) {
	printf(" %10d", test->segs[i].bytes[j]);
      }
      printf("\n");
    }
  }
}

/*
 * Return a string with a short summary of the current test.  The returned
 * string is a static buffer, so it will be replaced on the next call.
 *
 * test is the current Test
 *
 * Returns a pointer to the buffer.
 */

char * summarize_test(Test *test) {
  static char buffer[256];
  const char * test_types[3] = { "pipe", "slow pipe", "file" };

  snprintf(buffer, sizeof(buffer),
	   "Input %s; %zd bytes; %u files; %u pipes",
	   test_types[test->in_type],
	   test->in_len, test->nfiles, test->npipes);
  return buffer;
}

/*
 * Run a single test.
 *
 * prog     is the program to run
 * test_num is the test number
 * test     is the current Test
 * fifos    is an array containing the locations of the named pipes
 *
 * Returns  0 if the test was successful.
 *         -1 if the test failed
 *         99 if some other failure occurred while trying to run the test
 */

int run_test(int argc, char **argv,
	     unsigned int test_num, Test *test, char **fifos) {
  char  *in_file   = NULL;
  int    in_fd     = -1;
  char **out_files = NULL;
  int   *out_fds   = NULL;
  int    fifo_fds[MAX_READERS];
  pid_t  child     = 0;
  int i, res = 0, wres;

  /* Write some information about the test to the log */
  print_test_info(test_num, test);

  assert(test->npipes <= MAX_READERS);

  for (i = 0; i < MAX_READERS; i++) fifo_fds[i] = -1;

  /* Open any output files needed */
  if (test->nfiles > 0) {
    if (0 != get_output_files(test, &out_files, &out_fds)) goto BAIL;
  }

  /* Create an input file if the test needs one */
  if (test->in_type == IN_FILE) {
    if (NULL == (in_file = get_input_file(test))) goto BAIL;
  }

  /* Start the program under test */
  child = start_prog(argc, argv, test, &in_fd, in_file, fifos, out_files);
  if (child < 0) goto BAIL;

  /* Open any named pipes required */
  if (test->npipes > 0) {
    if (0 != open_fifos(test->npipes, fifos, fifo_fds)) goto BAIL;
  }

  if (test->npipes) {
    /* reading from pipes, run the full poll loop */
    res = run_poll_loop(test, in_fd, fifo_fds, fifos);
    /* Close any pipes that are still open */
    if (0 != close_fifos(test->npipes, fifos, fifo_fds)) {
      res = -1;
    }
  } else if (test->in_type == IN_PIPE || test->in_type == IN_SLOW_PIPE) {
    /* only sending to a pipe, so just shove the data down it */
    size_t offset = 0;
    if (gen_data("pipe", in_fd, test->in_len, &offset, 0, 0) != test->in_len) {
      res = -1;
    }
    if (0 != close(in_fd)) {
      perror("Closing pipe");
      res = -1;
    }
  }

  /* Wait for the program to finish and check the exit status */
  wres = wait_child(child);
  if (-99 == wres) goto BAIL;
  if (wres < 0) res = -1;


  /* Check the contents of any regular files that were created */
  if (test->nfiles > 0) {
    if (0 != check_output_files(test, out_fds, out_files)) res = -1;
  }

  /* Clean up */
  if (NULL != in_file) remove_files(&in_file, 1);
  if (test->nfiles > 0) {
    remove_files(out_files, test->nfiles);
  }
  return res;

 BAIL:
  /* Something went wrong, clean up and return 99 */
  if (NULL != in_file) remove_files(&in_file, 1);
  if (test->nfiles > 0 && NULL != out_files) {
    remove_files(out_files, test->nfiles);
  }
  if (test->npipes > 0) {
    close_fifos(test->npipes, fifos, fifo_fds);
  }
  return 99;
}

/*
 * Run a set of tests
 *
 * prog is the program to test
 *
 * Returns  0 if all tests succeed
 *          1 if one or more tests fail
 *         99 if a fatal error occured, so not all tests were run
 */

int run_tests(int argc, char **argv) {
  char *fifos[MAX_READERS] = { NULL };
  /* The test schedule */
  TestSegment test_segs1[1] = {{{ 100 }}};
  TestSegment test_segs2[1] = {{{ 100, 100 }}};
  TestSegment test_segs3[2] = {{{ 100, 0 }}, {{ 0, 100 }}};
  TestSegment test_segs4[1] = {{{ 100, -1 }}};
  TestSegment test_segs5[2] = {{{ 50, 50 }}, {{ -1, -1 }}};
  TestSegment test_segs6[2] = {{{ 100, 100 }}, {{ -1, 900 }}};
  TestSegment test_segs7[20] = {{{1000000, 0}}, {{0, 1000000}},
				{{1000000, 0}}, {{0, 1000000}},
				{{1000000, 0}}, {{0, 1000000}},
				{{1000000, 0}}, {{0, 1000000}},
				{{1000000, 0}}, {{0, 1000000}},
				{{1000000, 0}}, {{0, 1000000}},
				{{1000000, 0}}, {{0, 1000000}},
				{{1000000, 0}}, {{0, 1000000}},
				{{1000000, 0}}, {{0, 1000000}},
				{{1000000, 0}}, {{0, 1000000}}};
  TestSegment test_segs8[3] = {{{1000000, 1000000}},
			       {{8000000, -1}},
			       {{1000000, -1}}};
  TestSegment test_segs9[2] = {{{10000000, 0}}, {{0, 10000000}}};
  TestSegment test_segs10[2] = {{{1000000, 100}}, {{-1, -1}}};
  Test tests[] = {{ 100, IN_PIPE, 1, 0, 0, 1, NULL },
		  { 100, IN_PIPE, 2, 0, 0, 1, NULL },
		  { 100, IN_PIPE, 0, 1, 1, 1, test_segs1 },
		  { 100, IN_PIPE, 0, 1, 2, 1, test_segs2 },
		  { 100, IN_PIPE, 0, 2, 2, 1, test_segs3 },
		  { 100, IN_PIPE, 0, 1, 2, 1, test_segs4 },
		  { 100, IN_PIPE, 0, 2, 2, 1, test_segs5 },
		  { 1000, IN_PIPE, 0, 2, 2, 7, test_segs6 },
		  { 10000000, IN_PIPE, 0, 20, 2, 167, test_segs7 },
		  { 10000000, IN_PIPE, 0, 20, 2, 167, test_segs7 },
		  { 10000000, IN_PIPE, 0, 3, 2, 167, test_segs8 },
		  { 10000000, IN_PIPE, 0, 2, 2, 167, test_segs9 },
		  { 10000000, IN_PIPE, 0, 2, 2, 167, test_segs10 },
		  { 10000000, IN_PIPE, 1, 2, 2, 167, test_segs10 },
  };
  unsigned int ntests = sizeof(tests) / sizeof(tests[0]);
  unsigned int i, res, failed = 0, test_num = 0;

  /* Create the named pipes */
  if (0 != make_fifos(fifos)) {
    res = 99;
    goto BAIL;
  }

  /* Print the expected number of tests for the driver */
  printf("1..%u\n", ntests * 3);
  
  /* Run all the tests twice, once reading a pipe and once reading a file */
  for (i = 0; i < ntests; i++) {
    Test t = tests[i];
    for (t.in_type = IN_PIPE; t.in_type <= IN_FILE; t.in_type++) {
      res = run_test(argc, argv, ++test_num, &t, fifos);
      /* Print the result to the driver */
      printf("%s %d - %s\n", res ? "not ok" : "ok",
	     test_num, summarize_test(&t));
      failed |= res;
      if (res == 99) break;
    }
    if (res == 99) break;
  }

 BAIL:
  if (res == 99) {
    /* Fatal error, tell the driver we gave up */
    printf("Bail out!\n");
  }

  /* Clean up and return the overall result */
  remove_files(fifos, MAX_READERS);
  return failed;
}

int main(int argc, char **argv) {
  struct sigaction sigact;

  /* Make stdout line buffered */
  if (0 != setvbuf(stdout, NULL, _IOLBF, 0)) {
    perror("Setting stdout to line buffered mode");
    return 99;
  }

  if (argc < 2) {
    fprintf(stderr, "Usage: %s <prog>\n", argv[0]);
    return 99;
  }

  /* Set up pipe for SIGCHLD */
  if (0 != pipe(chld_fds)) {
    perror("Making pipe");
    return 99;
  }
  if (0 != setnonblock("child pipe read end", chld_fds[0])
      || 0 != setnonblock("child pipe write end", chld_fds[1])) {
    return 99;
  }

  /* Set up signal handlers */
  sigact.sa_handler = sig_handler;
  sigemptyset(&sigact.sa_mask);
  sigact.sa_flags = SA_NOCLDSTOP;
  if (0 != sigaction(SIGCHLD, &sigact, NULL)) {
    perror("Setting up SIGCHLD handler");
    return 99;
  }

  sigact.sa_handler = SIG_IGN;
  sigact.sa_flags = 0;
  if (0 != sigaction(SIGPIPE, &sigact, NULL)) {
    perror("Setting SIGPIPE to ignore");
    return EXIT_FAILURE;
  }

  /* Run the tests */
  if (run_tests(argc - 1, argv + 1) != 0) return 1;
  return 0;
}
