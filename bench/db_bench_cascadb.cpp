// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

// This file is copied from LevelDB and modifed a little 
// to add LevelDB style benchmark

#include <stdio.h>
#include <stdlib.h>
#include <iostream>
#include <string>
#include <vector>

#include "cascadb/db.h"
#include "sys/sys.h"
#include "util/logger.h"

#include "random.h"
#include "testutil.h"
#include "histogram.h"

using namespace std;
using namespace cascadb;

// Comma-separated list of operations to run in the specified order
//   Actual benchmarks:
//
//   fillseq       -- write N values in sequential key order in async mode
//   fillrandom    -- write N values in random key order in async mode
//   readseq       -- read N times sequentially
//   readrandom    -- read N times in random order
static const char* FLAGS_benchmarks =
    "fillseq,"
    "readrandom,"
    "readhot,"
    "fillrandom,"
    "readrandom,"
    "readhot,"
    ;

// Number of key/values to place in database
static size_t FLAGS_num = 1000000;

// Number of read operations to do.  If zero, do FLAGS_num reads.
static size_t FLAGS_reads = 0;

// Number of concurrent threads to run.
static int FLAGS_threads = 1;

// Size of each value
static size_t FLAGS_value_size = 100;

// Arrange to generate values that shrink to this fraction of
// their original size after compression
static double FLAGS_compression_ratio = 0.5;

// Print histogram of operation timings
static bool FLAGS_histogram = false;

// Number of bytes to use as a cache of uncompressed data.
// Zero means use default setings.
static size_t FLAGS_cache_size = 0;

// If true, do not destroy the existing database.  If you set this
// flag and also specify a benchmark that wants a fresh database, that
// benchmark will fail.
static bool FLAGS_use_existing_db = false;

// Use the db with the following name.
static const char* FLAGS_db = NULL;


// Helper for quickly generating random values.
class RandomGenerator {
 private:
  std::string data_;
  size_t pos_;

 public:
  RandomGenerator() {
    // We use a limited amount of data over and over again and ensure
    // that it is larger than the compression window (32K), and also
    // large enough to serve all typical value sizes we want to write.
    Random rnd(301);
    std::string piece;
    while (data_.size() < 1048576) {
      // Add a short fragment that is as compressible as specified
      // by compression_ratio.
      CompressibleSlice(&rnd, FLAGS_compression_ratio, 100U, &piece);
      data_.append(piece);
    }
    pos_ = 0;
  }

  Slice Generate(size_t len) {
    if (pos_ + len > data_.size()) {
      pos_ = 0;
      assert(len < data_.size());
    }
    pos_ += len;
    return Slice(data_.data() + pos_ - len, len);
  }
};

inline
static void DBSynchronize(DB* db)
{
  // Synchronize will flush writes to disk
  db->flush();
}

static Slice TrimSpace(Slice s) {
  size_t start = 0;
  while (start < s.size() && isspace(s[start])) {
    start++;
  }
  size_t limit = s.size();
  while (limit > start && isspace(s[limit-1])) {
    limit--;
  }
  return Slice(s.data() + start, limit - start);
}

static void AppendWithSpace(std::string* str, Slice msg) {
  if (msg.empty()) return;
  if (!str->empty()) {
    str->push_back(' ');
  }
  str->append(msg.data(), msg.size());
}

class Stats {
 private:
  double start_;
  double finish_;
  double seconds_;
  int done_;
  int next_report_;
  int64_t bytes_;
  double last_op_finish_;
  Histogram hist_;
  std::string message_;

 public:
  Stats() { Start(); }

  void Start() {
    next_report_ = 100;
    last_op_finish_ = start_;
    hist_.Clear();
    done_ = 0;
    bytes_ = 0;
    seconds_ = 0;
    start_ = now_micros();
    finish_ = start_;
    message_.clear();
  }

  void Merge(const Stats& other) {
    hist_.Merge(other.hist_);
    done_ += other.done_;
    bytes_ += other.bytes_;
    seconds_ += other.seconds_;
    if (other.start_ < start_) start_ = other.start_;
    if (other.finish_ > finish_) finish_ = other.finish_;

    // Just keep the messages from one thread
    if (message_.empty()) message_ = other.message_;
  }

  void Stop() {
    finish_ = now_micros();
    seconds_ = (finish_ - start_) * 1e-6;
  }

  void AddMessage(Slice msg) {
    AppendWithSpace(&message_, msg);
  }

  void FinishedSingleOp() {
    if (FLAGS_histogram) {
      double now = now_micros();
      double micros = now - last_op_finish_;
      hist_.Add(micros);
      if (micros > 20000) {
        fprintf(stderr, "long op: %.1f micros%30s\r", micros, "");
        fflush(stderr);
      }
      last_op_finish_ = now;
    }

    done_++;
    if (done_ >= next_report_) {
      if      (next_report_ < 1000)   next_report_ += 100;
      else if (next_report_ < 5000)   next_report_ += 500;
      else if (next_report_ < 10000)  next_report_ += 1000;
      else if (next_report_ < 50000)  next_report_ += 5000;
      else if (next_report_ < 100000) next_report_ += 10000;
      else if (next_report_ < 500000) next_report_ += 50000;
      else                            next_report_ += 100000;
      fprintf(stderr, "... finished %d ops%30s\r", done_, "");
      fflush(stderr);
    }
  }

  void AddBytes(int64_t n) {
    bytes_ += n;
  }

  void Report(const Slice& name) {
    // Pretend at least one op was done in case we are running a benchmark
    // that does not call FinishedSingleOp().
    if (done_ < 1) done_ = 1;

    double elapsed = (finish_ - start_) * 1e-6;

    std::string extra;
    if (bytes_ > 0) {
      // Rate is computed on actual elapsed time, not the sum of per-thread
      // elapsed times.
      char rate[100];
      snprintf(rate, sizeof(rate), "%6.1f MB/s",
               (bytes_ / 1048576.0) / elapsed);
      extra = rate;
    }
    AppendWithSpace(&extra, message_);

    //should computed on actual elapsed time too
    fprintf(stdout, "%-12s : %11.3f micros/op;%s%s\n",
            name.to_string().c_str(),
            elapsed * 1e6 / done_,
            (extra.empty() ? "" : " "),
            extra.c_str());
    if (FLAGS_histogram) {
      fprintf(stdout, "Microseconds per op:\n%s\n", hist_.ToString().c_str());
    }
    fflush(stdout);
  }
};

// State shared by all concurrent executions of the same benchmark.
struct SharedState {
  Mutex mu;
  CondVar cv;
  int total;

  // Each thread goes through the following states:
  //    (1) initializing
  //    (2) waiting for others to be initialized
  //    (3) running
  //    (4) done

  int num_initialized;
  int num_done;
  bool start;

  SharedState() : cv(&mu) { }
};

// Per-thread state for concurrent executions of the same benchmark.
struct ThreadState {
  int tid;             // 0..n-1 when running in n threads
  Random rand;         // Has different seeds for different threads
  Stats stats;
  SharedState* shared;

  ThreadState(int index)
      : tid(index),
        rand(1000 + index) {
  }
};

class Benchmark {
private:
  Directory *directory_;
  Comparator *comparator_;
  DB *db_;
  int db_num_;
  size_t num_;
  size_t reads_;
  double start_;
  double last_op_finish_;
  int64_t bytes_;
  std::string message_;
  Histogram hist_;
  RandomGenerator gen_;
  Random rand_;

  // State kept for progress messages
  int done_;
  int next_report_;     // When to report next

  void PrintHeader() {
    const int kKeySize = 16;
    PrintEnvironment();
    fprintf(stdout, "Keys:       %d bytes each\n", kKeySize);
    fprintf(stdout, "Values:     %ld bytes each (%ld bytes after compression)\n",
            FLAGS_value_size,
            static_cast<size_t>(FLAGS_value_size * FLAGS_compression_ratio + 0.5));
    fprintf(stdout, "Entries:    %ld\n", num_);
    fprintf(stdout, "RawSize:    %.1f MB (estimated)\n",
            ((static_cast<int64_t>(kKeySize + FLAGS_value_size) * num_)
             / 1048576.0));
#ifdef HAS_SNAPPY
    fprintf(stdout, "FileSize:   %.1f MB (estimated)\n",
            (((kKeySize + FLAGS_value_size * FLAGS_compression_ratio) * num_)
             / 1048576.0));
#else
    fprintf(stdout, "FileSize:   %.1f MB (estimated, compression disabled)\n",
            (((kKeySize + FLAGS_value_size) * num_)
             / 1048576.0));
#endif
    PrintWarnings();
    fprintf(stdout, "------------------------------------------------\n");
  }

  void PrintWarnings() {
#if defined(__GNUC__) && !defined(__OPTIMIZE__)
    fprintf(stdout,
            "WARNING: Optimization is disabled: benchmarks unnecessarily slow\n"
            );
#endif
#ifndef NDEBUG
    fprintf(stdout,
            "WARNING: Assertions are enabled; benchmarks unnecessarily slow\n");
#endif
#ifndef HAS_SNAPPY
    fprintf(stdout,
            "WARNING: Snappy compression is disabled\n");
#endif
#ifndef HAS_LIBAIO
    fprintf(stdout,
            "WARNING: Linux AIO is disabled, Posix AIO (simulate AIO with user threads) is used instead\n");
#endif
  }

  void PrintEnvironment() {
    fprintf(stderr, "CascaDB:    Alpha version\n");

#if defined(__linux)
    time_t now = time(NULL);
    fprintf(stderr, "Date:       %s", ctime(&now));  // ctime() adds newline

    FILE* cpuinfo = fopen("/proc/cpuinfo", "r");
    if (cpuinfo != NULL) {
      char line[1000];
      int num_cpus = 0;
      std::string cpu_type;
      std::string cache_size;
      while (fgets(line, sizeof(line), cpuinfo) != NULL) {
        const char* sep = strchr(line, ':');
        if (sep == NULL) {
          continue;
        }
        Slice key = TrimSpace(Slice(line, sep - 1 - line));
        Slice val = TrimSpace(Slice(sep + 1));
        if (key == "model name") {
          ++num_cpus;
          cpu_type = val.to_string();
        } else if (key == "cache size") {
          cache_size = val.to_string();
        }
      }
      fclose(cpuinfo);
      fprintf(stderr, "CPU:        %d * %s\n", num_cpus, cpu_type.c_str());
      fprintf(stderr, "CPUCache:   %s\n", cache_size.c_str());
    }
#endif
  }

 public:

  Benchmark()
  : directory_(create_fs_directory(FLAGS_db)),
    comparator_(new LexicalComparator()),
    db_(NULL),
    db_num_(0),
    num_(FLAGS_num),
    reads_(FLAGS_reads == 0 ? FLAGS_num : FLAGS_reads),
    bytes_(0),
    rand_(301) {
  }

  ~Benchmark() {
    delete db_;
    delete comparator_;
    delete directory_;
  }


  void Run() {
    PrintHeader();
    Open();

    int cnt = 0;

    const char* benchmarks = FLAGS_benchmarks;
    while (benchmarks != NULL) {
      const char* sep = strchr(benchmarks, ',');
      Slice name;
      if (sep == NULL) {
        name = benchmarks;
        benchmarks = NULL;
      } else {
        name = Slice(benchmarks, sep - benchmarks);
        benchmarks = sep + 1;
      }

      void (Benchmark::*method)(ThreadState*) = NULL;

      bool fresh_db = false;
      int num_threads = FLAGS_threads;

      if (name == Slice("fillseq")) {
        fresh_db = true;
        method = &Benchmark::WriteSeq;
      } else if (name == Slice("fillrandom")) {
        fresh_db = true;
        method = &Benchmark::WriteRandom;
      } else if (name == Slice("readseq")) {
        method = &Benchmark::ReadSequential;
      } else if (name == Slice("readrandom")) {
        method = &Benchmark::ReadRandom;
      } else if (name == Slice("readhot")) {
        method = &Benchmark::ReadHot;
      } else {
        if (name != Slice()) {  // No error message for empty name
          fprintf(stderr, "unknown benchmark '%s'\n", name.to_string().c_str());
        }
      }

      // Create new database if recreate is true
      if (fresh_db) {
        if (FLAGS_use_existing_db) {
          message_ = "skipping (--use_existing_db is true)";
          method = NULL;
        } else {
          if (cnt != 0) {
            delete db_;
            db_ = NULL;
            Open();
          }
        }
      }

      if (method) {
        RunBenchmark(num_threads, name, method);
        cnt ++;
      }
    }
  }

private:
  struct ThreadArg {
    Benchmark* bm;
    SharedState* shared;
    ThreadState* thread;
    void (Benchmark::*method)(ThreadState*);
  };

  static void* ThreadBody(void* v) {
    ThreadArg* arg = reinterpret_cast<ThreadArg*>(v);
    SharedState* shared = arg->shared;
    ThreadState* thread = arg->thread;
    {
      ScopedMutex l(&shared->mu);
      shared->num_initialized++;
      if (shared->num_initialized >= shared->total) {
        shared->cv.notify_all();
      }
      while (!shared->start) {
        shared->cv.wait();
      }
    }

    thread->stats.Start();
    (arg->bm->*(arg->method))(thread);
    thread->stats.Stop();

    {
      ScopedMutex l(&shared->mu);
      shared->num_done++;
      if (shared->num_done >= shared->total) {
        shared->cv.notify_all();
      }
    }
    return NULL;
  }

  void RunBenchmark(int n, Slice name,
                    void (Benchmark::*method)(ThreadState*)) {
    SharedState shared;
    shared.total = n;
    shared.num_initialized = 0;
    shared.num_done = 0;
    shared.start = false;

    ThreadArg* arg = new ThreadArg[n];
    for (int i = 0; i < n; i++) {
      arg[i].bm = this;
      arg[i].method = method;
      arg[i].shared = &shared;
      arg[i].thread = new ThreadState(i);
      arg[i].thread->shared = &shared;
      Thread *thr = new Thread(ThreadBody);
      thr->start(&arg[i]);
    }

    shared.mu.lock();
    while (shared.num_initialized < n) {
      shared.cv.wait();
    }

    shared.start = true;
    shared.cv.notify_all();
    while (shared.num_done < n) {
      shared.cv.wait();
    }
    shared.mu.unlock();

    for (int i = 1; i < n; i++) {
      arg[0].thread->stats.Merge(arg[i].thread->stats);
    }
    arg[0].thread->stats.Report(name);

    for (int i = 0; i < n; i++) {
      delete arg[i].thread;
    }
    delete[] arg;
  }

  void Open()
  {
    assert(db_ == NULL);

    Options opts;
    opts.dir = directory_;
    opts.comparator = comparator_;
#ifdef HAS_SNAPPY
    opts.compress = kSnappyCompress;
#endif
    if (FLAGS_cache_size) {
        opts.cache_limit = FLAGS_cache_size;
    }

    char file_name[100];
    db_num_++;
    snprintf(file_name, sizeof(file_name),
             "dbbench_cascadb-%d", db_num_);

    db_ = DB::open(file_name, opts);
    if (!db_) {
      fprintf(stderr, "open error %s\n", file_name);
      exit(1);
    }
  }

  void WriteSeq(ThreadState* thread)
  {
    Write(thread, false);
  }

  void WriteRandom(ThreadState* thread)
  {
    Write(thread, true);
  }

  void Write(ThreadState* thread, bool random)
  {
    int64_t bytes = 0;
    for (size_t i = 0; i < num_; i++ ) {
      uint64_t k = random ? rand() % FLAGS_num: i;
      char key[100];
      snprintf(key, sizeof(key), "%016ld", k);
      bytes += FLAGS_value_size + strlen(key); 

      if (!db_->put(key, gen_.Generate(FLAGS_value_size))) {
        fprintf(stderr, "put key %ld error\n", k);
      }
      thread->stats.FinishedSingleOp();
    }
    thread->stats.AddBytes(bytes);
  }

  void ReadSequential(ThreadState* thread) {
    int bytes = 0;
    Slice value;
    for (size_t i = 0; i < reads_; i++) {
      uint64_t k = i;
      char key[100];
      snprintf(key, sizeof(key), "%016ld", k);
      if (db_->get(key, value)) {
        bytes += value.size() + strlen(key);
        value.destroy();
      }
      thread->stats.FinishedSingleOp();
    }
    thread->stats.AddBytes(bytes);
  }

  void ReadRandom(ThreadState* thread) {
    int bytes = 0;
    Slice value;
    for (size_t i = 0; i < reads_; i++) {
      uint64_t k = rand() % FLAGS_num;
      char key[100];
      snprintf(key, sizeof(key), "%016ld", k);
      if (db_->get(key, value)) {
        bytes += value.size() + strlen(key);
        value.destroy();
      }
      thread->stats.FinishedSingleOp();
    }
    thread->stats.AddBytes(bytes);
  }

  void ReadHot(ThreadState* thread) {
    int bytes = 0;
    Slice value;
    uint64_t range = (FLAGS_num + 99) / 100;
    for (size_t i = 0; i < reads_; i++) {
      char key[100];
      uint64_t k = rand() % range;
      snprintf(key, sizeof(key), "%016ld", k);
      if (db_->get(key, value)) {
        bytes += value.size() + strlen(key);
        value.destroy();
      }
      thread->stats.FinishedSingleOp();
    }
    thread->stats.AddBytes(bytes);
  }
};


int main(int argc, char** argv)
{
    for (int i = 1; i < argc; i++) {
        double d;
        long n;
        char junk;
        if (strncmp(argv[i], "--benchmarks=", 13) == 0) {
          FLAGS_benchmarks = argv[i] + strlen("--benchmarks=");
        } else if (sscanf(argv[i], "--compression_ratio=%lf%c", &d, &junk) == 1) {
          FLAGS_compression_ratio = d;
        } else if (sscanf(argv[i], "--histogram=%ld%c", &n, &junk) == 1 &&
               (n == 0 || n == 1)) {
          FLAGS_histogram = n;
        } else if (sscanf(argv[i], "--use_existing_db=%ld%c", &n, &junk) == 1 &&
               (n == 0 || n == 1)) {
          FLAGS_use_existing_db = n;
        } else if(sscanf(argv[i], "--cache_size=%ld%c", &n, &junk) == 1) {
            FLAGS_cache_size = n;
        } else if(sscanf(argv[i], "--num=%ld%c", &n, &junk) == 1) {
            FLAGS_num = n;
        } else if (sscanf(argv[i], "--reads=%ld%c", &n, &junk) == 1) {
            FLAGS_reads = n;
        } else if (sscanf(argv[i], "--threads=%ld%c", &n, &junk) == 1) {
            FLAGS_threads = n;
        } else if (sscanf(argv[i], "--value_size=%ld%c", &n, &junk) == 1) {
            FLAGS_value_size = n;
        } else if(strncmp(argv[i], "--db=", 5) == 0) {
            FLAGS_db = argv[i] + 5;
        } else {
            cerr << "Invalid flag '" << argv[i] << "'" << endl;
            exit(1);
        }
    }

  // Choose a location for the test database if none given with --db=<path>
  if (FLAGS_db == NULL) {
      FLAGS_db = ".";
  }

  string logpath = FLAGS_db;
  logpath += "/cascadb.log";
  init_logger(logpath, kInfo);

  Benchmark benchmark;
  benchmark.Run();
  return 0;
}
