CascaDB
=======

Yet another write-optimized storage engine, using buffered B-tree algorithm inspired by TokuDB.

## Features
* Provides a key-value access API similar to LevelDB.
* Support Snappy compression.
* Support Direct IO and Linux AIO.


## Compile
CascaDB utilizes CMake to build, so first of all you should install CMake.

It's recommended to make out of source build, that is, object files are generated into a separated directory with the source directory.

        mkdir build
        cd build
        cmake ..
        make && make install

## Performance

Running bin/db_bench to get detailed performance report of how CascaDB running on your machine.

e.g. `bin/db_bench --num=1000000`, This will create and test on a database of a million records, each record has a 16 byte key, and a 100 byte value.

### Write Performance
        fillseq      :       1.020 micros/op;                 
        fillrandom   :       2.559 micros/op;                 

### Read Performance
Read benchmark results after sequential insertions

        readrandom   :       1.125 micros/op;                 
        readseq      :       1.282 micros/op;      

Read benchmark results after random insertions

        readrandom   :       3.227 micros/op;                 
        readseq      :       1.295 micros/op;                 

## Have a try!

1. Include the header file.

        #include <cascadb/db.h>

2. Open the database.

        Options opts;
        opts.dir = create_fs_directory("/tmp/db_bench");
        opts.comparator = new LexicalComparator();
        opts.compress = kSnappyCompress;

        const char *dbname = "example";
        DB *db = DB::open(dbname, opts);
        if (!db) {
          fprintf(stderr, "open error %s\n", dbname);
          exit(1);
        }

3. Insert record.

        if (!db->put("This is the key", "This is the value") {
            fprintf(stderr, "put error\n");
        }

4. Read record.

        Slice value;
        if (!db->get("This is the key", value) {
            fprintf(stderr, "get error\n");
        }
        fprintf(stdout, "the value is %s", value.to_string().c_str());

5. Delete record.

        if (!db->del("This is the key")) {
            fprintf(stderr, "del error\n");
        }

6. Destory database.

        delete db;
        delete opts.comparator;
        delete opts.dir;

## TODO
* Add forward and backward iteration over data.
* Support more compression methods.
* Implement write ahead log to ensure write operation is atomic.