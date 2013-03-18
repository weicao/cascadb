CascaDB
=======

Yet another write-optimized storage engine, using buffered B-tree algorithm inspired by TokuDB.

Currently CascaDB provides a key-value read/write API similar to LevelDB.


Compile
-------
CascaDB utilizes CMake to build, so first of all you should install CMake.

It's recommended to make out of source build, that is, object files are generated into a separated directory with the source directory.

mkdir build
cd build
cmake ..
make && make install