add_executable(
    db_bench
    db_bench_cascadb.cpp
    testutil.cpp
    histogram.cpp)
target_link_libraries(
    db_bench
    cascadbShared
    ${CASCADB_LIBS}
    )

