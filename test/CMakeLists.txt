file( GLOB_RECURSE SOURCE RELATIVE "${CMAKE_CURRENT_SOURCE_DIR}"
        "*.cpp"
        )
include_directories(
    ${PROJECT_SOURCE_DIR}/thirdparty/gtest/include
    ${PROJECT_SOURCE_DIR}/test
    )
set(GTEST_LIBS
    gtest_main
    gtest)
add_executable(
    t_cascadb
    ${SOURCE})
target_link_libraries(
    t_cascadb
    cascadbStatic
    ${CASCADB_LIBS}
    ${GTEST_LIBS})
