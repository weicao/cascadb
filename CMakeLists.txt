# author: Wei Cao <mingsong.cw@taobao.com>

CMAKE_MINIMUM_REQUIRED(VERSION 2.6)

PROJECT(cascadb)

set ( LIBRARY_OUTPUT_PATH  ${PROJECT_SOURCE_DIR}/lib)
set ( EXECUTABLE_OUTPUT_PATH ${PROJECT_SOURCE_DIR}/bin)

# set rpath

# use, i.e. don't skip the full RPATH for the build tree
SET(CMAKE_SKIP_BUILD_RPATH  FALSE)

# when building, don't use the install RPATH already
# (but later on when installing)
SET(CMAKE_BUILD_WITH_INSTALL_RPATH FALSE) 

# add the automatically determined parts of the RPATH
# which point to directories outside the build tree to the install RPATH
SET(CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)

# the RPATH to be used when installing, but only if it's not a system directory
LIST(FIND CMAKE_PLATFORM_IMPLICIT_LINK_DIRECTORIES "${CMAKE_INSTALL_PREFIX}/lib" isSystemDir)
IF("${isSystemDir}" STREQUAL "-1")
   SET(CMAKE_INSTALL_RPATH "${CMAKE_INSTALL_PREFIX}/lib")
ENDIF("${isSystemDir}" STREQUAL "-1")

add_definitions("-g -O2 -Wall")

include_directories(
${PROJECT_SOURCE_DIR}/include
${PROJECT_SOURCE_DIR}/src
)

link_directories(
${PROJECT_SOURCE_DIR}/lib
)

# check dependencies

# Check Snappy
include(${CMAKE_SOURCE_DIR}/cmake/FindSnappy.cmake)
if (SNAPPY_FOUND)
    message(STATUS "Find snappy include:${SNAPPY_INCLUDE_DIR} libs:${SNAPPY_LIBRARIES}")
    add_definitions("-DHAS_SNAPPY")
    include_directories(${SNAPPY_INCLUDE_DIR})
    link_libraries(${SNAPPY_LIBRARIES})
else (SNAPPY_FOUND)
    message(WARNING "Cannot find snappy, compression is disabled in cascadb")
endif (SNAPPY_FOUND)

# Check Libaio
include(${CMAKE_SOURCE_DIR}/cmake/FindLibaio.cmake)
if (LIBAIO_FOUND)
    message(STATUS "Find libaio include:${LIBAIO_INCLUDE_DIR} libs:${LIBAIO_LIBRARIES}")
    add_definitions("-DHAS_LIBAIO")
    include_directories(${LIBAIO_INCLUDE_DIR})
    link_libraries(${LIBAIO_LIBRARIES})
else (LIBAIO_FOUND)
    message(WARNING "Cannot find libaio, posix aio is used instead")
endif (LIBAIO_FOUND)

# environment

if (${CMAKE_SYSTEM_NAME} MATCHES "Linux")
    add_definitions("-DOS_LINUX")
endif (${CMAKE_SYSTEM_NAME} MATCHES "Linux")

set(CASCADB_LIBS
pthread m rt dl z
)

# Source files

MESSAGE(STATUS "Installation path is: ${CMAKE_INSTALL_PREFIX}   (overwrite with -DCMAKE_INSTALL_PREFIX=/your/path)")

install(DIRECTORY include/cascadb DESTINATION include)

add_subdirectory(src)
add_subdirectory(bench)
add_subdirectory(thirdparty)

enable_testing()
add_subdirectory(test)

