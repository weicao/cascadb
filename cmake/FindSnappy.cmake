# - Try to find Snappy
# Once done, this will define
#
#  SNAPPY_FOUND - system has Snappy installed
#  SNAPPY_INCLUDE_DIRS - the Snappy include directories
#  SNAPPY_LIBRARIES - link these to use Snappy
#
# The user may wish to set, in the CMake GUI or otherwise, this variable:
#  SNAPPY_DIR - path to start searching for the module

find_path(SNAPPY_INCLUDE_DIR
    snappy.h
    HINTS
    PATH_SUFFIXES
    include
    )

#IF(WIN32)
#    SET(CMAKE_FIND_LIBRARY_SUFFIXES .lib .a ${CMAKE_FIND_LIBRARY_SUFFIXES})
#ELSE(WIN32)
#    SET(CMAKE_FIND_LIBRARY_SUFFIXES .a ${CMAKE_FIND_LIBRARY_SUFFIXES})
#ENDIF(WIN32)


find_library(SNAPPY_LIBRARY
    snappy 
    HINTS
    PATH_SUFFIXES
    lib
    )

mark_as_advanced(SNAPPY_INCLUDE_DIR SNAPPY_LIBRARY)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(Snappy
    DEFAULT_MSG
    SNAPPY_INCLUDE_DIR
    SNAPPY_LIBRARY)

if(SNAPPY_FOUND)
    set(SNAPPY_LIBRARIES "${SNAPPY_LIBRARY}") # Add any dependencies here
endif()

