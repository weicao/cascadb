file( GLOB_RECURSE SOURCE RELATIVE "${CMAKE_CURRENT_SOURCE_DIR}"
        "*.cpp"
		)
		
add_library(cascadbShared SHARED ${SOURCE})
target_link_libraries(cascadbShared ${CASCADB_LIBS})
set_target_properties(cascadbShared PROPERTIES OUTPUT_NAME cascadb)

add_library(cascadbStatic STATIC ${SOURCE})

if(WIN32)
install(TARGETS cascadbShared cascadbStatic RUNTIME DESTINATION lib ARCHIVE DESTINATION lib)
else(WIN32)
install(TARGETS cascadbShared cascadbStatic LIBRARY DESTINATION lib ARCHIVE DESTINATION lib)
endif(WIN32)
