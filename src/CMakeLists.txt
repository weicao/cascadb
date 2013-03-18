file( GLOB_RECURSE SOURCE RELATIVE "${CMAKE_CURRENT_SOURCE_DIR}"
        "*.cpp"
		)
		
add_library(cascadbShared SHARED ${SOURCE})
target_link_libraries(cascadbShared ${CASCADB_LIBS})
add_library(cascadbStatic STATIC ${SOURCE})
set_target_properties(cascadbShared cascadbStatic PROPERTIES OUTPUT_NAME cascadb)

if(WIN32)
install(TARGETS cascadbShared cascadbStatic RUNTIME DESTINATION lib ARCHIVE DESTINATION lib)
else(WIN32)
install(TARGETS cascadbShared cascadbStatic LIBRARY DESTINATION lib ARCHIVE DESTINATION lib)
endif(WIN32)
