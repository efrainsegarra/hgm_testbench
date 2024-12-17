add_library(N2readData                 N2readData.c SimpleLog.c)
target_include_directories(N2readData PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(N2readData	m config )
