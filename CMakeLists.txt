cmake_minimum_required(VERSION 3.16)
project(couch C)

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_C_EXTENSIONS ON)

find_package(Threads REQUIRED)
find_package(PkgConfig REQUIRED)
pkg_check_modules(SQLITE3 REQUIRED sqlite3)
pkg_check_modules(CURL REQUIRED libcurl)

add_executable(couch src/couch.c src/http.c src/db.c src/config.c)
target_include_directories(couch PRIVATE include ${SQLITE3_INCLUDE_DIRS})
target_link_libraries(couch PRIVATE Threads::Threads ${SQLITE3_LIBRARIES})

add_executable(couch11 src/couch11.c src/http_client.c src/config.c)
target_include_directories(couch11 PRIVATE include ${CURL_INCLUDE_DIRS})
target_link_libraries(couch11 PRIVATE Threads::Threads ${CURL_LIBRARIES})

install(TARGETS couch couch11 RUNTIME DESTINATION bin)
