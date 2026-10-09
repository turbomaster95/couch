cmake_minimum_required(VERSION 3.16)
project(couch C)

set(CMAKE_C_STANDARD 17)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_C_EXTENSIONS ON)

find_package(Threads REQUIRED)
find_package(PkgConfig REQUIRED)
find_package(Python3 REQUIRED COMPONENTS Interpreter)

pkg_check_modules(SQLITE3 REQUIRED sqlite3)
pkg_check_modules(CURL REQUIRED libcurl)

set(COUCH_WEB_FILES
    ${CMAKE_CURRENT_SOURCE_DIR}/web/index.html
    ${CMAKE_CURRENT_SOURCE_DIR}/web/job.html
    ${CMAKE_CURRENT_SOURCE_DIR}/web/app.js
    ${CMAKE_CURRENT_SOURCE_DIR}/web/style.css
)

set(COUCH_EMBEDDED_WEB ${CMAKE_CURRENT_BINARY_DIR}/embweb.c)

add_custom_command(
    OUTPUT ${COUCH_EMBEDDED_WEB}
    COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/.embed_web.py ${COUCH_EMBEDDED_WEB} ${COUCH_WEB_FILES}
    DEPENDS ${COUCH_WEB_FILES} ${CMAKE_CURRENT_SOURCE_DIR}/.embed_web.py
    VERBATIM
)

add_executable(couch
    src/couch.c
    src/http.c
    src/db.c
    src/config.c
    ${COUCH_EMBEDDED_WEB}
)

target_include_directories(couch PRIVATE include ${SQLITE3_INCLUDE_DIRS})
target_link_libraries(couch PRIVATE Threads::Threads ${SQLITE3_LIBRARIES})

add_executable(couch11
    src/couch11.c
    src/http_client.c
    src/config.c
    third_party/tomlc17/src/tomlc17.c
)

target_include_directories(couch11 PRIVATE
    include
    ${CURL_INCLUDE_DIRS}
    ${CMAKE_CURRENT_SOURCE_DIR}/third_party/tomlc17/src
)

target_link_libraries(couch11 PRIVATE Threads::Threads ${CURL_LIBRARIES})

install(TARGETS couch couch11 RUNTIME DESTINATION bin)
install(FILES couch.conf couch11.conf DESTINATION etc/couch)
