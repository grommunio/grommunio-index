cmake_minimum_required(VERSION 3.14)
project(grommunio-index VERSION 0.1)
set(CMAKE_CXX_STANDARD 17)

find_package(SQLite3 REQUIRED)
find_package(exmdbpp 1.8 REQUIRED)
find_package(PkgConfig REQUIRED)
pkg_get_variable(UNITDIR systemd systemdsystemunitdir)

add_executable(grommunio-index grommunio-index.cpp)
target_include_directories(grommunio-index PRIVATE ${SQLite3_INCLUDE_DIRS})
target_link_libraries(grommunio-index ${SQLite3_LIBRARIES} exmdbpp::exmdbpp)

option(LOGGING_TRACE "Enable TRACE logging level" OFF)
if(LOGGING_TRACE)
    add_compile_definitions(ENABLE_TRACE)
endif()

install(TARGETS grommunio-index RUNTIME)
install(PROGRAMS grommunio-index-run.sh DESTINATION sbin)
install(FILES grommunio-index.service grommunio-index.timer DESTINATION ${UNITDIR})
