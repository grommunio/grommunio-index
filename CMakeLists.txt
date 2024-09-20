cmake_minimum_required(VERSION 3.14)
project(grommunio-index VERSION 1.0)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(exmdbpp 1.8 REQUIRED)
find_package(LibXml2 REQUIRED)
find_package(SQLite3 REQUIRED)
find_package(PkgConfig REQUIRED)

pkg_get_variable(SYSUSERDIR systemd sysusersdir)
pkg_get_variable(UNITDIR systemd systemdsystemunitdir)
pkg_check_modules(HX REQUIRED "libHX >= 3.27")

find_program(MARIADB_CONFIG NAMES mariadb_config mysql_config)
execute_process(COMMAND ${MARIADB_CONFIG} --variable=pkgincludedir OUTPUT_VARIABLE MYSQL_INCLUDE_DIRS OUTPUT_STRIP_TRAILING_WHITESPACE)
separate_arguments(MYSQL_INCLUDE_DIRS UNIX_COMMAND "${MYSQL_INCLUDE_DIRS}")
execute_process(COMMAND ${MARIADB_CONFIG} --libs OUTPUT_VARIABLE MYSQL_LIBRARIES OUTPUT_STRIP_TRAILING_WHITESPACE)
separate_arguments(MYSQL_LIBRARIES UNIX_COMMAND "${MYSQL_LIBRARIES}")

add_executable(grommunio-index grommunio-index.cpp)
target_include_directories(grommunio-index PRIVATE
	${SQLite3_INCLUDE_DIRS}
	${MYSQL_INCLUDE_DIRS}
	${LIBXML2_INCLUDE_DIRS}
	)
target_compile_options(grommunio-index PRIVATE ${HX_CFLAGS} ${MYSQL_CFLAGS} -Wall)
target_link_libraries(grommunio-index
	${HX_LIBRARIES}
	${MYSQL_LIBRARIES}
	${SQLite3_LIBRARIES}
	exmdbpp::exmdbpp
	${LIBXML2_LIBRARY}
	)

option(LOGGING_TRACE "Enable TRACE logging level" OFF)
if(LOGGING_TRACE)
    add_compile_definitions(ENABLE_TRACE)
endif()

install(TARGETS grommunio-index RUNTIME)
install(FILES grommunio-index.service grommunio-index.timer DESTINATION ${UNITDIR})
install(FILES system-user-groindex.conf DESTINATION ${SYSUSERDIR})
