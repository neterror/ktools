cmake_minimum_required(VERSION 3.20)
project(ktools)

find_package(Qt6 REQUIRED COMPONENTS Core Network StateMachine)
qt_standard_project_setup()

set(CMAKE_BUILD_TYPE MinSizeRel)
set(CMAKE_COLOR_MAKEFILE OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

add_subdirectory(kproxy)


##### Schema Registry
########################################################
add_executable(kreg src/kreg.cpp)
target_link_libraries(kreg PUBLIC Qt6::Core Qt6::Network kproxy)


##### KTopics
########################################################
add_executable(ktopics src/ktopics.cpp)
target_link_libraries(ktopics PUBLIC Qt6::Core Qt6::Network kproxy)


##### KWrite
########################################################
add_executable(kwrite src/kwrite.cpp src/stdin_reader.cpp src/stdin_reader.h)
target_link_libraries(kwrite PUBLIC Qt6::Core Qt6::Network kproxy)


##### KRead
########################################################
add_executable(kread src/kread.cpp)
target_link_libraries(kread PUBLIC Qt6::Core Qt6::Network Qt6::StateMachine kproxy)

##### KGroups
########################################################
add_executable(kgroups src/kgroups.cpp)
target_link_libraries(kgroups PUBLIC Qt6::Core Qt6::Network Qt6::StateMachine kproxy)


install(TARGETS kreg DESTINATION bin)
install(TARGETS ktopics DESTINATION bin)
install(TARGETS kwrite DESTINATION bin)
install(TARGETS kread DESTINATION bin)
install(TARGETS kgroups DESTINATION bin)



include(GNUInstallDirs)
set(INCLUDE_INSTALL_DIR ${CMAKE_INSTALL_INCLUDEDIR}/kproxy
  CACHE PATH "Location of header files"
)


include(CMakePackageConfigHelpers)
configure_package_config_file(kproxyConfig.cmake.in
  ${CMAKE_CURRENT_BINARY_DIR}/kproxyConfig.cmake
  INSTALL_DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/kproxy
  PATH_VARS INCLUDE_INSTALL_DIR 
)

write_basic_package_version_file(
  ${CMAKE_CURRENT_BINARY_DIR}/kproxyConfigVersion.cmake
  VERSION 1.0.0
  COMPATIBILITY SameMajorVersion
)

install(FILES ${CMAKE_CURRENT_BINARY_DIR}/kproxyConfig.cmake
  ${CMAKE_CURRENT_BINARY_DIR}/kproxyConfigVersion.cmake
  DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/kproxy
)
