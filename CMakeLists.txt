cmake_minimum_required(VERSION 3.20)
project(ktools)

find_package(Qt6 REQUIRED COMPONENTS Core Network StateMachine)
qt_standard_project_setup()

set(CMAKE_BUILD_TYPE Release)
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

