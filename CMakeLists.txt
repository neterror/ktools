cmake_minimum_required(VERSION 3.20)
project(httpkafka)

find_package(Qt6 REQUIRED COMPONENTS Core Network StateMachine)
qt_standard_project_setup()

set(CMAKE_BUILD_TYPE Debug)
set(CMAKE_COLOR_MAKEFILE OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

add_subdirectory(proxy)


##### Schema Registry
########################################################
add_executable(kreg src/kreg.cpp)
target_link_libraries(kreg PUBLIC Qt6::Core Qt6::Network proxy)


##### KTopics
########################################################
add_executable(ktopics src/ktopics.cpp)
target_link_libraries(ktopics PUBLIC Qt6::Core Qt6::Network proxy)


##### KSend
########################################################
add_executable(ksend src/ksend.cpp)
target_link_libraries(ksend PUBLIC Qt6::Core Qt6::Network proxy)


##### KRead
########################################################
add_executable(kread src/kread.cpp)
target_link_libraries(kread PUBLIC Qt6::Core Qt6::Network Qt6::StateMachine proxy)

##### KGroups
########################################################
add_executable(kgroups src/kgroups.cpp)
target_link_libraries(kgroups PUBLIC Qt6::Core Qt6::Network Qt6::StateMachine proxy)


