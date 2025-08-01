set(HEADERS
  http_client.h
  kafka_consumer.h
  kafka_protobuf_producer.h
  kafka_messages.h
  kafka_proxy_v2.h
  kafka_proxy_v3.h
  schema_registry.h
  schema_create.h
  topics_delete.h
)  

add_library(kproxy STATIC
  http_client.cpp
  kafka_consumer.cpp
  kafka_protobuf_producer.cpp
  kafka_proxy_v2.cpp
  kafka_proxy_v3.cpp
  schema_registry.cpp
  schema_create.cpp
  topics_delete.cpp

  ${HEADERS}
)

set_target_properties(kproxy PROPERTIES POSITION_INDEPENDENT_CODE ON)

# Find pqueue package
find_package(pqueue REQUIRED)

target_include_directories(kproxy PUBLIC 
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
    $<INSTALL_INTERFACE:${HEADER_INSTALL_DIR}>
)
target_link_libraries(kproxy PUBLIC Qt6::Core Qt6::Network Qt6::StateMachine pqueue::pqueue)


set_target_properties(kproxy PROPERTIES PUBLIC_HEADER "${HEADERS}")
set_target_properties(kproxy PROPERTIES POSITION_INDEPENDENT_CODE ON)

# Platform-specific header installation path
include(GNUInstallDirs)
if(TARGET_ARDOS_ARCHITECTURE STREQUAL "ARMv7")
    # For ARM cross-compilation, headers go to usr/include but config should reference include
    set(HEADER_INSTALL_DIR usr/include/kproxy)
    set(CONFIG_INCLUDE_DIR include/kproxy)
else()
    # For x86, use standard include dir
    set(HEADER_INSTALL_DIR ${CMAKE_INSTALL_INCLUDEDIR}/kproxy)
    set(CONFIG_INCLUDE_DIR ${CMAKE_INSTALL_INCLUDEDIR}/kproxy)
endif()

install(TARGETS kproxy
  EXPORT kproxyTargets
  LIBRARY DESTINATION lib
  PUBLIC_HEADER DESTINATION ${HEADER_INSTALL_DIR})

# Install export targets
install(EXPORT kproxyTargets
  FILE kproxyTargets.cmake
  NAMESPACE kproxy::
  DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/kproxy
)

set(INCLUDE_INSTALL_DIR ${CONFIG_INCLUDE_DIR}
  CACHE PATH "Location of header files"
)

include(CMakePackageConfigHelpers)
configure_package_config_file(${CMAKE_CURRENT_LIST_DIR}/../kproxyConfig.cmake.in
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

