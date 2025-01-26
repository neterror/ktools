set(HEADERS
  http_client.h
  kafka_consumer.h
  kafka_protobuf_producer.h
  kafka_messages.h
  kafka_proxy_v2.h
  kafka_proxy_v3.h
  schema_registry.h
  schema_delete.h
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
  schema_delete.cpp
  schema_create.cpp
  topics_delete.cpp

  ${HEADERS}
)

target_include_directories(kproxy PUBLIC ${CMAKE_CURRENT_LIST_DIR})
target_link_libraries(kproxy PUBLIC Qt6::Core Qt6::Network Qt6::StateMachine)


set_target_properties(kproxy PROPERTIES PUBLIC_HEADER "${HEADERS}")
set_target_properties(kproxy PROPERTIES POSITION_INDEPENDENT_CODE ON)

install(TARGETS kproxy
  EXPORT kproxyTargets
  LIBRARY DESTINATION lib
  PUBLIC_HEADER DESTINATION include/kproxy)

