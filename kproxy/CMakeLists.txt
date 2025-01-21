add_library(kproxy STATIC
  http_client.cpp
  http_client.h
  kafka_consumer.cpp
  kafka_consumer.h
  kafka_producer.cpp
  kafka_producer.h
  kafka_proxy_v2.cpp
  kafka_proxy_v2.h
  kafka_proxy_v3.cpp
  kafka_proxy_v3.h
  schema_registry.cpp
  schema_registry.h
  schema_delete.cpp
  schema_delete.h
  schema_create.cpp
  schema_create.h
  topics_delete.cpp
  topics_delete.h
)

target_include_directories(kproxy PUBLIC ${CMAKE_CURRENT_LIST_DIR})
target_link_libraries(kproxy PUBLIC Qt6::Core Qt6::Network Qt6::StateMachine)


set_target_properties(kproxy PROPERTIES PUBLIC_HEADER "kafka_proxy_v2.h;kafka_proxy_v3.h;kproxy.h")

install(TARGETS kproxy
  LIBRARY DESTINATION lib
  PUBLIC_HEADER DESTINATION include/kproxy)

