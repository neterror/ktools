add_library(proxy STATIC
  http_client.cpp
  http_client.h
  kafka_consumer.cpp
  kafka_consumer.h
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

target_include_directories(proxy PUBLIC ${CMAKE_CURRENT_LIST_DIR})
target_link_libraries(proxy PUBLIC Qt6::Core Qt6::Network Qt6::StateMachine)
