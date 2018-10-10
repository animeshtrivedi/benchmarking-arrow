#include <cstdint>
#include <cstdio>
#include <cstring>
#include <memory>
#include <string>
#include <vector>

#include <arrow/array.h>
#include <arrow/memory_pool.h>

#include <iostream>
int main(int argc, char **argv) {
  auto memory_pool = arrow::default_memory_pool();
  std::cout << "__cplusplus version is : " << __cplusplus << std::endl;
  std::cout << " address of memory pool is " << memory_pool << std::endl;
  return 0;
}
