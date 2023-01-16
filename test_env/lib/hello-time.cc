#include "lib/hello-time.h"
#include <iostream>
#include <ctime>

void print_localtime() {
  std::time_t result = std::time(nullptr);
  std::cout << std::asctime(std::localtime(&result));
}
