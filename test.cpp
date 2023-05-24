#include "betree.hpp"
#include <vector>
using namespace std;

int main(int argc, char** argv) {
  uint64_t min_flush_size = 10;
  uint64_t max_node_size = 20;
  betree<uint64_t, uint64_t> bepsilontree(max_node_size, min_flush_size);
  vector<uint64_t> random_numbers;
  clock_t start, end;
  srand((unsigned)time(NULL));
  while (random_numbers.size() < 100000) {
    random_numbers.push_back(rand());
  }
  // Sequential Write
  start = clock();
    for (uint64_t i = 0;i < 100000;i++) {
    bepsilontree.insert(i, i);
  }
  end = clock();
  
  std::cout << "循序寫入時間：" << double(end - start) / CLOCKS_PER_SEC << endl;
  betree<uint64_t, uint64_t> bepsilontree2(max_node_size, min_flush_size);
  //Random Write
  start = clock();
  for (uint64_t i : random_numbers) {
    bepsilontree2.insert(i, i);
  }
  end = clock();
  std::cout << "隨機寫入時間：" << double(end - start) / CLOCKS_PER_SEC << endl;
  cin.get();
}