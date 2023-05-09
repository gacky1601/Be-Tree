// This test program performs a series of inserts, deletes, updates,
// and queries to a betree.  It performs the same sequence of
// operatons on a std::map.  It checks that it always gets the same
// result from both data structures.

// The program takes 1 command-line parameter -- the number of
// distinct keys it can use in the test.

// The values in this test are strings.  Since updates use operator+
// on the values, this test performs concatenation on the strings.

#include <string.h>
#include <sys/types.h>
#include <sys/time.h>
#include <unistd.h>
#include "betree.hpp"
using namespace std;

void list_nodes(std::unordered_map <uint64_t, swap_space::object*> objects) {

    map<uint64_t, uint64_t> idtobsid;
    map<uint64_t, swap_space::object*> temp;
    for (auto _obj : objects) {
        temp.insert(_obj);
    }

    for (auto key : objects)
        idtobsid.insert(pair<uint64_t, uint64_t>(key.second->id, key.second->bsid));

    for (auto key : temp) {
        cout << "id: " << key.second->id << "  "
            << "bsid: " << key.second->bsid << "  "
            << "drity: " << key.second->target_is_dirty << "  "
            << "is_leaf: " << key.second->is_leaf << "  "
            << "address: " << objects[key.second->id]->target << "  ";
        if (objects[key.second->id]->target != 0) {
            cout << "\n";
            cout << "========================================================" << endl;
            auto* temp_target = (objects[key.second->id]->target);
            temp_target->node_infomation(&idtobsid);
        }
        cout << "\n";
        cout << "========================================================" << endl;
    }
}


int main(int argc, char **argv)
{
  uint64_t cache_size = 12;
	uint64_t min_flush_size = 4;
	uint64_t max_node_size = 8;
	one_file_per_object_backing_store ofpobs("./cache");
	swap_space sspace(&ofpobs, cache_size);
	betree<uint64_t, uint64_t> b(&sspace, max_node_size, min_flush_size);
  	// for (uint64_t i = 0;i < 8;i++){
	// 	b.insert(i, i);
	// 	list_nodes(b.ss->objects);
	// }
  cin.get();
  return 0;
}

