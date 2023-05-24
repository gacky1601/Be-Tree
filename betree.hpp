#include <cassert>
#include <iostream>
#include <map>
#include <string>

#define DEFAULT_MAX_NODE_SIZE (1ULL<<18)
#define DEFAULT_MIN_FLUSH_SIZE (DEFAULT_MAX_NODE_SIZE / 16ULL)

template<class Key>
class MessageKey {
public:
  MessageKey(void):key(), timestamp(0) {}
  MessageKey(const Key& k, uint64_t tstamp):key(k), timestamp(tstamp) {}
  static MessageKey range_start(const Key& key) { return MessageKey(key, 0); }
  static MessageKey range_end(const Key& key) { return MessageKey(key, UINT64_MAX); }
  MessageKey range_start(void) const { return range_start(key); }
  MessageKey range_end(void) const { return range_end(key); }
  Key key;
  uint64_t timestamp;
};

template<class Key>
bool operator<(const MessageKey<Key>& mkey1, const MessageKey<Key>& mkey2) {
  return mkey1.key < mkey2.key || (mkey1.key == mkey2.key && mkey1.timestamp < mkey2.timestamp);
}

template<class Key>
bool operator<(const Key& key, const MessageKey<Key>& mkey) {
  return key < mkey.key;
}

template<class Key>
bool operator<(const MessageKey<Key>& mkey, const Key& key) {
  return mkey.key < key;
}

template<class Key>
bool operator==(const MessageKey<Key>& a, const MessageKey<Key>& b) {
  return a.key == b.key && a.timestamp == b.timestamp;
}

#define INSERT (0)
#define DELETE (1)
#define UPDATE (2)

template<class Value>
class Message {
public:
  Message(void): opcode(INSERT), val() {}
  Message(int opc, const Value& v):opcode(opc), val(v) {}
  int opcode;
  Value val;
};

template <class Value>
bool operator==(const Message<Value>& a, const Message<Value>& b) {
  return a.opcode == b.opcode && a.val == b.val;
}

template<class Key, class Value> class betree {
public:
  class node;
  class child_info {
  public:
    child_info(void): child(), child_size(0) {}
    child_info(betree::node* child, uint64_t child_size): child(child), child_size(child_size) {}
    betree::node* child;
    uint64_t child_size;
  };

  typedef typename std::map<Key, child_info> pivot_map;
  typedef typename std::map<MessageKey<Key>, Message<Value> > message_map;

  class node {
  public:
    pivot_map pivots;
    message_map elements;
    bool is_leaf(void) const { return pivots.empty(); }

    template<class OUT, class IN>
    static OUT get_pivot(IN& mp, const Key& k) {
      assert(mp.size() > 0);
      auto it = mp.lower_bound(k);
      if (it == mp.begin() && k < it->first)
        throw std::out_of_range("Key does not exist "
          "(it is smaller than any key in DB)");
      if (it == mp.end() || k < it->first)
        --it;
      return it;
    }

    typename pivot_map::const_iterator get_pivot(const Key& k) const {
      return get_pivot<typename pivot_map::const_iterator,
        const pivot_map>(pivots, k);
    }

    typename pivot_map::iterator
      get_pivot(const Key& k) {
      return get_pivot<typename pivot_map::iterator, pivot_map>(pivots, k);
    }

    template<class OUT, class IN>
    static OUT get_element_begin(IN& elts, const Key& k) {
      return elts.lower_bound(MessageKey<Key>::range_start(k));
    }

    typename message_map::iterator get_element_begin(const Key& k) {
      return get_element_begin<typename message_map::iterator,
        message_map>(elements, k);
    }

    typename message_map::const_iterator get_element_begin(const Key& k) const {
      return get_element_begin<typename message_map::const_iterator,
        const message_map>(elements, k);
    }

    typename message_map::iterator
      get_element_begin(const typename pivot_map::iterator it) {
      return it == pivots.end() ? elements.end() : get_element_begin(it->first);
    }

    void apply(const MessageKey<Key>& mkey, const Message<Value>& elt, Value& default_value) {
      switch (elt.opcode) {
        case INSERT:
          elements.erase(elements.lower_bound(mkey.range_start()), elements.upper_bound(mkey.range_end()));
          elements[mkey] = elt;
          break;

        case DELETE:
          elements.erase(elements.lower_bound(mkey.range_start()), elements.upper_bound(mkey.range_end()));
          if (!is_leaf())
            elements[mkey] = elt;
          break;

        case UPDATE:{
          auto iter = elements.upper_bound(mkey.range_end());
          if (iter != elements.begin())
            iter--;
          if (iter == elements.end() || iter->first.key != mkey.key)
            if (is_leaf()) {
              Value dummy = default_value;
              apply(mkey, Message<Value>(INSERT, dummy + elt.val), default_value);
            }
            else {
              elements[mkey] = elt;
            }
            else {
              assert(iter != elements.end() && iter->first.key == mkey.key);
              if (iter->second.opcode == INSERT) {
                apply(mkey, Message<Value>(INSERT, iter->second.val + elt.val),default_value);
              }
              else {
                elements[mkey] = elt;
              }
            }
          }
          break;
        default:
          assert(0);
      }
    }

    pivot_map split(betree& bet) {
      assert(pivots.size() + elements.size() >= bet.max_node_size);
      int num_new_leaves = (pivots.size() + elements.size()) / (10 * bet.max_node_size / 24);
      int things_per_new_leaf = (pivots.size() + elements.size() + num_new_leaves - 1) / num_new_leaves;
      pivot_map result;
      auto pivot_idx = pivots.begin();
      auto elt_idx = elements.begin();
      int things_moved = 0;
      for (int i = 0; i < num_new_leaves; i++) {
        if (pivot_idx == pivots.end() && elt_idx == elements.end())
          break;
        node* new_node = new node();
        result[pivot_idx != pivots.end() ? pivot_idx->first : elt_idx->first.key] = child_info(new_node, new_node->elements.size() + new_node->pivots.size());
        while (things_moved < (i + 1) * things_per_new_leaf && (pivot_idx != pivots.end() || elt_idx != elements.end())) {
          if (pivot_idx != pivots.end()) {
            new_node->pivots[pivot_idx->first] = pivot_idx->second;
            ++pivot_idx;
            things_moved++;
            auto elt_end = get_element_begin(pivot_idx);
            while (elt_idx != elt_end) {
              new_node->elements[elt_idx->first] = elt_idx->second;
              ++elt_idx;
              things_moved++;
            }
          }
          else {
            assert(pivots.size() == 0);
            new_node->elements[elt_idx->first] = elt_idx->second;
            ++elt_idx;
            things_moved++;
          }
        }
      }
      for (auto it = result.begin(); it != result.end(); ++it)
        it->second.child_size = it->second.child->elements.size() + it->second.child->pivots.size();

      assert(pivot_idx == pivots.end());
      assert(elt_idx == elements.end());
      pivots.clear();
      elements.clear();
      return result;
    }

    pivot_map flush(betree& bet, message_map& elts) {
      pivot_map result;
      if (elts.size() == 0)
        return result;

      if (is_leaf()) {
        for (auto it = elts.begin(); it != elts.end(); ++it)
          apply(it->first, it->second, bet.default_value);
        if (elements.size() + pivots.size() >= bet.max_node_size) 
          result = split(bet);
        return result;
      }

      ////////////// Non-leaf // Update the key of the first child, if necessary
      Key oldmin = pivots.begin()->first;
      MessageKey<Key> newmin = elts.begin()->first;

      if (newmin < oldmin) {
        pivots[newmin.key] = pivots[oldmin];
        pivots.erase(oldmin);
      }

      auto first_pivot_idx = get_pivot(elts.begin()->first.key);
      auto last_pivot_idx = get_pivot((--elts.end())->first.key);
      if (first_pivot_idx == last_pivot_idx) {
        auto next_pivot_idx = next(first_pivot_idx);
        auto elt_start = get_element_begin(first_pivot_idx);
        auto elt_end = get_element_begin(next_pivot_idx);
        assert(elt_start == elt_end);
        pivot_map new_children = first_pivot_idx->second.child->flush(bet, elts);
        if (!new_children.empty()) {
          pivots.erase(first_pivot_idx);
          pivots.insert(new_children.begin(), new_children.end());
        }
        else {
          first_pivot_idx->second.child_size = first_pivot_idx->second.child->pivots.size() + first_pivot_idx->second.child->elements.size();
        }
      }
      else {
        for (auto it = elts.begin(); it != elts.end(); ++it)
          apply(it->first, it->second, bet.default_value);

        while (elements.size() + pivots.size() >= bet.max_node_size) {
          unsigned int max_size = 0;
          auto child_pivot = pivots.begin();
          auto next_pivot = pivots.begin();
          for (auto it = pivots.begin(); it != pivots.end(); ++it) {
            auto it2 = next(it);
            auto elt_it = get_element_begin(it);
            auto elt_it2 = get_element_begin(it2);
            unsigned int dist = distance(elt_it, elt_it2);
            if (dist > max_size) {
              child_pivot = it;
              next_pivot = it2;
              max_size = dist;
            }
          }
          if (!(max_size > bet.min_flush_size || max_size > bet.min_flush_size / 2))
            break;
          auto elt_child_it = get_element_begin(child_pivot);
          auto elt_next_it = get_element_begin(next_pivot);
          message_map child_elts(elt_child_it, elt_next_it);
          pivot_map new_children = child_pivot->second.child->flush(bet, child_elts);
          elements.erase(elt_child_it, elt_next_it);
          if (!new_children.empty()) {
            pivots.erase(child_pivot);
            pivots.insert(new_children.begin(), new_children.end());
          }
          else {
            first_pivot_idx->second.child_size =
              child_pivot->second.child->pivots.size() +
              child_pivot->second.child->elements.size();
          }
        }
        if (elements.size() + pivots.size() > bet.max_node_size) {
          result = split(bet);
        }
      }
      return result;
    }

    Value query(const betree& bet, const Key k) const {
      if (is_leaf()) {
        auto it = elements.lower_bound(MessageKey<Key>::range_start(k));
        if (it != elements.end() && it->first.key == k) {
          assert(it->second.opcode == INSERT);
          return it->second.val;
        }
        else {
          throw std::out_of_range("Key does not exist");
        }
      }
      ///////////// Non-leaf
      auto message_iter = get_element_begin(k);
      Value v = bet.default_value;
      if (message_iter == elements.end() || k < message_iter->first)
        v = get_pivot(k)->second.child->query(bet, k);
      else if (message_iter->second.opcode == UPDATE) {
        try {
          Value t = get_pivot(k)->second.child->query(bet, k);
          v = t;
        }
        catch (std::out_of_range e) {}
      }
      else if (message_iter->second.opcode == DELETE) {
        message_iter++;
        if (message_iter == elements.end() || k < message_iter->first)
          throw std::out_of_range("Key does not exist");
      }
      else if (message_iter->second.opcode == INSERT) {
        v = message_iter->second.val;
        message_iter++;
      }
      while (message_iter != elements.end() && message_iter->first.key == k) {
        assert(message_iter->second.opcode == UPDATE);
        v = v + message_iter->second.val;
        message_iter++;
      }
      return v;
    }
  };

  uint64_t min_flush_size;
  uint64_t max_node_size;
  uint64_t min_node_size;
  node root;
  uint64_t next_timestamp = 1; // Nothing has a timestamp of 0
  Value default_value;

  betree( uint64_t maxnodesize = DEFAULT_MAX_NODE_SIZE, 
          uint64_t minnodesize = DEFAULT_MAX_NODE_SIZE / 4, 
          uint64_t minflushsize = DEFAULT_MIN_FLUSH_SIZE
          ):
            min_flush_size(minflushsize), 
            max_node_size(maxnodesize), 
            min_node_size(minnodesize) {}

  void upsert(int opcode, Key k, Value v) {
    message_map tmp;
    tmp[MessageKey<Key>(k, next_timestamp++)] = Message<Value>(opcode, v);
    pivot_map new_nodes = root.flush(*this, tmp);
    if (new_nodes.size() > 0)
      root.pivots = new_nodes;
  }

  void insert(Key k, Value v) { upsert(INSERT, k, v); }
  void update(Key k, Value v) { upsert(UPDATE, k, v); }
  void erase(Key k) { upsert(DELETE, k, default_value); }

  Value query(Key k) {
    Value v = root.query(*this, k);
    return v;
  }
};
