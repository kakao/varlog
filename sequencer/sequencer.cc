#include <iostream>

#include "sequencer.h"

namespace solar {

uint64_t Sequencer::Next() {
  return glsn_.fetch_add(1, std::memory_order_relaxed);
}

}


