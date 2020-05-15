#ifndef SOLAR_SEQUENCER_H_
#define SOLAR_SEQUENCER_H_

#include <atomic>
#include <mutex>

namespace solar {

class Sequencer {
public:
  explicit Sequencer() {}
  Sequencer(const Sequencer &) = delete;
  Sequencer(Sequencer &&) = delete;

  Sequencer &operator=(const Sequencer &) = delete;
  Sequencer &operator=(Sequencer &&) = delete;

  uint64_t Next();

private:
  std::atomic<uint64_t> glsn_{0};
};

}

#endif
