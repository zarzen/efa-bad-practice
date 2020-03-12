#ifndef EFA_THD_H
#define EFA_THD_H

#include "efa_ep.h"
#include "util.h"
#include <mutex>
#include <queue>
#include <vector>

namespace trans {
void efa_worker_thd(std::string thd_name, trans::EFAEndpoint **efa,
                    std::queue<Tasks *> *task_q, std::mutex *task_m);
}; // namespace trans
#endif
