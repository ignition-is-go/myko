#pragma once

// myko C++ bindings
//
// This header provides C++ bindings for the Myko event-sourcing framework.
//
// Usage:
//   #include <myko/myko.h>
//
//   int main() {
//       auto client = myko::new_client();
//       client->set_address("ws://localhost:5155/myko");
//
//       if (client->is_connected()) {
//           // Send event as JSON
//           std::string result = client->send_event_json(R"({
//               "item": {"id": "...", "name": "..."},
//               "itemType": "Target",
//               "changeType": "SET",
//               "tx": "...",
//               "createdAt": "..."
//           })");
//
//           if (!result.empty()) {
//               std::cerr << "Error: " << result << std::endl;
//           }
//       }
//
//       client->disconnect();
//       return 0;
//   }

// Include the generated cxx bridge header
// This path is set by CMake to point to the generated header
#include "lib.rs.h"
