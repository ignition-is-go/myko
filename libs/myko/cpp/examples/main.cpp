#include <iostream>
#include <thread>
#include <chrono>

// Include the cxx-generated header directly
// In actual projects, use: #include <myko/myko.h>
#include "lib.rs.h"

int main() {
    std::cout << "Creating MykoClient..." << std::endl;

    // Create a new client
    auto client = myko::new_client();

    // Connect to server
    std::cout << "Connecting to ws://localhost:5155/myko..." << std::endl;
    client->set_address("ws://localhost:5155/myko");

    // Wait a bit for connection
    std::this_thread::sleep_for(std::chrono::seconds(1));

    // Check connection status
    if (client->is_connected()) {
        std::cout << "Connected!" << std::endl;

        // Send an event
        std::string event_json = R"({
            "item": {"id": "test-id", "name": "Test Item", "hash": "abc123"},
            "itemType": "Target",
            "changeType": "SET",
            "tx": "test-tx-123",
            "createdAt": "2024-01-01T00:00:00Z"
        })";

        std::cout << "Sending event..." << std::endl;
        auto result = client->send_event_json(event_json);

        if (result.empty()) {
            std::cout << "Event sent successfully!" << std::endl;
        } else {
            std::cerr << "Error sending event: " << result << std::endl;
        }
    } else {
        std::cout << "Not connected (server may not be running)" << std::endl;
    }

    // Disconnect
    std::cout << "Disconnecting..." << std::endl;
    client->disconnect();

    std::cout << "Done!" << std::endl;
    return 0;
}
