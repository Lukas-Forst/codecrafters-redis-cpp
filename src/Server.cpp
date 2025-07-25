#include <iostream>
#include <string>
#include <string_view>
#include <cstring>
#include <cstdlib>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <csignal>

int main(int argc, char** argv) {
    std::cout << std::unitbuf;
    std::cerr << std::unitbuf;

    // Ignore SIGPIPE so send() on a closed socket doesn't kill us
    std::signal(SIGPIPE, SIG_IGN);

    int server_fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) {
        std::cerr << "Failed to create server socket\n";
        return 1;
    }

    int reuse = 1;
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0) {
        std::cerr << "setsockopt failed\n";
        return 1;
    }

    sockaddr_in server_addr{};
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = htonl(INADDR_ANY);   // or inet_addr("127.0.0.1")
    server_addr.sin_port = htons(6379);

    if (bind(server_fd, reinterpret_cast<sockaddr*>(&server_addr), sizeof(server_addr)) != 0) {
        std::cerr << "Failed to bind to port 6379\n";
        return 1;
    }

    if (listen(server_fd, 5) != 0) {
        std::cerr << "listen failed\n";
        return 1;
    }

    std::cout << "Waiting for a client to connect...\n";

    sockaddr_in client_addr{};
    socklen_t client_len = sizeof(client_addr);
    int client_fd = ::accept(server_fd, reinterpret_cast<sockaddr*>(&client_addr), &client_len);
    if (client_fd < 0) {
        std::cerr << "accept failed\n";
        return 1;
    }
    std::cout << "Client connected\n";

    // Read once (single-line protocol for now)
    char buf[1024];
    ssize_t n = ::recv(client_fd, buf, sizeof(buf) - 1, 0);
    if (n <= 0) {
        std::cerr << "recv failed or client closed\n";
        close(client_fd);
        close(server_fd);
        return 1;
    }
    buf[n] = '\0';
    std::string_view req(buf, static_cast<size_t>(n));

    // Very naive check; if you want full RESP, you must parse it properly.
    if (req.find("PING") == 0) {
        static constexpr char pong[] = "+PONG\r\n";
        ::send(client_fd, pong, sizeof(pong) - 1, 0);
    } else {
        // echo back what we got (not RESP-compliant, but shows the flow)
        ::send(client_fd, buf, n, 0);
    }

    close(client_fd);
    close(server_fd);
    return 0;
}
