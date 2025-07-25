#include <iostream>
#include <string>
#include <vector>
#include <algorithm>
#include <optional>
#include <csignal>
#include <cstring>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <thread>
#include <semaphore>   // C++20
#include <atomic>
#include <vector>
// ------------- utilities -----------------
#include <unordered_map>  // add this
static std::unordered_map<std::string, std::string> store;


static bool send_all(int fd, const void* buf, size_t len) {
    const char* p = static_cast<const char*>(buf);
    while (len > 0) {
        ssize_t n = ::send(fd, p, len, 0);
        if (n <= 0) return false;
        p   += n;
        len -= n;
    }
    return true;
}

static void to_upper(std::string& s) {
    std::transform(s.begin(), s.end(), s.begin(),
                   [](unsigned char c){ return std::toupper(c); });
}

// ------------- RESP parsing bits (your existing code, unchanged) -----------------

struct RespArray {
    std::vector<std::string> elems;
};

static bool parse_int(const std::string& s, size_t& pos, long& out) {
    size_t start = pos;
    bool neg = false;
    if (pos < s.size() && s[pos] == '-') { neg = true; ++pos; }
    long v = 0;
    while (pos < s.size() && std::isdigit(static_cast<unsigned char>(s[pos]))) {
        v = v * 10 + (s[pos] - '0');
        ++pos;
    }
    if (pos + 1 >= s.size() || s[pos] != '\r' || s[pos + 1] != '\n') return false;
    pos += 2;
    out = neg ? -v : v;
    return pos > start + 2;
}

static bool parse_bulk_string(const std::string& s, size_t& pos, std::string& out) {
    if (pos >= s.size() || s[pos] != '$') return false;
    ++pos;
    long len = 0;
    if (!parse_int(s, pos, len)) return false;
    if (len < 0) { out.clear(); return true; } // null bulk string (not used here)
    if (pos + static_cast<size_t>(len) + 2 > s.size()) return false;
    out.assign(s.data() + pos, static_cast<size_t>(len));
    pos += static_cast<size_t>(len);
    if (pos + 1 >= s.size() || s[pos] != '\r' || s[pos + 1] != '\n') return false;
    pos += 2;
    return true;
}

// ---- the missing definition ----
bool parse_resp_array_of_bulk_strings(const std::string& s, RespArray& arr) {
    size_t pos = 0;
    if (s.empty() || s[pos] != '*') return false;
    ++pos;
    long n = 0;
    if (!parse_int(s, pos, n) || n < 0) return false;

    arr.elems.clear();
    arr.elems.reserve(static_cast<size_t>(n));
    for (long i = 0; i < n; ++i) {
        std::string elem;
        if (!parse_bulk_string(s, pos, elem)) return false;
        arr.elems.push_back(std::move(elem));
    }
    return true;
}

// ------------- request handling -----------------

std::string handle_request(const std::string& req) {
    RespArray a;
    std::string reply;

    if (parse_resp_array_of_bulk_strings(req, a) && !a.elems.empty()) {
        std::string cmd = a.elems[0];
        to_upper(cmd);

        if (cmd == "PING") {
            if (a.elems.size() == 1) {
                reply = "+PONG\r\n";
            } else {
                // If tester expects simple string back, keep '+'; if bulk, adapt.
                reply = "+" + a.elems[1] + "\r\n";
            }
        }else if (cmd == "ECHO") {
            if (a.elems.size() == 2) {
                const std::string& msg = a.elems[1];
                reply = "$" + std::to_string(msg.size()) + "\r\n" + msg + "\r\n";
            } else {
                reply = "-ERR wrong number of arguments for 'echo' command\r\n";
            }
        } else if (cmd == "SET") {
            if (a.elems.size() == 3) {
                const std::string& key = a.elems[1];
                const std::string& val = a.elems[2];
                store.insert_or_assign(key, val);
                reply = "+OK\r\n"; // Simple String
            } else {
                reply = "-ERR wrong number of arguments for 'set' command\r\n";
            }
        }else if (cmd == "GET") {
            if (a.elems.size() == 2) {
                const std::string& key = a.elems[1];
                auto it = store.find(key);
                if (it == store.end()) {
                    reply = "$-1\r\n"; // Null bulk string
                } else {
                    const std::string& val = it->second;
                    reply = "$" + std::to_string(val.size()) + "\r\n" + val + "\r\n";
                }
            } else {
                reply = "-ERR wrong number of arguments for 'get' command\r\n";
            }
}

              
        else {
            reply = "-ERR unknown command\r\n";
        }
    } else {
        // fallback if someone sends inline "PING\r\n"
        if (req.rfind("PING", 0) == 0) {
            reply = "+PONG\r\n";
        } else {
            reply = "-ERR protocol error\r\n";
        }
    }

    return reply;
}

// ------------- per-client loop -----------------

void handle_client(int client_fd) {
    for (;;) {
        char buf[4096];
        ssize_t n = ::recv(client_fd, buf, sizeof(buf), 0);
        if (n == 0) {
            // client closed the connection
            break;
        }
        if (n < 0) {
            perror("recv");
            break;
        }

        std::string req(buf, buf + n);
        std::string reply = handle_request(req);

        if (!send_all(client_fd, reply.data(), reply.size())) {
            break; // client closed while we were sending
        }
    }
}


// ------------- accept loop -----------------
std::counting_semaphore<128> slots(128);
std::atomic_bool running = true;

void serve_forever(int server_fd) {
    std::vector<std::jthread> workers; // optionally keep to join on shutdown

    while (running) {
        sockaddr_in caddr{};
        socklen_t clen = sizeof(caddr);
        int client_fd = ::accept(server_fd,
                                 reinterpret_cast<sockaddr*>(&caddr),
                                 &clen);
        if (client_fd < 0) {
            if (errno == EINTR) continue;
            perror("accept");
            continue; // or break, depending on your policy
        }

        slots.acquire(); // block if we’re at capacity

        // Spawn a thread that handles this client
        workers.emplace_back([client_fd](std::stop_token st) {
            // RAII to ensure close + release the slot
            struct Guard {
                int fd;
                ~Guard(){ ::close(fd); slots.release(); }
            } guard{client_fd};

            handle_client(client_fd); // your loop that serves many commands
        });
        // If you don't want to store workers, call workers.back().detach() or use std::thread and detach it.
    }

    // Optional: if you keep workers, they will auto-join when `workers` goes out of scope.
}


// ------------- setup socket -----------------

int create_listen_socket(uint16_t port) {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;

    int reuse = 1;
    if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0) {
        close(fd);
        return -1;
    }

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons(port);

    if (bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
        close(fd);
        return -1;
    }

    if (listen(fd, 16) != 0) {
        close(fd);
        return -1;
    }
    return fd;
}

// ------------- main -----------------

int main() {
    std::cout << std::unitbuf;
    std::cerr << std::unitbuf;
    std::signal(SIGPIPE, SIG_IGN);

    int server_fd = create_listen_socket(6379);
    if (server_fd < 0) {
        std::cerr << "Failed to setup server socket\n";
        return 1;
    }

    std::cout << "Waiting for a client to connect...\n";
    serve_forever(server_fd);

    close(server_fd);
    return 0;
}
