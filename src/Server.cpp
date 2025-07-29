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
#include <chrono>
#include <mutex>
#include <condition_variable>
#include <map>

static std::unordered_map<std::string, std::string> store;
static std::unordered_map<std::string, std::chrono::steady_clock::time_point> ttl;

static std::unordered_map<std::string, std::vector<std::string>> lists; // lists

struct StreamEntry {
    std::string id;
    std::map<std::string, std::string> fields;
};

std::unordered_map<std::string, std::vector<StreamEntry>> streams;


// Add this after your existing globals
struct ServerState {
    std::mutex mtx;
    std::unordered_map<std::string, std::condition_variable> waiting;
    // Note: we'll keep using your existing `lists` map instead of adding another kv map
};

static bool parse_timeout(const std::string& s, double& out) {
    try {
        size_t idx = 0;
        out = std::stod(s, &idx);
        return idx == s.size() && out >= 0.0;
    } catch (...) {
        return false;
    }
}

static ServerState server_state;

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
static bool is_expired_now(const std::string& key) {
    using clock = std::chrono::steady_clock;
    auto it = ttl.find(key);
    if (it == ttl.end()) return false;
    if (clock::now() >= it->second) {
        store.erase(key);
        ttl.erase(it);
        return true;
    }
    return false;
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
static inline std::string bulk(const std::string& s) {
    std::string out;
    out.reserve(1 + 20 + 2 + s.size() + 2);
    out += "$";
    out += std::to_string(s.size());
    out += "\r\n";
    out += s;
    out += "\r\n";
    return out;
}

static inline std::string bulk_array(const std::vector<std::string>& vals) {
    std::string out;
    out.reserve(1 + 20 + 2);
    out += "*";
    out += std::to_string(vals.size());
    out += "\r\n";
    for (const auto& v : vals) {
        out += "$";
        out += std::to_string(v.size());
        out += "\r\n";
        out += v;
        out += "\r\n";
    }
    return out;
}

static bool parse_ll(const std::string& s, long long& out) {
    try {
        size_t idx = 0;
        out = std::stoll(s, &idx, 10);
        return idx == s.size();
    } catch (...) {
        return false;
    }
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

// Update to pass client_fd (same as my previous suggestion)
std::string handle_request(const std::string& req, int client_fd) {
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
        ttl.erase(key);                 // clear previous expiry if any
        reply = "+OK\r\n";
    } else if (a.elems.size() == 5) {
        const std::string& key = a.elems[1];
        const std::string& val = a.elems[2];

        std::string opt = a.elems[3];
        to_upper(opt);                   // case-insensitive option
        if (opt != "PX") {
            reply = "-ERR syntax error\r\n";
                } else {
                    // parse milliseconds (non-negative)
                    long long ms = 0;
                    try {
                        size_t idx = 0;
                        ms = std::stoll(a.elems[4], &idx, 10);
                        if (idx != a.elems[4].size() || ms < 0) {
                            reply = "-ERR value is not an integer or out of range\r\n";
                        } else {
                            store.insert_or_assign(key, val);
                            using clock = std::chrono::steady_clock;
                            ttl[key] = clock::now() + std::chrono::milliseconds(ms);
                            reply = "+OK\r\n";
                        }
                    } catch (...) {
                        reply = "-ERR value is not an integer or out of range\r\n";
                    }
                }
            } else {
                reply = "-ERR wrong number of arguments for 'set' command\r\n";
            }
        }
        else if (cmd == "GET") {
    if (a.elems.size() == 2) {
        const std::string& key = a.elems[1];

        // lazy expiration: drop and return null if expired
        if (is_expired_now(key)) {
            reply = "$-1\r\n";
        } else {
            auto it = store.find(key);
            if (it == store.end()) {
                reply = "$-1\r\n"; // Missing key
            } else {
                const std::string& val = it->second;
                reply = "$" + std::to_string(val.size()) + "\r\n" + val + "\r\n";
            }
        }
            } else {
                reply = "-ERR wrong number of arguments for 'get' command\r\n";
            }
        }
        else if (cmd == "RPUSH") {
    if (a.elems.size() >= 3) {
        const std::string& key = a.elems[1];

        if (store.find(key) != store.end()) {
            reply = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
        } else {
            {
                std::lock_guard<std::mutex> lock(server_state.mtx);
                auto& vec = lists[key];
                vec.reserve(vec.size() + (a.elems.size() - 2));
                for (std::size_t i = 2; i < a.elems.size(); ++i) {
                    vec.push_back(a.elems[i]);
                }
                reply = ":" + std::to_string(vec.size()) + "\r\n";
            }
            
            // Notify any clients waiting on this list (outside the lock)
            server_state.waiting[key].notify_one(); // or notify_all() if multiple clients should be woken
        }
    } else {
        reply = "-ERR wrong number of arguments for 'rpush' command\r\n";
    }
}else if (cmd == "XADD") {
    if (a.elems.size() >= 5 && a.elems.size() % 2 == 1) {
        const std::string& key = a.elems[1];
        const std::string& id = a.elems[2];

        std::map<std::string, std::string> fields;
        for (size_t i = 3; i < a.elems.size(); i += 2) {
            const std::string& field = a.elems[i];
            const std::string& value = a.elems[i + 1];
            fields[field] = value;
        }

        streams[key].push_back(StreamEntry{ id, fields });

        reply = "$" + std::to_string(id.size()) + "\r\n" + id + "\r\n";
    } else {
        reply = "-ERR wrong number of arguments for 'XADD'\r\n";
    }
}





        else if (cmd == "LRANGE") {
            if (a.elems.size() == 4) {
                const std::string& key = a.elems[1];

                // Type check against string keys
                if (store.find(key) != store.end()) {
                    reply = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
                } else {
                    auto it = lists.find(key);
                    if (it == lists.end()) {
                        reply = "*0\r\n"; // missing key => empty array
                    } else {
                        const auto& vec = it->second;
                        const long long len = static_cast<long long>(vec.size());

                        long long start = 0, stop = 0;
                        if (!parse_ll(a.elems[2], start) || !parse_ll(a.elems[3], stop)) {
                            reply = "-ERR value is not an integer or out of range\r\n";
                        } else if (len == 0) {
                            reply = "*0\r\n"; // empty list
                        } else {
                            // Normalize negatives: -1 is last element
                            if (start < 0) start = len + start;
                            if (stop  < 0) stop  = len + stop;

                            // Clamp to [0, len-1]
                            if (start < 0) start = 0;
                            if (stop  < 0) stop  = 0;
                            if (start >= len) start = len;     // may exceed -> empty
                            if (stop  >= len) stop  = len - 1;

                            if (start > stop || start >= len) {
                                reply = "*0\r\n";
                            } else {
                                // Collect [start, stop] inclusive
                                std::vector<std::string> slice;
                                slice.reserve(static_cast<size_t>(stop - start + 1));
                                for (long long i = start; i <= stop; ++i) {
                                    slice.push_back(vec[static_cast<size_t>(i)]);
                                }
                                reply = bulk_array(slice);
                            }
                        }
                    }
                }
            } else {
                reply = "-ERR wrong number of arguments for 'lrange' command\r\n";
            }
        }

        else if (cmd == "LINDEX") {
            if (a.elems.size() == 3) {
                const std::string& key = a.elems[1];

                // Type check
                if (store.find(key) != store.end()) {
                    reply = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
                } else {
                    auto it = lists.find(key);
                    if (it == lists.end()) {
                        reply = "$-1\r\n"; // missing key -> null bulk
                    } else {
                        const auto& vec = it->second;
                        long long idx = 0;
                        if (!parse_ll(a.elems[2], idx)) {
                            reply = "-ERR value is not an integer or out of range\r\n";
                        } else {
                            long long len = static_cast<long long>(vec.size());
                            if (len == 0) {
                                reply = "$-1\r\n";
                            } else {
                                if (idx < 0) idx = len + idx; // negative index
                                if (idx < 0 || idx >= len) {
                                    reply = "$-1\r\n";        // OOB -> null bulk
                                } else {
                                    const std::string& v = vec[static_cast<size_t>(idx)];
                                    reply = bulk(v);          // $len\r\nv\r\n
                                }
                            }
                        }
                    }
                }
            } else {
                reply = "-ERR wrong number of arguments for 'lindex' command\r\n";
            }
        } else if (cmd == "LPUSH") {
    if (a.elems.size() >= 3) {
        const std::string& key = a.elems[1];

        if (store.find(key) != store.end()) {
            reply = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
        } else {
            {
                std::lock_guard<std::mutex> lock(server_state.mtx);
                auto& vec = lists[key];
                vec.reserve(vec.size() + (a.elems.size() - 2));
                for (std::size_t i = 2; i < a.elems.size(); ++i) {
                    vec.insert(vec.begin(), a.elems[i]);
                }
                reply = ":" + std::to_string(vec.size()) + "\r\n";
            }
            
            // Notify waiting clients
            server_state.waiting[key].notify_one();
        }
    }
}else if (cmd == "LLEN"){
            if (a.elems.size() == 2) {
            const std::string& key = a.elems[1];

        // Type check: if the key exists as a string, it's a WRONGTYPE error.
            if (store.find(key) != store.end()) {
                reply = ":0\r\n";
            } else {
                auto& vec = lists[key];
                reply = ":"+ std::to_string(vec.size()) +"\r\n";
                
        }
    }
} 

else if (cmd == "TYPE") {
    if (a.elems.size() == 2) {
        const std::string& key = a.elems[1];

        if (store.find(key) != store.end()) {
            reply = "+string\r\n";
        } else if (streams.find(key) != streams.end()) {
            reply = "+stream\r\n";
        } else {
            reply = "+none\r\n";
        }
    } else {
        reply = "-ERR wrong number of arguments for 'TYPE'\r\n";
    }
}

else if (cmd == "LPOP") {
    if (a.elems.size() == 2) {
        // LPOP key - single element
        const std::string& key = a.elems[1];
        
        if (lists.find(key) == lists.end() || lists[key].empty()) {
            reply = "$-1\r\n";  // Key doesn't exist or list is empty
        } else {
            auto& vec = lists[key];
            std::string first = vec.front();
            vec.erase(vec.begin());
            reply = "$" + std::to_string(first.size()) + "\r\n" + first + "\r\n";
            
            // Clean up empty list
            if (vec.empty()) {
                lists.erase(key);
            }
        }
    } 
    else if (a.elems.size() == 3) {
        // LPOP key count - multiple elements
        const std::string& key = a.elems[1];
        
        if (lists.find(key) == lists.end() || lists[key].empty()) {
            reply = "*0\r\n";  // Return empty array
        } else {
            auto& vec = lists[key];
            int count = std::stoi(a.elems[2]);
            int actualCount = std::min(count, static_cast<int>(vec.size()));
            
            // Build array response
            reply = "*" + std::to_string(actualCount) + "\r\n";
            
            for (int i = 0; i < actualCount; ++i) {
                std::string element = vec.front();
                vec.erase(vec.begin());
                reply += "$" + std::to_string(element.size()) + "\r\n" + element + "\r\n";
            }
            
            // Clean up empty list
            if (vec.empty()) {
                lists.erase(key);
            }
        }
    }
}
else if (cmd == "BLPOP") {
    if (a.elems.size() != 3) {
        reply = "-ERR wrong number of arguments for 'blpop' command\r\n";
    } else {
        const std::string& list_key = a.elems[1];
        
        double timeout_seconds = 0.0;
        if (!parse_timeout(a.elems[2], timeout_seconds)) {
            reply = "-ERR timeout is not a float or out of range\r\n";
        } else if (store.find(list_key) != store.end()) {
            reply = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
        } else {
            std::unique_lock<std::mutex> lock(server_state.mtx);
            
            // Check if list exists and has elements
            auto list_it = lists.find(list_key);
            if (list_it != lists.end() && !list_it->second.empty()) {
                // Immediate pop
                auto& list = list_it->second;
                std::string val = std::move(list.front());
                list.erase(list.begin());
                
                // Clean up empty list
                if (list.empty()) {
                    lists.erase(list_it);
                }
                
                // Format: array with [key, value]
                reply = "*2\r\n$" + std::to_string(list_key.size()) + "\r\n" + list_key + 
                       "\r\n$" + std::to_string(val.size()) + "\r\n" + val + "\r\n";
            } else {
                // Blocking mode
                auto& cond = server_state.waiting[list_key];
                
                if (timeout_seconds == 0.0) {
                    // Wait indefinitely
                    cond.wait(lock, [&]() { 
                        auto it = lists.find(list_key);
                        return it != lists.end() && !it->second.empty();
                    });
                } else {
                    // Wait with timeout - convert to milliseconds for precision
                    auto timeout_ms = std::chrono::duration<double, std::milli>(timeout_seconds * 1000.0);
                    auto end = std::chrono::steady_clock::now() + 
                              std::chrono::duration_cast<std::chrono::milliseconds>(timeout_ms);
                    
                    cond.wait_until(lock, end, [&]() { 
                        auto it = lists.find(list_key);
                        return it != lists.end() && !it->second.empty();
                    });
                }
                
                // Check if we got an element or timed out
                auto final_it = lists.find(list_key);
                if (final_it != lists.end() && !final_it->second.empty()) {
                    auto& list = final_it->second;
                    std::string val = std::move(list.front());
                    list.erase(list.begin());
                    
                    if (list.empty()) {
                        lists.erase(final_it);
                    }
                    
                    reply = "*2\r\n$" + std::to_string(list_key.size()) + "\r\n" + list_key + 
                           "\r\n$" + std::to_string(val.size()) + "\r\n" + val + "\r\n";
                } else {
                    reply = "$-1\r\n"; // Timeout occurred
                }
            }
        }
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
        std::string reply = handle_request(req, client_fd);  // Add client_fd parameter

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
