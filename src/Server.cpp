#include <iostream>
#include <string>
#include <string_view>
#include <vector>
#include <algorithm>
#include <csignal>
#include <cstring>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>

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
    std::transform(s.begin(), s.end(), s.begin(), [](unsigned char c){ return std::toupper(c); });
}

// Very small RESP parser for arrays of bulk strings only; good enough for "*1\r\n$4\r\nPING\r\n"
struct RespArray {
    std::vector<std::string> elems;
};

static bool parse_int(const std::string& s, size_t& pos, long& out) {
    // expects digits until \r\n
    size_t start = pos;
    bool neg = false;
    if (pos < s.size() && s[pos] == '-') { neg = true; ++pos; }
    long val = 0;
    while (pos < s.size() && std::isdigit(static_cast<unsigned char>(s[pos]))) {
        val = val * 10 + (s[pos] - '0');
        ++pos;
    }
    if (pos + 1 >= s.size() || s[pos] != '\r' || s[pos+1] != '\n') return false;
    pos += 2;
    out = neg ? -val : val;
    return pos > start + 2; // read something
}

static bool parse_bulk_string(const std::string& s, size_t& pos, std::string& out) {
    if (pos >= s.size() || s[pos] != '$') return false;
    ++pos;
    long len = 0;
    if (!parse_int(s, pos, len)) return false;
    if (len < 0) { out.clear(); return true; } // Null bulk string, not needed here
    if (static_cast<size_t>(pos + len + 2) > s.size()) return false;
    out.assign(s.data() + pos, static_cast<size_t>(len));
    pos += static_cast<size_t>(len);
    if (pos + 1 >= s.size() || s[pos] != '\r' || s[pos+1] != '\n') return false;
    pos += 2;
    return true;
}

static bool parse_resp_array_of_bulk_strings(const std::string& s, RespArray& arr) {
    size_t pos = 0;
    if (s.empty() || s[0] != '*') return false;
    ++pos;
    long n = 0;
    if (!parse_int(s, pos, n) || n < 0) return false;
    arr.elems.clear();
    arr.elems.reserve(static_cast<size_t>(n));
    for (long i = 0; i < n; ++i) {
        std::string bs;
        if (!parse_bulk_string(s, pos, bs)) return false;
        arr.elems.push_back(std::move(bs));
    }
    return true;
}

int main() {
    std::cout << std::unitbuf;
    std::cerr << std::unitbuf;

    std::signal(SIGPIPE, SIG_IGN);

    int server_fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) { std::cerr << "Failed to create server socket\n"; return 1; }

    int reuse = 1;
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0) {
        std::cerr << "setsockopt failed\n";
        return 1;
    }

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons(6379);

    if (bind(server_fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
        std::cerr << "Failed to bind to port 6379\n";
        return 1;
    }

    if (listen(server_fd, 16) != 0) {
        std::cerr << "listen failed\n";
        return 1;
    }

    std::cout << "Waiting for a client to connect...\n";
    sockaddr_in caddr{};
    socklen_t clen = sizeof(caddr);
    int client_fd = ::accept(server_fd, reinterpret_cast<sockaddr*>(&caddr), &clen);
    if (client_fd < 0) {
        std::cerr << "accept failed\n";
        return 1;
    }
    std::cout << "Client connected\n";

    // Read once (simple test harness). For real-world, loop and accumulate until full command is received.
    char buf[4096];
    ssize_t n = ::recv(client_fd, buf, sizeof(buf), 0);
    if (n <= 0) {
        std::cerr << "recv failed\n";
        close(client_fd);
        close(server_fd);
        return 1;
    }
    std::string req(buf, buf + n);

    RespArray a;
    std::string reply;
    if (parse_resp_array_of_bulk_strings(req, a) && !a.elems.empty()) {
        std::string cmd = a.elems[0];
        to_upper(cmd);
        if (cmd == "PING") {
            if (a.elems.size() == 1) {
                reply = "+PONG\r\n";  // what your tester expects
            } else {
                // Redis returns the message as a bulk string; but if your tester wants simple string, adapt accordingly.
                reply = "+" + a.elems[1] + "\r\n";
            }
        } else {
            reply = "-ERR unknown command\r\n";
        }
    } else {
        // Very naive fallback: if you ever get the inline "PING\r\n" form
        if (req.rfind("PING", 0) == 0) {
            reply = "+PONG\r\n";
        } else {
            reply = "-ERR protocol error\r\n";
        }
    }

    (void)send_all(client_fd, reply.data(), reply.size());
    close(client_fd);
    close(server_fd);
    return 0;
}
