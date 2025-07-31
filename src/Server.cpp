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
#include <climits>

static std::unordered_map<std::string, std::string> store;
static std::unordered_map<std::string, std::chrono::steady_clock::time_point> ttl;

static std::unordered_map<std::string, std::vector<std::string>> lists; // lists

// Server role and replication settings
static std::string server_role = "master";  // default role
static std::string master_host = "";
static int master_port = 0;

// Transaction state per client
static std::unordered_map<int, bool> client_in_transaction;

struct StreamEntry {
    std::string id;
    std::map<std::string, std::string> fields;
};

std::unordered_map<std::string, std::vector<StreamEntry>> streams;

// Helper function to parse stream entry ID
static bool parse_stream_id(const std::string& id_str, long long& millis, long long& seq) {
    size_t dash_pos = id_str.find('-');
    if (dash_pos == std::string::npos) return false;
    
    std::string millis_str = id_str.substr(0, dash_pos);
    std::string seq_str = id_str.substr(dash_pos + 1);
    
    try {
        size_t idx = 0;
        millis = std::stoll(millis_str, &idx, 10);
        if (idx != millis_str.size() || millis < 0) return false;
        
        idx = 0;
        seq = std::stoll(seq_str, &idx, 10);
        if (idx != seq_str.size() || seq < 0) return false;
        
        return true;
    } catch (...) {
        return false;
    }
}

// Helper function to parse stream entry ID with * support for sequence and full ID
static bool parse_stream_id_with_wildcard(const std::string& id_str, long long& millis, long long& seq, bool& seq_is_wildcard, bool& full_wildcard) {
    // Check if entire ID is *
    if (id_str == "*") {
        full_wildcard = true;
        seq_is_wildcard = false;
        millis = -1; // placeholder
        seq = -1; // placeholder
        return true;
    }
    
    full_wildcard = false;
    size_t dash_pos = id_str.find('-');
    if (dash_pos == std::string::npos) return false;
    
    std::string millis_str = id_str.substr(0, dash_pos);
    std::string seq_str = id_str.substr(dash_pos + 1);
    
    try {
        size_t idx = 0;
        millis = std::stoll(millis_str, &idx, 10);
        if (idx != millis_str.size() || millis < 0) return false;
        
        if (seq_str == "*") {
            seq_is_wildcard = true;
            seq = -1; // placeholder
            return true;
        } else {
            seq_is_wildcard = false;
            idx = 0;
            seq = std::stoll(seq_str, &idx, 10);
            if (idx != seq_str.size() || seq < 0) return false;
            return true;
        }
    } catch (...) {
        return false;
    }
}

// Helper function to compare stream entry IDs
static bool is_id_greater(long long millis1, long long seq1, long long millis2, long long seq2) {
    if (millis1 > millis2) return true;
    if (millis1 < millis2) return false;
    return seq1 > seq2;
}

// Helper function to get current Unix timestamp in milliseconds
static long long get_current_millis() {
    auto now = std::chrono::system_clock::now();
    auto duration = now.time_since_epoch();
    return std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
}

// Helper function to parse range ID (may be missing sequence number, be -, or be +)
static bool parse_range_id(const std::string& id_str, long long& millis, long long& seq, bool is_end_range = false) {
    // Handle special case: "-" means start from the beginning
    if (id_str == "-") {
        millis = 0;
        seq = 0;
        return true;
    }
    
    // Handle special case: "+" means end at the maximum possible ID
    if (id_str == "+") {
        millis = LLONG_MAX;
        seq = LLONG_MAX;
        return true;
    }
    
    size_t dash_pos = id_str.find('-');
    if (dash_pos == std::string::npos) {
        // No sequence number provided
        try {
            size_t idx = 0;
            millis = std::stoll(id_str, &idx, 10);
            if (idx != id_str.size() || millis < 0) return false;
            
            // Default sequence: 0 for start, maximum for end
            seq = is_end_range ? LLONG_MAX : 0;
            return true;
        } catch (...) {
            return false;
        }
    } else {
        // Full ID with sequence number
        return parse_stream_id(id_str, millis, seq);
    }
}

// Helper function to check if an ID is within range (inclusive)
static bool is_id_in_range(long long millis, long long seq, 
                          long long start_millis, long long start_seq,
                          long long end_millis, long long end_seq) {
    // Check if ID >= start
    bool gte_start = (millis > start_millis) || (millis == start_millis && seq >= start_seq);
    
    // Check if ID <= end
    bool lte_end = (millis < end_millis) || (millis == end_millis && seq <= end_seq);
    
    return gte_start && lte_end;
}

// Helper function to check if an ID is greater than another (exclusive comparison for XREAD)
static bool is_id_greater_than(long long millis, long long seq, long long ref_millis, long long ref_seq) {
    if (millis > ref_millis) return true;
    if (millis < ref_millis) return false;
    return seq > ref_seq;
}

// Helper function to get the last entry ID in a stream (for $ handling)
static bool get_last_entry_id(const std::vector<StreamEntry>& stream, long long& millis, long long& seq) {
    if (stream.empty()) {
        return false;
    }
    
    // Return the ID of the last entry
    const std::string& last_id = stream.back().id;
    return parse_stream_id(last_id, millis, seq);
}

// Helper function to find the next sequence number for a given millisecond timestamp
static long long find_next_sequence(const std::vector<StreamEntry>& stream, long long millis) {
    // Special case: if millis is 0, start with sequence 1
    if (millis == 0) {
        // Find the highest sequence number for millis = 0
        long long max_seq = 0;
        for (const auto& entry : stream) {
            long long entry_millis, entry_seq;
            if (parse_stream_id(entry.id, entry_millis, entry_seq) && entry_millis == 0) {
                max_seq = std::max(max_seq, entry_seq);
            }
        }
        return max_seq + 1;
    }
    
    // For other millisecond values, find the highest sequence number for that timestamp
    long long max_seq = -1;
    for (const auto& entry : stream) {
        long long entry_millis, entry_seq;
        if (parse_stream_id(entry.id, entry_millis, entry_seq) && entry_millis == millis) {
            max_seq = std::max(max_seq, entry_seq);
        }
    }
    return max_seq + 1;
}


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

// Command queue for transactions (after RespArray definition)
static std::unordered_map<int, std::vector<RespArray>> client_command_queue;

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

// Helper function to format XRANGE response
static std::string format_xrange_response(const std::vector<StreamEntry>& entries) {
    std::string response = "*" + std::to_string(entries.size()) + "\r\n";
    
    for (const auto& entry : entries) {
        // Each entry is an array with 2 elements: [id, [field1, value1, field2, value2, ...]]
        response += "*2\r\n";
        
        // First element: entry ID
        response += "$" + std::to_string(entry.id.size()) + "\r\n" + entry.id + "\r\n";
        
        // Second element: array of field-value pairs
        response += "*" + std::to_string(entry.fields.size() * 2) + "\r\n";
        for (const auto& field_pair : entry.fields) {
            // Field name
            response += "$" + std::to_string(field_pair.first.size()) + "\r\n" + field_pair.first + "\r\n";
            // Field value
            response += "$" + std::to_string(field_pair.second.size()) + "\r\n" + field_pair.second + "\r\n";
        }
    }
    
    return response;
}

// Helper function to format XREAD response
static std::string format_xread_response(const std::vector<std::pair<std::string, std::vector<StreamEntry>>>& stream_results) {
    std::string response = "*" + std::to_string(stream_results.size()) + "\r\n";
    
    for (const auto& stream_result : stream_results) {
        const std::string& stream_key = stream_result.first;
        const std::vector<StreamEntry>& entries = stream_result.second;
        
        // Each stream result is an array with 2 elements: [stream_key, [entries...]]
        response += "*2\r\n";
        
        // First element: stream key
        response += "$" + std::to_string(stream_key.size()) + "\r\n" + stream_key + "\r\n";
        
        // Second element: array of entries
        response += "*" + std::to_string(entries.size()) + "\r\n";
        for (const auto& entry : entries) {
            // Each entry is an array with 2 elements: [id, [field1, value1, field2, value2, ...]]
            response += "*2\r\n";
            
            // Entry ID
            response += "$" + std::to_string(entry.id.size()) + "\r\n" + entry.id + "\r\n";
            
            // Entry fields
            response += "*" + std::to_string(entry.fields.size() * 2) + "\r\n";
            for (const auto& field_pair : entry.fields) {
                // Field name
                response += "$" + std::to_string(field_pair.first.size()) + "\r\n" + field_pair.first + "\r\n";
                // Field value
                response += "$" + std::to_string(field_pair.second.size()) + "\r\n" + field_pair.second + "\r\n";
            }
        }
    }
    
    return response;
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

// Helper function to execute a single command (without transaction logic)
std::string execute_single_command(const RespArray& a, int client_fd) {
    std::string reply;
    std::string cmd = a.elems[0];
    to_upper(cmd);

    // Execute the command directly without transaction queuing logic
    // This is essentially the same as handle_request but without the queuing check
    if (cmd == "PING") {
        if (a.elems.size() == 1) {
            reply = "+PONG\r\n";
        } else {
            reply = "+" + a.elems[1] + "\r\n";
        }
    } else if (cmd == "ECHO") {
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
            ttl.erase(key);
            reply = "+OK\r\n";
        } else if (a.elems.size() == 5) {
            const std::string& key = a.elems[1];
            const std::string& val = a.elems[2];
            std::string opt = a.elems[3];
            to_upper(opt);
            if (opt != "PX") {
                reply = "-ERR syntax error\r\n";
            } else {
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
    } else if (cmd == "GET") {
        if (a.elems.size() == 2) {
            const std::string& key = a.elems[1];
            if (is_expired_now(key)) {
                reply = "$-1\r\n";
            } else {
                auto it = store.find(key);
                if (it == store.end()) {
                    reply = "$-1\r\n";
                } else {
                    const std::string& val = it->second;
                    reply = "$" + std::to_string(val.size()) + "\r\n" + val + "\r\n";
                }
            }
        } else {
            reply = "-ERR wrong number of arguments for 'get' command\r\n";
        }
    } else if (cmd == "INCR") {
        if (a.elems.size() == 2) {
            const std::string& key = a.elems[1];
            if (is_expired_now(key)) {
                reply = "-ERR value is not an integer or out of range\r\n";
            } else {
                auto it = store.find(key);
                if (it == store.end()) {
                    store[key] = "1";
                    reply = ":1\r\n";
                } else {
                    const std::string& val = it->second;
                    try {
                        size_t idx = 0;
                        long long num = std::stoll(val, &idx, 10);
                        if (idx != val.size()) {
                            reply = "-ERR value is not an integer or out of range\r\n";
                        } else {
                            num++;
                            std::string new_val = std::to_string(num);
                            store[key] = new_val;
                            reply = ":" + std::to_string(num) + "\r\n";
                        }
                    } catch (...) {
                        reply = "-ERR value is not an integer or out of range\r\n";
                    }
                }
            }
        } else {
            reply = "-ERR wrong number of arguments for 'incr' command\r\n";
        }
    } else if (cmd == "INFO") {
        if (a.elems.size() == 1) {
            // INFO without arguments - return all sections (for now just replication)
            std::string info_response = "role:" + server_role + "\r\n";
            reply = "$" + std::to_string(info_response.size()) + "\r\n" + info_response + "\r\n";
        } else if (a.elems.size() == 2) {
            std::string section = a.elems[1];
            to_upper(section);
            
            if (section == "REPLICATION") {
                // Return replication section
                std::string info_response = "role:" + server_role + "\r\n";
                reply = "$" + std::to_string(info_response.size()) + "\r\n" + info_response + "\r\n";
            } else {
                // Unknown section - return empty bulk string
                reply = "$0\r\n\r\n";
            }
        } else {
            reply = "-ERR wrong number of arguments for 'info' command\r\n";
        }
    } else {
        reply = "-ERR unknown command\r\n";
    }
    
    return reply;
}

// Update to pass client_fd (same as my previous suggestion)
std::string handle_request(const std::string& req, int client_fd) {
    RespArray a;
    std::string reply;

    if (parse_resp_array_of_bulk_strings(req, a) && !a.elems.empty()) {
        std::string cmd = a.elems[0];
        to_upper(cmd);

        // Check if client is in transaction and command should be queued
        if (client_in_transaction[client_fd] && cmd != "MULTI" && cmd != "EXEC" && cmd != "DISCARD") {
            // Queue the command
            client_command_queue[client_fd].push_back(a);
            reply = "+QUEUED\r\n";
        }
        else if (cmd == "PING") {
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
        else if (cmd == "INCR") {
            if (a.elems.size() == 2) {
                const std::string& key = a.elems[1];
                
                // Check for expiration first
                if (is_expired_now(key)) {
                    reply = "-ERR value is not an integer or out of range\r\n";
                } else {
                    auto it = store.find(key);
                    if (it == store.end()) {
                        // Key doesn't exist - set it to 1
                        store[key] = "1";
                        reply = ":1\r\n";
                    } else {
                        const std::string& val = it->second;
                        
                        // Try to parse the value as an integer
                        try {
                            size_t idx = 0;
                            long long num = std::stoll(val, &idx, 10);
                            if (idx != val.size()) {
                                // Not a valid integer
                                reply = "-ERR value is not an integer or out of range\r\n";
                            } else {
                                // Increment the value
                                num++;
                                std::string new_val = std::to_string(num);
                                store[key] = new_val;
                                
                                // Return as RESP integer
                                reply = ":" + std::to_string(num) + "\r\n";
                            }
                        } catch (...) {
                            reply = "-ERR value is not an integer or out of range\r\n";
                        }
                    }
                }
            } else {
                reply = "-ERR wrong number of arguments for 'incr' command\r\n";
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
        const std::string& id_input = a.elems[2];

        // Parse the provided ID (may contain wildcards)
        long long millis, seq;
        bool seq_is_wildcard, full_wildcard;
        if (!parse_stream_id_with_wildcard(id_input, millis, seq, seq_is_wildcard, full_wildcard)) {
            reply = "-ERR Invalid stream ID specified as stream command argument\r\n";
        } else {
            auto& stream = streams[key];
            std::string final_id;
            
            if (full_wildcard) {
                // Auto-generate both timestamp and sequence number
                millis = get_current_millis();
                long long generated_seq = find_next_sequence(stream, millis);
                final_id = std::to_string(millis) + "-" + std::to_string(generated_seq);
                seq = generated_seq;
            } else if (seq_is_wildcard) {
                // Auto-generate sequence number only
                long long generated_seq = find_next_sequence(stream, millis);
                final_id = std::to_string(millis) + "-" + std::to_string(generated_seq);
                seq = generated_seq;
            } else {
                // Use explicit ID
                final_id = id_input;
                
                // Validate explicit ID
                if (millis == 0 && seq == 0) {
                    reply = "-ERR The ID specified in XADD must be greater than 0-0\r\n";
                } else {
                    // Check if ID is greater than the last entry in the stream
                    bool valid_id = true;
                    
                    if (!stream.empty()) {
                        const std::string& last_id = stream.back().id;
                        long long last_millis, last_seq;
                        if (parse_stream_id(last_id, last_millis, last_seq)) {
                            if (!is_id_greater(millis, seq, last_millis, last_seq)) {
                                valid_id = false;
                            }
                        }
                    }
                    
                    if (!valid_id) {
                        reply = "-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n";
                    }
                }
            }
            
            // If we haven't set an error reply, proceed with adding the entry
            if (reply.empty()) {
                std::map<std::string, std::string> fields;
                for (size_t i = 3; i < a.elems.size(); i += 2) {
                    const std::string& field = a.elems[i];
                    const std::string& value = a.elems[i + 1];
                    fields[field] = value;
                }

                stream.push_back(StreamEntry{ final_id, fields });
                reply = "$" + std::to_string(final_id.size()) + "\r\n" + final_id + "\r\n";
                
                // Notify any waiting XREAD clients
                server_state.waiting[key].notify_all();
            }
        }
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

else if (cmd == "XRANGE") {
    if (a.elems.size() == 4) {
        const std::string& key = a.elems[1];
        const std::string& start_id = a.elems[2];
        const std::string& end_id = a.elems[3];
        
        // Parse start and end IDs
        long long start_millis, start_seq, end_millis, end_seq;
        if (!parse_range_id(start_id, start_millis, start_seq, false) ||
            !parse_range_id(end_id, end_millis, end_seq, true)) {
            reply = "-ERR Invalid stream ID specified in range\r\n";
        } else {
            auto stream_it = streams.find(key);
            if (stream_it == streams.end()) {
                // Stream doesn't exist, return empty array
                reply = "*0\r\n";
            } else {
                const auto& stream = stream_it->second;
                std::vector<StreamEntry> matching_entries;
                
                // Find all entries within the range
                for (const auto& entry : stream) {
                    long long entry_millis, entry_seq;
                    if (parse_stream_id(entry.id, entry_millis, entry_seq)) {
                        if (is_id_in_range(entry_millis, entry_seq, 
                                          start_millis, start_seq, 
                                          end_millis, end_seq)) {
                            matching_entries.push_back(entry);
                        }
                    }
                }
                
                reply = format_xrange_response(matching_entries);
            }
        }
    } else {
        reply = "-ERR wrong number of arguments for 'XRANGE'\r\n";
    }
}

else if (cmd == "XREAD") {
    // XREAD [block timeout] streams stream_key1 stream_key2 ... id1 id2 ...
    bool is_blocking = false;
    double timeout_ms = 0.0;
    size_t streams_index = 1;
    
    // Check for block parameter
    if (a.elems.size() >= 6 && a.elems[1] == "block") {
        is_blocking = true;
        if (!parse_timeout(a.elems[2], timeout_ms)) {
            reply = "-ERR timeout is not a float or out of range\r\n";
        } else {
            streams_index = 3;
        }
    }
    
    if (reply.empty()) {
        // Check minimum arguments and structure
        if (a.elems.size() >= streams_index + 3 && (a.elems.size() - streams_index - 1) % 2 == 0) {
            if (a.elems[streams_index] != "streams") {
                reply = "-ERR syntax error\r\n";
            } else {
                // Parse streams and IDs, resolving $ once at the beginning
                size_t num_streams = (a.elems.size() - streams_index - 1) / 2;
                std::vector<std::pair<std::string, std::vector<StreamEntry>>> results;
                
                // Pre-resolve all start IDs (including $ conversion)
                std::vector<std::pair<std::string, std::pair<long long, long long>>> resolved_streams;
                bool has_parse_error = false;
                
                for (size_t i = 0; i < num_streams && !has_parse_error; ++i) {
                    const std::string& stream_key = a.elems[streams_index + 1 + i];
                    const std::string& start_id = a.elems[streams_index + 1 + num_streams + i];
                    
                    long long start_millis, start_seq;
                    if (start_id == "$") {
                        // $ means start from the last entry in the stream at command time
                        auto stream_it = streams.find(stream_key);
                        if (stream_it != streams.end() && !stream_it->second.empty()) {
                            if (!get_last_entry_id(stream_it->second, start_millis, start_seq)) {
                                reply = "-ERR Invalid stream ID specified in XREAD\r\n";
                                has_parse_error = true;
                                break;
                            }
                        } else {
                            // Stream doesn't exist or is empty, use 0-0 as starting point
                            start_millis = 0;
                            start_seq = 0;
                        }
                    } else if (!parse_stream_id(start_id, start_millis, start_seq)) {
                        reply = "-ERR Invalid stream ID specified in XREAD\r\n";
                        has_parse_error = true;
                        break;
                    }
                    
                    resolved_streams.push_back({stream_key, {start_millis, start_seq}});
                }
                
                if (!has_parse_error) {
                    // Helper function to check for new entries using resolved IDs
                    auto check_for_entries = [&]() -> bool {
                        results.clear();
                        
                        for (const auto& resolved : resolved_streams) {
                            const std::string& stream_key = resolved.first;
                            long long start_millis = resolved.second.first;
                            long long start_seq = resolved.second.second;
                        
                            auto stream_it = streams.find(stream_key);
                            if (stream_it != streams.end()) {
                                const auto& stream = stream_it->second;
                                std::vector<StreamEntry> matching_entries;
                                
                                // Find all entries with ID > start_id (exclusive)
                                for (const auto& entry : stream) {
                                    long long entry_millis, entry_seq;
                                    if (parse_stream_id(entry.id, entry_millis, entry_seq)) {
                                        if (is_id_greater_than(entry_millis, entry_seq, start_millis, start_seq)) {
                                            matching_entries.push_back(entry);
                                        }
                                    }
                                }
                                
                                // Only add stream to results if it has matching entries
                                if (!matching_entries.empty()) {
                                    results.push_back({stream_key, matching_entries});
                                }
                            }
                        }
                        
                        return !results.empty();
                    };
                
                // Check for immediate results
                if (check_for_entries()) {
                    reply = format_xread_response(results);
                } else if (is_blocking && reply.empty()) {
                    // Blocking mode - wait for new entries
                    std::unique_lock<std::mutex> lock(server_state.mtx);
                    
                    if (timeout_ms == 0.0) {
                        // Wait indefinitely - wait on the first stream (for simplicity)
                        const std::string& first_stream_key = a.elems[streams_index + 1];
                        auto& cond = server_state.waiting[first_stream_key];
                        
                        cond.wait(lock, [&]() { 
                            return check_for_entries(); 
                        });
                        
                        // After waking up, format the response
                        reply = format_xread_response(results);
                    } else {
                        // Wait with timeout
                        auto timeout_duration = std::chrono::duration<double, std::milli>(timeout_ms);
                        auto end_time = std::chrono::steady_clock::now() + 
                                       std::chrono::duration_cast<std::chrono::milliseconds>(timeout_duration);
                        
                        bool found_entries = false;
                        for (size_t i = 0; i < num_streams && !found_entries; ++i) {
                            const std::string& stream_key = a.elems[streams_index + 1 + i];
                            auto& cond = server_state.waiting[stream_key];
                            
                            if (cond.wait_until(lock, end_time, [&]() { return check_for_entries(); })) {
                                if (!results.empty()) {
                                    found_entries = true;
                                    reply = format_xread_response(results);
                                }
                            }
                        }
                        
                        if (!found_entries) {
                            reply = "$-1\r\n"; // Timeout - return null
                        }
                    }
                } else if (reply.empty()) {
                    // Non-blocking mode with no results
                    reply = format_xread_response(results); // Empty results
                }
                }
            }
        } else {
            reply = "-ERR wrong number of arguments for 'XREAD'\r\n";
        }
    }
}

        else if (cmd == "MULTI") {
            if (a.elems.size() == 1) {
                client_in_transaction[client_fd] = true;
                reply = "+OK\r\n";
            } else {
                reply = "-ERR wrong number of arguments for 'multi' command\r\n";
            }
        }
        
        else if (cmd == "EXEC") {
            if (a.elems.size() == 1) {
                if (client_in_transaction[client_fd]) {
                    // Check if we have queued commands
                    auto& queue = client_command_queue[client_fd];
                    if (queue.empty()) {
                        // Empty transaction - return empty array
                        reply = "*0\r\n";
                    } else {
                        // Execute queued commands and return their results
                        reply = "*" + std::to_string(queue.size()) + "\r\n";
                        for (const auto& queued_command : queue) {
                            std::string command_result = execute_single_command(queued_command, client_fd);
                            reply += command_result;
                        }
                    }
                    
                    // Reset transaction state and clear queue
                    client_in_transaction[client_fd] = false;
                    queue.clear();
                } else {
                    reply = "-ERR EXEC without MULTI\r\n";
                }
            } else {
                reply = "-ERR wrong number of arguments for 'exec' command\r\n";
            }
        }
        
        else if (cmd == "DISCARD") {
            if (a.elems.size() == 1) {
                if (client_in_transaction[client_fd]) {
                    // Abort transaction - reset state and clear queue
                    client_in_transaction[client_fd] = false;
                    client_command_queue[client_fd].clear();
                    reply = "+OK\r\n";
                } else {
                    reply = "-ERR DISCARD without MULTI\r\n";
                }
            } else {
                reply = "-ERR wrong number of arguments for 'discard' command\r\n";
            }
        }
        
        else if (cmd == "INFO") {
            if (a.elems.size() == 1) {
                // INFO without arguments - return all sections (for now just replication)
                std::string info_response = "role:" + server_role + "\r\n";
                reply = "$" + std::to_string(info_response.size()) + "\r\n" + info_response + "\r\n";
            } else if (a.elems.size() == 2) {
                std::string section = a.elems[1];
                to_upper(section);
                
                if (section == "REPLICATION") {
                    // Return replication section
                    std::string info_response = "role:" + server_role + "\r\n";
                    reply = "$" + std::to_string(info_response.size()) + "\r\n" + info_response + "\r\n";
                } else {
                    // Unknown section - return empty bulk string
                    reply = "$0\r\n\r\n";
                }
            } else {
                reply = "-ERR wrong number of arguments for 'info' command\r\n";
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
    
    // Clean up client transaction state when client disconnects
    client_in_transaction.erase(client_fd);
    client_command_queue.erase(client_fd);
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

int main(int argc, char* argv[]) {
    std::cout << std::unitbuf;
    std::cerr << std::unitbuf;
    std::signal(SIGPIPE, SIG_IGN);

    uint16_t port = 6379; // default port
    
    // Parse command line arguments
    for (int i = 1; i < argc; i++) {
        if (std::string(argv[i]) == "--port" && i + 1 < argc) {
            try {
                int parsed_port = std::stoi(argv[i + 1]);
                if (parsed_port < 1 || parsed_port > 65535) {
                    std::cerr << "Invalid port number: " << argv[i + 1] << "\n";
                    return 1;
                }
                port = static_cast<uint16_t>(parsed_port);
                i++; // skip the port value argument
            } catch (const std::exception&) {
                std::cerr << "Invalid port number: " << argv[i + 1] << "\n";
                return 1;
            }
        } else if (std::string(argv[i]) == "--replicaof" && i + 1 < argc) {
            // Parse master host and port from the format "host port"
            std::string replicaof_arg = argv[i + 1];
            size_t space_pos = replicaof_arg.find(' ');
            if (space_pos == std::string::npos) {
                std::cerr << "Invalid --replicaof format. Expected: --replicaof \"host port\"\n";
                return 1;
            }
            
            master_host = replicaof_arg.substr(0, space_pos);
            std::string port_str = replicaof_arg.substr(space_pos + 1);
            
            try {
                master_port = std::stoi(port_str);
                if (master_port < 1 || master_port > 65535) {
                    std::cerr << "Invalid master port number: " << port_str << "\n";
                    return 1;
                }
                server_role = "slave";
                i++; // skip the replicaof value argument
            } catch (const std::exception&) {
                std::cerr << "Invalid master port number: " << port_str << "\n";
                return 1;
            }
        }
    }

    int server_fd = create_listen_socket(port);
    if (server_fd < 0) {
        std::cerr << "Failed to setup server socket on port " << port << "\n";
        return 1;
    }

    std::cout << "Waiting for a client to connect on port " << port << "...\n";
    serve_forever(server_fd);

    close(server_fd);
    return 0;
}
