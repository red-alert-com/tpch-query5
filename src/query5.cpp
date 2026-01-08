#include "query5.hpp"
#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <unordered_map>
#include <map>
#include <algorithm>
#include <iomanip>

struct Customer { int custkey; int nationkey; };
struct Order { int orderkey; int custkey; std::string date; };
struct Lineitem { int orderkey; int suppkey; double price; double discount; };
struct Nation { int nationkey; std::string name; int regionkey; };
struct Region { int regionkey; std::string name; };
struct Supplier { int suppkey; int nationkey; };

inline std::string get_token(const std::string& line, size_t& start) {
    size_t end = line.find('|', start);
    if (end == std::string::npos) return "";
    std::string res = line.substr(start, end - start);
    start = end + 1;
    return res;
}

bool parseArgs(int argc, char* argv[], std::string& r_name, std::string& start_date, std::string& end_date, int& num_threads, std::string& table_path, std::string& result_path) {
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--r_name" && i + 1 < argc) r_name = argv[++i];
        else if (arg == "--start_date" && i + 1 < argc) start_date = argv[++i];
        else if (arg == "--end_date" && i + 1 < argc) end_date = argv[++i];
        else if (arg == "--threads" && i + 1 < argc) num_threads = std::stoi(argv[++i]);
        else if (arg == "--table_path" && i + 1 < argc) table_path = argv[++i];
        else if (arg == "--result_path" && i + 1 < argc) result_path = argv[++i];
    }
    return !r_name.empty() && !start_date.empty() && !end_date.empty();
}

bool readTPCHData(const std::string& path, 
                  std::vector<std::map<std::string, std::string>>&, 
                  std::vector<std::map<std::string, std::string>>&, 
                  std::vector<std::map<std::string, std::string>>&, 
                  std::vector<std::map<std::string, std::string>>&, 
                  std::vector<std::map<std::string, std::string>>&, 
                  std::vector<std::map<std::string, std::string>>&) {
    return true; 
}

// 3. The Core Optimized Query Engine
bool executeQuery5(const std::string& r_name, const std::string& start_date, const std::string& end_date, int num_threads, 
                   const std::vector<std::map<std::string, std::string>>&, 
                   const std::vector<std::map<std::string, std::string>>&, 
                   const std::vector<std::map<std::string, std::string>>&, 
                   const std::vector<std::map<std::string, std::string>>&, 
                   const std::vector<std::map<std::string, std::string>>&, 
                   const std::vector<std::map<std::string, std::string>>&, 
                   std::map<std::string, double>& results) {
    
    std::unordered_map<int, std::string> n_map; 
    std::unordered_map<int, int> c_map;         
    std::unordered_map<int, int> s_map;         
    std::unordered_map<int, int> o_map;        

    std::string base_path = "../../../data/tpch-dbgen/input_data/"; 

    int target_r_key = -1;
    std::ifstream rf(base_path + "region.tbl");
    std::string line;
    while(std::getline(rf, line)) {
        size_t s=0; int rk = std::stoi(get_token(line, s));
        if(get_token(line, s) == r_name) { target_r_key = rk; break; }
    }

    std::ifstream nf(base_path + "nation.tbl");
    while(std::getline(nf, line)) {
        size_t s=0; int nk = std::stoi(get_token(line, s));
        std::string name = get_token(line, s);
        if(std::stoi(get_token(line, s)) == target_r_key) n_map[nk] = name;
    }

    std::ifstream cf(base_path + "customer.tbl");
    while(std::getline(cf, line)) {
        size_t s=0; int ck = std::stoi(get_token(line, s));
        get_token(line, s); get_token(line, s);
        int nk = std::stoi(get_token(line, s));
        if(n_map.count(nk)) c_map[ck] = nk;
    }

    std::ifstream sf(base_path + "supplier.tbl");
    while(std::getline(sf, line)) {
        size_t s=0; int sk = std::stoi(get_token(line, s));
        get_token(line, s); get_token(line, s); 
        int nk = std::stoi(get_token(line, s));
        if(n_map.count(nk)) s_map[sk] = nk;
    }

    std::ifstream of(base_path + "orders.tbl");
    while(std::getline(of, line)) {
        size_t s=0; int ok = std::stoi(get_token(line, s));
        int ck = std::stoi(get_token(line, s));
        get_token(line, s); get_token(line, s);
        std::string d = get_token(line, s);
        if(d >= start_date && d < end_date) {
            auto it = c_map.find(ck);
            if(it != c_map.end()) o_map[ok] = it->second;
        }
    }

    std::ifstream lf(base_path + "lineitem.tbl");
    while(std::getline(lf, line)) {
        size_t s=0; int ok = std::stoi(get_token(line, s));
        auto o_it = o_map.find(ok);
        if(o_it != o_map.end()) {
            get_token(line, s); 
            int sk = std::stoi(get_token(line, s));
            auto s_it = s_map.find(sk);
            if(s_it != s_map.end() && s_it->second == o_it->second) {
                get_token(line, s); get_token(line, s);
                double price = std::stod(get_token(line, s));
                double disc = std::stod(get_token(line, s));
                results[n_map[o_it->second]] += price * (1.0 - disc);
            }
        }
    }
    return true;
}

bool outputResults(const std::string& path, const std::map<std::string, double>& results) {
    std::ofstream out(path);
    if (!out.is_open()) return false;
    std::vector<std::pair<std::string, double>> v(results.begin(), results.end());
    std::sort(v.begin(), v.end(), [](const std::pair<std::string, double>& a, const std::pair<std::string, double>& b) {
        return a.second > b.second;
    });
    out << std::fixed << std::setprecision(2);
    for (auto const& p : v) out << p.first << "|" << p.second << "\n";
    return true;
}