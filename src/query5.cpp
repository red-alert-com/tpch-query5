#include "query5.hpp"
#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <unordered_map>
#include <map>
#include <algorithm>
#include <iomanip>
#include <thread>
#include <mutex>

std::mutex results_mutex;

inline std::string get_token(const std::string& line, size_t& start) {
    size_t end = line.find('|', start);
    if (end == std::string::npos) return "";
    std::string res = line.substr(start, end - start);
    start = end + 1;
    return res;
}

// Thread function to process a specific byte range of lineitem.tbl
void process_lineitem_chunk(const std::string& path, long start_byte, long end_byte,
                            const std::unordered_map<int, int>& o_map,
                            const std::unordered_map<int, int>& s_map,
                            const std::unordered_map<int, std::string>& n_map,
                            std::map<std::string, double>& global_results) {
    
    std::ifstream file(path);
    file.seekg(start_byte);
    
    std::string line;
    if (start_byte != 0) std::getline(file, line);

    std::map<std::string, double> local_results;

    while (file.tellg() < end_byte && std::getline(file, line)) {
        if (line.empty()) continue;
        size_t s = 0;
        int ok = std::stoi(get_token(line, s));
        
        auto o_it = o_map.find(ok);
        if (o_it != o_map.end()) {
            get_token(line, s); 
            int sk = std::stoi(get_token(line, s));
            
            auto s_it = s_map.find(sk);
            if (s_it != s_map.end() && s_it->second == o_it->second) {
                get_token(line, s); get_token(line, s);
                double price = std::stod(get_token(line, s));
                double disc = std::stod(get_token(line, s));
                
                local_results[n_map.at(o_it->second)] += price * (1.0 - disc);
            }
        }
    }

    std::lock_guard<std::mutex> lock(results_mutex);
    for (auto const& p : local_results) global_results[p.first] += p.second;
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
    return true;
}

// Stubs to satisfy main.cpp (we read inside executeQuery5 for performance)
bool readTPCHData(const std::string&, std::vector<std::map<std::string,std::string>>&, std::vector<std::map<std::string,std::string>>&, std::vector<std::map<std::string,std::string>>&, std::vector<std::map<std::string,std::string>>&, std::vector<std::map<std::string,std::string>>&, std::vector<std::map<std::string,std::string>>&) {
    return true; 
}

bool executeQuery5(const std::string& r_name, const std::string& start_date, const std::string& end_date, int num_threads, 
                   const std::vector<std::map<std::string,std::string>>&, const std::vector<std::map<std::string,std::string>>&, 
                   const std::vector<std::map<std::string,std::string>>&, const std::vector<std::map<std::string,std::string>>&, 
                   const std::vector<std::map<std::string,std::string>>&, const std::vector<std::map<std::string,std::string>>&, 
                   std::map<std::string, double>& results) {

    std::string base_path = "../../../data/tpch-dbgen/input_data/"; 

    std::unordered_map<int, std::string> n_map;
    std::unordered_map<int, int> c_map, s_map, o_map;

    int rk_target = -1;
    std::string line;
    std::ifstream rf(base_path + "region.tbl");
    while(std::getline(rf, line)) { 
        size_t s=0; 
        int rk=std::stoi(get_token(line, s)); 
        if(get_token(line, s) == r_name) rk_target = rk; 
    }

    std::ifstream nf(base_path + "nation.tbl");
    while(std::getline(nf, line)) {
        size_t s=0; int nk=std::stoi(get_token(line, s)); std::string name=get_token(line, s);
        if(std::stoi(get_token(line, s)) == rk_target) n_map[nk] = name;
    }

    std::ifstream cf(base_path + "customer.tbl");
    while(std::getline(cf, line)) {
        size_t s=0; 
        int ck=std::stoi(get_token(line, s)); 
        get_token(line, s); 
        get_token(line, s);
        int nk=std::stoi(get_token(line, s)); 
        if(n_map.count(nk)) c_map[ck] = nk;
    }

    std::ifstream sf(base_path + "supplier.tbl");
    while(std::getline(sf, line)) {
        size_t s=0; 
        int sk=std::stoi(get_token(line, s)); 
        get_token(line, s); 
        get_token(line, s);
        int nk=std::stoi(get_token(line, s)); 
        if(n_map.count(nk)) s_map[sk] = nk;
    }

    std::ifstream of(base_path + "orders.tbl");
    while(std::getline(of, line)) {
        size_t s=0; 
        int ok=std::stoi(get_token(line, s)); 
        int ck=std::stoi(get_token(line, s));
        get_token(line, s); 
        get_token(line, s);
        std::string d = get_token(line, s);
        if(d >= start_date && d < end_date && c_map.count(ck)) o_map[ok] = c_map[ck];
    }

    std::string lp = base_path + "lineitem.tbl";
    std::ifstream l_file(lp, std::ios::binary | std::ios::ate);
    long total_size = l_file.tellg();
    l_file.close();

    std::vector<std::thread> threads;
    long chunk_size = total_size / num_threads;
    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back(process_lineitem_chunk, lp, i * chunk_size, (i == num_threads - 1) ? total_size : (i + 1) * chunk_size, 
                             std::ref(o_map), std::ref(s_map), std::ref(n_map), std::ref(results));
    }
    for (auto& t : threads) t.join();

    return true;
}

bool outputResults(const std::string& path, const std::map<std::string, double>& results) {
    std::ofstream out(path);
    std::vector<std::pair<std::string, double>> v(results.begin(), results.end());
    std::sort(v.begin(), v.end(), [](const std::pair<std::string, double>& a, const std::pair<std::string, double>& b) { return a.second > b.second; });
    out << std::fixed << std::setprecision(2);
    for (auto const& p : v) out << p.first << "|" << p.second << "\n";
    return true;
}