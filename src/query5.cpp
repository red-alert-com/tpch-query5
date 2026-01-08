#include "query5.hpp"
#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <unordered_map>
#include <map>
#include <algorithm>
#include <iomanip>

// Helper to trim potential whitespace (crucial for TPC-H fixed-width strings)
std::string trim(const std::string& s) {
    size_t first = s.find_first_not_of(' ');
    if (std::string::npos == first) return s;
    size_t last = s.find_last_not_of(' ');
    return s.substr(first, (last - first + 1));
}

// Simple and fast parser for pipe-separated files
std::vector<std::string> split_line(const std::string& s) {
    std::vector<std::string> tokens;
    size_t start = 0, end = 0;
    while ((end = s.find('|', start)) != std::string::npos) {
        tokens.push_back(trim(s.substr(start, end - start)));
        start = end + 1;
    }
    return tokens;
}

// Generic table reader
bool readTable(const std::string& path, const std::vector<std::string>& cols, std::vector<std::map<std::string, std::string>>& data) {
    std::ifstream file(path);
    if (!file.is_open()) {
        std::cerr << "Could not open: " << path << std::endl;
        return false;
    }
    std::string line;
    while (std::getline(file, line)) {
        if (line.empty()) continue;
        auto tokens = split_line(line);
        std::map<std::string, std::string> row;
        for (size_t i = 0; i < cols.size() && i < tokens.size(); ++i) {
            row[cols[i]] = tokens[i];
        }
        data.push_back(std::move(row));
    }
    return true;
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
    return !r_name.empty() && !start_date.empty() && !end_date.empty() && !table_path.empty() && !result_path.empty();
}

bool readTPCHData(const std::string& table_path, 
                  std::vector<std::map<std::string, std::string>>& customer_data, 
                  std::vector<std::map<std::string, std::string>>& orders_data, 
                  std::vector<std::map<std::string, std::string>>& lineitem_data, 
                  std::vector<std::map<std::string, std::string>>& supplier_data, 
                  std::vector<std::map<std::string, std::string>>& nation_data, 
                  std::vector<std::map<std::string, std::string>>& region_data) {
    
    // Mapped exactly to the columns provided in your data samples
    if (!readTable(table_path + "/customer.tbl", {"c_custkey", "c_name", "c_address", "c_nationkey"}, customer_data)) return false;
    if (!readTable(table_path + "/orders.tbl", {"o_orderkey", "o_custkey", "o_orderstatus", "o_totalprice", "o_orderdate"}, orders_data)) return false;
    if (!readTable(table_path + "/lineitem.tbl", {"l_orderkey", "l_partkey", "l_suppkey", "l_linenumber", "l_quantity", "l_extendedprice", "l_discount"}, lineitem_data)) return false;
    if (!readTable(table_path + "/supplier.tbl", {"s_suppkey", "s_name", "s_address", "s_nationkey"}, supplier_data)) return false;
    if (!readTable(table_path + "/nation.tbl", {"n_nationkey", "n_name", "n_regionkey"}, nation_data)) return false;
    if (!readTable(table_path + "/region.tbl", {"r_regionkey", "r_name"}, region_data)) return false;
    
    return true;
}

bool executeQuery5(const std::string& r_name, const std::string& start_date, const std::string& end_date, int num_threads, 
                   const std::vector<std::map<std::string, std::string>>& customer_data, 
                   const std::vector<std::map<std::string, std::string>>& orders_data, 
                   const std::vector<std::map<std::string, std::string>>& lineitem_data, 
                   const std::vector<std::map<std::string, std::string>>& supplier_data, 
                   const std::vector<std::map<std::string, std::string>>& nation_data, 
                   const std::vector<std::map<std::string, std::string>>& region_data, 
                   std::map<std::string, double>& results) {

    // Step 1: Find target region key
    std::string target_r_key;
    for (const auto& r : region_data) {
        if (r.at("r_name") == r_name) { target_r_key = r.at("r_regionkey"); break; }
    }
    if (target_r_key.empty()) return true;

    // Step 2: Nations in region
    std::unordered_map<std::string, std::string> n_map; 
    for (const auto& n : nation_data) {
        if (n.at("n_regionkey") == target_r_key) n_map[n.at("n_nationkey")] = n.at("n_name");
    }

    // Step 3: Customers in those nations
    std::unordered_map<std::string, std::string> c_map; 
    for (const auto& c : customer_data) {
        if (n_map.count(c.at("c_nationkey"))) c_map[c.at("c_custkey")] = c.at("c_nationkey");
    }

    // Step 4: Suppliers in those nations
    std::unordered_map<std::string, std::string> s_map;
    for (const auto& s : supplier_data) {
        if (n_map.count(s.at("s_nationkey"))) s_map[s.at("s_suppkey")] = s.at("s_nationkey");
    }

    // Step 5: Orders within date range and customer region
    std::unordered_map<std::string, std::string> o_map;
    for (const auto& o : orders_data) {
        const std::string& d = o.at("o_orderdate");
        if (d >= start_date && d < end_date) {
            auto it = c_map.find(o.at("o_custkey"));
            if (it != c_map.end()) o_map[o.at("o_orderkey")] = it->second;
        }
    }

    // Step 6: Lineitem join (The Big One)
    for (const auto& l : lineitem_data) {
        auto o_it = o_map.find(l.at("l_orderkey"));
        if (o_it != o_map.end()) {
            const std::string& n_key = o_it->second;
            auto s_it = s_map.find(l.at("l_suppkey"));
            if (s_it != s_map.end() && s_it->second == n_key) {
                double rev = std::stod(l.at("l_extendedprice")) * (1.0 - std::stod(l.at("l_discount")));
                results[n_map.at(n_key)] += rev;
            }
        }
    }
    return true;
}

bool outputResults(const std::string& result_path, const std::map<std::string, double>& results) {
    std::ofstream out(result_path);
    if (!out.is_open()) return false;

    std::vector<std::pair<std::string, double>> sorted_res(results.begin(), results.end());
    std::sort(sorted_res.begin(), sorted_res.end(), 
        [](const std::pair<std::string, double>& a, const std::pair<std::string, double>& b) {
            return a.second > b.second;
        }
    );

    out << std::fixed << std::setprecision(2);
    for (const auto& p : sorted_res) {
        out << p.first << "|" << p.second << "\n";
    }
    return true;
}