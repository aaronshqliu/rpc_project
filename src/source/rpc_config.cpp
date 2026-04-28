#include "rpc_config.h"
#include <algorithm>
#include <fstream>
#include <glog/logging.h>

bool RpcConfig::LoadConfigFile(const std::string &fileName)
{
    std::ifstream ifs(fileName);
    if (!ifs.is_open()) {
        LOG(ERROR) << "open config file failed: " << fileName;
        return false;
    }

    std::string line;
    while (std::getline(ifs, line)) {
        Trim(line);

        // 跳过空行或注释
        if (line.empty() || line[0] == '#') {
            continue;
        }

        auto pos = line.find('=');
        if (pos == std::string::npos) {
            continue;
        }

        std::string key = line.substr(0, pos);
        std::string value = line.substr(pos + 1);
        Trim(key);
        Trim(value);

        config_map.try_emplace(std::move(key), std::move(value));
    }

    return true;
}

std::string RpcConfig::GetString(const std::string &key) const
{
    auto it = config_map.find(key);
    if (it != config_map.end()) {
        return it->second;
    }
    return "";
}

void RpcConfig::Trim(std::string &str)
{
    // 左 trim
    str.erase(str.begin(),
              std::find_if(str.begin(), str.end(),
                          [](unsigned char ch) { return !std::isspace(ch); }));

    // 右 trim
    str.erase(std::find_if(str.rbegin(), str.rend(),
                          [](unsigned char ch) { return !std::isspace(ch); })
                  .base(),
              str.end());
}
