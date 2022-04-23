#ifndef RISKENGINE_H
#define RISKENGINE_H

#include <list>
#include <string.h>
#include <string>
#include <stdio.h>
#include <thread>
#include <mutex>
#include <unordered_map>
#include "PackMessage.hpp"
#include "Util.hpp"
#include "Logger.h"
#include "YMLConfig.hpp"
#include "HPPackServer.h"
#include "HPPackClient.h"

class RiskEngine
{
    friend class Utils::Singleton<RiskEngine>;
public:
    explicit RiskEngine();
    void LoadConfig(const std::string& yml);
    void Start();
protected:
    void RegisterServer(const char *ip, unsigned int port);
    void RegisterClient(const char *ip, unsigned int port);
    void HandleRequestFunc();
    void HandleResponseFunc();
public:
    static Utils::RingBuffer<Message::PackMessage> m_RiskResponseQueue;
private:
    HPPackServer* m_HPPackServer;
    HPPackClient* m_HPPackClient;
    Utils::XRiskJudgeConfig m_XRiskJudgeConfig;
    std::thread* m_RequestThread;
    std::thread* m_ResponseThread;
};


#endif // RISKENGINE_H