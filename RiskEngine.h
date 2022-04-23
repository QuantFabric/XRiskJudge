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

struct XRiskLimit
{
    int FlowLimit;
    int TickerCancelLimit;
    int OrderCancelLimit;
};

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
    void HandleRequest(Message::PackMessage& msg);
    void HandleResponse(const Message::PackMessage& msg);
    void HandleCommand(const Message::PackMessage& msg);
    void HandleOrderStatus(const Message::PackMessage& msg);
    void HandleOrderRequest(Message::PackMessage& msg);
    void HandleActionRequest(Message::PackMessage& msg);
public:
    static Utils::RingBuffer<Message::PackMessage> m_RiskResponseQueue;
private:
    HPPackServer* m_HPPackServer;
    HPPackClient* m_HPPackClient;
    Utils::XRiskJudgeConfig m_XRiskJudgeConfig;
    std::thread* m_RequestThread;
    std::thread* m_ResponseThread;
    std::unordered_map<std::string, Message::TOrderStatus> m_PendingOrderMap;// OrderRef, TOrderStatus
    std::unordered_map<std::string, std::list<Message::TOrderStatus>> m_TickerPendingOrderListMap;// Ticker, OrderList
    std::unordered_map<std::string, int> m_OrderCancelledCounterMap;// OrderRef, Cancelled Count
    static std::unordered_map<std::string, Message::TRiskReport> m_TickerCancelledCounterMap;// Ticker, TRiskReport
    static XRiskLimit m_XRiskLimit;
};


#endif // RISKENGINE_H