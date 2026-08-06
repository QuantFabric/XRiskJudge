#ifndef RISKENGINE_H
#define RISKENGINE_H

#include <list>
#include <string.h>
#include <string>
#include <stdio.h>
#include <thread>
#include <cmath>
#include <algorithm>
#include <mutex>
#include <fmt/core.h>
#include <unordered_map>
#include "PackMessage.hpp"
#include "Util.hpp"
#include "FMTLogger.hpp"
#include "YMLConfig.hpp"
#include "RiskJudgeServer.hpp"
#include "HPPackClient.h"
#include "RiskDBManager.hpp"
#include "LockFreeQueue.hpp"


// 组合键结构体（推荐，性能好）
struct RiskLimitKey 
{
    std::string Account;
    std::string Ticker;

    bool operator==(const RiskLimitKey& other) const 
    {
        return Account == other.Account && Ticker == other.Ticker;
    }
};

// 自定义哈希函数
struct RiskLimitKeyHash 
{
    size_t operator()(const RiskLimitKey& k) const 
    {
        return std::hash<std::string>()(k.Account) ^ (std::hash<std::string>()(k.Ticker) << 1);
    }
};


// 组合键结构体（推荐，性能好）
struct VirtualPositionKey 
{
    std::string Account;
    std::string Ticker;
    uint32_t EngineID;
    
    bool operator==(const VirtualPositionKey& other) const 
    {
        return Account == other.Account && Ticker == other.Ticker && EngineID == other.EngineID;
    }
};

// 自定义哈希函数
struct VirtualPositionKeyHash 
{
    size_t operator()(const VirtualPositionKey& k) const 
    {
        return std::hash<std::string>()(k.Account) ^ 
                (std::hash<std::string>()(k.Ticker) << 1) ^ 
                (std::hash<uint64_t>()(k.EngineID) << 2);
    }
};

static bool isPriceInvalid(double price, double tickSize) 
{
    // 基础校验：价格必须为正数
    if (price <= 0.0) 
        return true;

    // 最小变动价位校验（核心）
    // 计算 price 是 tickSize 的多少倍
    double ratio = price / tickSize;
    // 四舍五入到最近的整数
    double nearestInt = std::round(ratio);
    // 判断倍数与整数的差距是否在容差范围内（例如 1e-9）
    if(std::abs(ratio - nearestInt) > 1e-9) 
    {
        return true; // 非法：不是整数倍
    }

    return false; // 合法
}

class RiskEngine
{
    friend class Utils::Singleton<RiskEngine>;
public:
    explicit RiskEngine();
    void LoadConfig(const std::string& yml);
    void SetCommand(const std::string& cmd);
    void Start();
protected:
    void RegisterClient(const char *ip, unsigned int port);
    void WorkThreadFunc();
    void HandleRequest(Message::PackMessage& msg);
    void HandleResponse(const Message::PackMessage& msg);
    void HandleCommand(const Message::PackMessage& msg);
    void HandleOrderStatus(const Message::PackMessage& msg);
    void HandleAccountPosition(const Message::PackMessage& msg);
    void HandleOrderRequest(Message::PackMessage& msg);
    void HandleActionRequest(Message::PackMessage& msg);
    bool Check(Message::PackMessage& msg);
    // 流速控制检查
    bool FlowLimited(Message::PackMessage& msg);
    // 账户锁定检查
    bool AccountLocked(Message::PackMessage& msg);
    // 交易指令检查
    bool TransactionCommand(Message::PackMessage& msg);
    // 自成交检查
    bool SelfMatched(Message::PackMessage& msg);
    // 撤单限制检查
    bool CancelLimited(Message::PackMessage& msg);
    // 检查策略持仓限制
    bool StrategyPositionLimitCheck(Message::PackMessage& msg);
    // 检查账户持仓限制
    bool AccountPositionLimitCheck(Message::PackMessage& msg);
    // 更新Ticker报撤单计数
    void UpdateTickerRiskLimit(const Message::TOrderStatus& status);
    // 更新账户报撤单计数
    void UpdateAccountRiskLimit(const Message::TOrderStatus& status);
    // 更新策略持仓
    void UpdateStrategyPosition(const Message::TOrderStatus& status);
    // 更新账户持仓
    void UpdateAccountPosition(const Message::TOrderStatus& status);

    // 打印风控检查前后报单请求的状态
    void PrintOrderRequest(const Message::TOrderRequest& req, const std::string& op);
    // 打印风控检查前后撤单请求的状态
    void PrintActionRequest(const Message::TActionRequest& req, const std::string& op);
    bool QueryRiskLimit();
    bool QueryPositionLimit();
    bool QueryAccountLocked();

    // 查询RiskLimitTable结果回调函数
    static int sqlite3_callback_RiskLimit(void *data, int argc, char **argv, char **azColName);
    // 查询PositionLimitTable结果回调函数
    static int sqlite3_callback_PositionLimit(void *data, int argc, char **argv, char **azColName);
    // 查询PositionLimitTable结果回调函数
    static int sqlite3_callback_AccountLocked(void *data, int argc, char **argv, char **azColName);
    void HandleRiskCommand(const Message::TCommand& command);
    // 风控参数设置命令解析
    bool ParseUpdateRiskLimitCommand(const std::string& cmd, std::string& sql, std::string& op, Message::TRiskReport& event);
    // 账户锁定命令解析
    bool ParseUpdatePositionLimitCommand(const std::string& cmd, std::string& sql, std::string& op, Message::TRiskReport& event);
    // 账户锁定命令解析
    bool ParseUpdateAccountLockedCommand(const std::string& cmd, std::string& sql, std::string& op, Message::TRiskReport& event);
    void InitAppStatus();
    static void UpdateAppStatus(const std::string& cmd, Message::TAppStatus& AppStatus);
public:
    static Utils::LockFreeQueue<Message::PackMessage> m_RiskResponseQueue;
private:
    RiskJudgeServer* m_RiskJudgeServer;
    HPPackClient* m_HPPackClient;
    Utils::XRiskJudgeConfig m_XRiskJudgeConfig;
    std::thread* m_WorkThread;
    std::unordered_map<std::string, Message::TOrderStatus> m_PendingOrderMap;// OrderRef, TOrderStatus
    std::unordered_map<std::string, std::list<Message::TOrderStatus>> m_TickerPendingOrderListMap;// Ticker, OrderList
    std::unordered_map<std::string, int> m_OrderCancelledCounterMap;// OrderRef, Cancelled Count
    std::unordered_map<std::string, int> m_AccountFlowLimitedMap;// Account, flow counter
    RiskDBManager* m_RiskDBManager;
    std::string m_Command;
    // 风控限制：Account, Ticker -> TRiskReport
    static std::unordered_map<RiskLimitKey, Message::TRiskReport, RiskLimitKeyHash> m_RiskLimitMap;
    // 策略持仓上限：Account, Ticker, EngineID -> TRiskReport
    static std::unordered_map<VirtualPositionKey, Message::TRiskReport, VirtualPositionKeyHash> m_StrategyPositionLimitMap;
    // 风控限制：Account, Ticker -> TRiskReport
    static std::unordered_map<RiskLimitKey, Message::TRiskReport, RiskLimitKeyHash> m_AccountLockedMap;
    std::unordered_map<std::string, Utils::TickerProperty> m_TickerPropertyMap;
};


#endif // RISKENGINE_H