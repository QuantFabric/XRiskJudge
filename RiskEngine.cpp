#include "RiskEngine.h"

extern Utils::Logger* gLogger;

Utils::RingBuffer<Message::PackMessage> RiskEngine::m_RiskResponseQueue(1 << 10);
XRiskLimit RiskEngine::m_XRiskLimit;
std::unordered_map<std::string, Message::TRiskReport> RiskEngine::m_TickerCancelledCounterMap;
std::unordered_map<std::string, Message::TRiskReport> RiskEngine::m_AccountLockedStatusMap;

RiskEngine::RiskEngine()
{
    m_HPPackServer = NULL;
    m_HPPackClient = NULL;
    m_RequestThread = NULL;
    m_ResponseThread = NULL;

    m_XRiskLimit.FlowLimit  = 10;
    m_XRiskLimit.OrderCancelLimit = 5;
    m_XRiskLimit.TickerCancelLimit = 400;
}

void RiskEngine::LoadConfig(const std::string& yml)
{
    std::string errorString;
    bool ret = Utils::LoadXRiskJudgeConfig(yml.c_str(), m_XRiskJudgeConfig, errorString);
    if(!ret)
    {
        Utils::gLogger->Log->warn("RiskEngine::LoadXRiskJudgeConfig failed, {}", errorString.c_str());
    }
    else
    {
        Utils::gLogger->Log->info("RiskEngine::LoadConfig successed, LoadXRiskJudgeConfig {}", yml.c_str());
    }
}

void RiskEngine::Start()
{
    RegisterServer(m_XRiskJudgeConfig.ServerIP.c_str(), m_XRiskJudgeConfig.ServerPort);
    RegisterClient(m_XRiskJudgeConfig.XWatcherIP.c_str(), m_XRiskJudgeConfig.XWatcherPort);
    
    m_RequestThread = new std::thread(&RiskEngine::HandleRequestFunc, this);
    m_ResponseThread = new std::thread(&RiskEngine::HandleResponseFunc, this);

    m_RequestThread->join();
    m_ResponseThread->join();
}

void RiskEngine::RegisterServer(const char *ip, unsigned int port)
{
    m_HPPackServer = new HPPackServer(ip, port);
    m_HPPackServer->Start();
}

void RiskEngine::RegisterClient(const char *ip, unsigned int port)
{
    m_HPPackClient = new HPPackClient(ip, port);
    m_HPPackClient->Start();
    sleep(1);
    Message::TLoginRequest login;
    login.ClientType = Message::EClientType::EXRISKJUDGE;
    strncpy(login.Account, APP_NAME, sizeof(login.Account));
    m_HPPackClient->Login(login);
}

void RiskEngine::HandleRequestFunc()
{
    Utils::gLogger->Log->info("RiskEngine::HandleRequestFunc Risk Service {} Running", m_XRiskJudgeConfig.RiskID);
    while (true)
    {
        Message::PackMessage message;
        memset(&message, 0, sizeof(message));
        bool ret = m_HPPackServer->m_RequestMessageQueue.pop(message);
        if(ret)
        {
            HandleRequest(message);
        }
        memset(&message, 0, sizeof(message));
        ret = m_HPPackClient->m_PackMessageQueue.pop(message);
        if(ret)
        {
            if(message.MessageType == Message::EMessageType::ECommand)
            {
                HandleCommand(message);
            }
        }
    }
}

void RiskEngine::HandleResponseFunc()
{
    Utils::gLogger->Log->info("RiskEngine::HandleResponseFunc Response Message Handling");
    while (true)
    {
        Message::PackMessage message;
        memset(&message, 0, sizeof(message));
        bool ret = m_RiskResponseQueue.pop(message);
        if(ret)
        {
            HandleResponse(message);
        }
        static long CurrentTimeStamp = 0;
        long Sec = Utils::getTimeStampMs(Utils::getCurrentTimeMs() + 11) / 1000;
        if(CurrentTimeStamp < Sec)
        {
            CurrentTimeStamp = Sec;
        }
        if(CurrentTimeStamp % 5 == 0)
        {
            m_HPPackClient->ReConnect();
            CurrentTimeStamp += 1;
        }
    }
}

void RiskEngine::HandleRequest(Message::PackMessage& msg)
{
    char buffer[256] = {0};
    sprintf(buffer, "Message Type:0X%X", msg.MessageType);
    Utils::gLogger->Log->debug("RiskEngine::HandleRequestMessage receive message {}", buffer);
    switch (msg.MessageType)
    {
    case Message::EMessageType::EOrderRequest:
        HandleOrderRequest(msg);
        break;
    case Message::EMessageType::EActionRequest:
        HandleActionRequest(msg);
        break;
    case Message::EMessageType::EOrderStatus:
        HandleOrderStatus(msg);
        break;
    case Message::EMessageType::ELoginRequest:
        break;
    case Message::EMessageType::EEventLog:
        m_HPPackClient->SendData((const unsigned char*)&msg, sizeof(msg));
        break;
    default:
        char errorString[256] = {0};
        sprintf(errorString, "RiskEngine::HandleRequestMessage Unkown Message Type:0X%X", msg.MessageType);
        Utils::gLogger->Log->warn(errorString);
        break;
    }
}

void RiskEngine::HandleResponse(const Message::PackMessage& msg)
{
    switch (msg.MessageType)
    {
    case Message::EMessageType::EOrderRequest:
    {
        for (auto it = m_HPPackServer->m_sConnections.begin(); it != m_HPPackServer->m_sConnections.end(); it++)
        {
            if(Utils::equalWith(msg.OrderRequest.Account, it->second.Account))
            {
                m_HPPackServer->SendData(it->second.dwConnID, (const unsigned char*)&msg, sizeof(msg));
                 break;
            }
        }
        break;
    }
    case Message::EMessageType::EActionRequest:
    {
        for (auto it = m_HPPackServer->m_sConnections.begin(); it != m_HPPackServer->m_sConnections.end(); it++)
        {
            if(Utils::equalWith(msg.ActionRequest.Account, it->second.Account))
            {
                m_HPPackServer->SendData(it->second.dwConnID, (const unsigned char*)&msg, sizeof(msg));
                break;
            }
        }
        break;
    }
    case Message::EMessageType::ERiskReport:
        m_HPPackClient->SendData((const unsigned char*)&msg, sizeof(msg));
        break;
    default:
        char errorString[256] = {0};
        sprintf(errorString, "RiskEngine::HandleResponseMessage Unkown Message Type:0X%X", msg.MessageType);
        Utils::gLogger->Log->warn(errorString);
        break;
    }
}

void RiskEngine::HandleCommand(const Message::PackMessage& msg)
{
    Utils::gLogger->Log->info("RiskEngine::HandleCommand Command:{}", msg.Command.Command);
    // TODO: Handle Command
}

void RiskEngine::HandleOrderStatus(const Message::PackMessage& msg)
{
    const Message::TOrderStatus& OrderStatus = msg.OrderStatus;
    // Add Pending Order
    {
        if(Message::EOrderStatus::EPARTTRADED == OrderStatus.OrderStatus ||
                Message::EOrderStatus::EEXCHANGE_ACK == OrderStatus.OrderStatus ||
                Message::EOrderStatus::EORDER_SENDED == OrderStatus.OrderStatus)
        {
            std::string OrderRef = OrderStatus.OrderRef;
            auto it = m_PendingOrderMap.find(OrderRef);
            if(m_PendingOrderMap.end() == it)
            {
                m_PendingOrderMap[OrderRef] = OrderStatus;
                std::list<Message::TOrderStatus>& orderList = m_TickerPendingOrderListMap[OrderStatus.Ticker];
                orderList.push_back(OrderStatus);
                char errorString[256] = {0};
                sprintf(errorString, "RiskEngine::HandleOrderStatus, Add Pending Order, Product:%s Account:%s Ticker:%s OrderRef:%s Pending Number:%d",
                        OrderStatus.Product, OrderStatus.Account, OrderStatus.Ticker, OrderStatus.OrderRef, orderList.size());
                Utils::gLogger->Log->info(errorString);
            }
            return;
        }
    }
    // Remove Pending Order when Order end
    {
        if(Message::EOrderStatus::EALLTRADED == OrderStatus.OrderStatus ||
                Message::EOrderStatus::EPARTTRADED_CANCELLED == OrderStatus.OrderStatus ||
                Message::EOrderStatus::ECANCELLED == OrderStatus.OrderStatus ||
                Message::EOrderStatus::EBROKER_ERROR == OrderStatus.OrderStatus ||
                Message::EOrderStatus::EEXCHANGE_ERROR)
        {
            // Remove Pending Order
            {
                std::list<Message::TOrderStatus>& orderList = m_TickerPendingOrderListMap[OrderStatus.Ticker];
                for (auto it = orderList.begin(); orderList.end() != it; )
                {
                    if(Utils::equalWith(it->OrderRef, OrderStatus.OrderRef))
                    {
                        orderList.erase(it++);
                    }
                    else
                    {
                        it++;
                    }
                }
                for(auto it = orderList.begin(); orderList.end() != it; it++)
                {
                    Utils::gLogger->Log->debug("RiskEngine::HandleOrderStatus Pengding Order Ticker:{} OrderRef:{}", it->Ticker, it->OrderRef);
                }
                Utils::gLogger->Log->info("RiskEngine::HandleOrderStatus, Remove Pending Order, Product:{} Account:{} Ticker:{} OrderRef:{} Pengding Number:{}",
                                          OrderStatus.Product, OrderStatus.Account, OrderStatus.Ticker, OrderStatus.OrderRef, orderList.size());
            }
            // Remove Pending Order
            {
                std::string OrderRef = OrderStatus.OrderRef;
                auto it = m_PendingOrderMap.find(OrderRef);
                if(m_PendingOrderMap.end() != it)
                {
                    m_PendingOrderMap.erase(it->first);
                }
            }
            // Remove Pending Order
            {
                std::string OrderRef = OrderStatus.OrderRef;
                auto it = m_OrderCancelledCounterMap.find(OrderRef);
                if(m_OrderCancelledCounterMap.end() != it)
                {
                    m_OrderCancelledCounterMap.erase(it->first);
                }
            }
        }
    }
    // Update Cancelled Order Counter
    {
        bool cancelled = Message::EOrderStatus::EPARTTRADED_CANCELLED == OrderStatus.OrderStatus ||
                         Message::EOrderStatus::ECANCELLED == OrderStatus.OrderStatus ||
                         Message::EOrderStatus::EEXCHANGE_ERROR == OrderStatus.OrderStatus;
        if(OrderStatus.OrderType == Message::EOrderType::ELIMIT && cancelled)
        {
            std::string Product = OrderStatus.Product;
            std::string Ticker = OrderStatus.Ticker;
            std::string Key = Product + ":" + Ticker;
            std::mutex mtx;
            mtx.lock();
            int CancelledCount = 0;
            auto it = m_TickerCancelledCounterMap.find(Key);
            if(m_TickerCancelledCounterMap.end() != it)
            {
                Message::TRiskReport& report = it->second;
                report.CancelledCount += 1;
                CancelledCount = report.CancelledCount;
                strncpy(report.UpdateTime, Utils::getCurrentTimeUs(), sizeof(report.UpdateTime));
                strncpy(report.RiskID, m_XRiskJudgeConfig.RiskID.c_str(), sizeof(report.RiskID));
                strncpy(report.Product, OrderStatus.Product, sizeof(report.Product));
                strncpy(report.Ticker, OrderStatus.Ticker, sizeof(report.Ticker));
                // TODO: Update SQLite CancelledCountTable
            }
            else
            {
                Message::TRiskReport& report = m_TickerCancelledCounterMap[Key];
                report.UpperLimit = m_XRiskLimit.TickerCancelLimit;
                report.CancelledCount += 1;
                CancelledCount = report.CancelledCount;
                report.ReportType = Message::ERiskReportType::ERISK_TICKER_CANCELLED;
                strncpy(report.RiskID, m_XRiskJudgeConfig.RiskID.c_str(), sizeof(report.RiskID));
                strncpy(report.Product, OrderStatus.Product, sizeof(report.Product));
                strncpy(report.Ticker, OrderStatus.Ticker, sizeof(report.Ticker));
                strncpy(report.UpdateTime, Utils::getCurrentTimeUs(), sizeof(report.UpdateTime));
                // TODO: Update SQLite CancelledCountTable
            }
            mtx.unlock();
            // Update Risk to Monitor
            {
                Message::PackMessage message;
                memset(&message, 0, sizeof(message));
                message.MessageType = Message::EMessageType::ERiskReport;
                memcpy(&message.RiskReport, &m_TickerCancelledCounterMap[Key], sizeof(message.RiskReport));
                m_RiskResponseQueue.push(message);
            }
            Utils::gLogger->Log->info("RiskEngine::HandleOrderStatus, Update Cancelled Order Counter, Product:{}"
                                      "Account:{} Ticker:{} OrderRef:{} OrderStatus:{} OrderType:{} CancelledCount:{}",
                                      OrderStatus.Product, OrderStatus.Account, OrderStatus.Ticker, OrderStatus.OrderRef,
                                      OrderStatus.OrderStatus, OrderStatus.OrderType, CancelledCount);
        }
    }
}

void RiskEngine::HandleOrderRequest(Message::PackMessage& msg)
{
    // 风控初始化检查
    if(Message::EMessageType::EOrderRequest == msg.MessageType && Message::ERiskStatusType::ECHECK_INIT == msg.OrderRequest.RiskStatus)
    {
        strncpy(msg.OrderRequest.RiskID, m_XRiskJudgeConfig.RiskID.c_str(), sizeof(msg.OrderRequest.RiskID));
        msg.OrderRequest.ErrorID = -1;
        strncpy(msg.OrderRequest.ErrorMsg, "Risk Check Init", sizeof(msg.OrderRequest.ErrorMsg));
        m_RiskResponseQueue.push(msg);
        return;
    }
    Check(msg);
}

void RiskEngine::HandleActionRequest(Message::PackMessage& msg)
{
    Check(msg);
}

bool RiskEngine::Check(Message::PackMessage& msg)
{
    bool ret = true;
    int start = Utils::getTimeUs();
    if((Message::EMessageType::EOrderRequest == msg.MessageType && Message::ERiskStatusType::EPREPARE_CHECKED == msg.OrderRequest.RiskStatus) ||
            (Message::EMessageType::EActionRequest == msg.MessageType && Message::ERiskStatusType::EPREPARE_CHECKED == msg.ActionRequest.RiskStatus))
    {
        // Flow Limited Check
        if(!FlowLimited(msg))
        {
            ret = false;
        }
        // Account Locked Check
        if(ret && !AccountLocked(msg))
        {
            ret = false;
        }
        // Self Matched Check
        if(ret && !SelfMatched(msg))
        {
            ret = false;
        }
        // Cancel Limit Check
        if(ret && !CancelLimited(msg))
        {
            ret = false;
        }
        if(ret)
        {
            if(Message::EMessageType::EOrderRequest == msg.MessageType)
            {
                msg.OrderRequest.RiskStatus = Message::ERiskStatusType::ECHECKED_PASS;
                strncpy(msg.OrderRequest.RiskID, m_XRiskJudgeConfig.RiskID.c_str(), sizeof(msg.OrderRequest.RiskID));
            }
            else if(Message::EMessageType::EActionRequest == msg.MessageType)
            {
                msg.ActionRequest.RiskStatus = Message::ERiskStatusType::ECHECKED_PASS;
                strncpy(msg.ActionRequest.RiskID, m_XRiskJudgeConfig.RiskID.c_str(), sizeof(msg.ActionRequest.RiskID));
            }
        }
        else
        {

            if(Message::EMessageType::EOrderRequest == msg.MessageType)
            {
                msg.OrderRequest.RiskStatus = Message::ERiskStatusType::ECHECKED_NOPASS;
                strncpy(msg.OrderRequest.RiskID, m_XRiskJudgeConfig.RiskID.c_str(), sizeof(msg.OrderRequest.RiskID));
            }
            else if(Message::EMessageType::EActionRequest == msg.MessageType)
            {
                msg.ActionRequest.RiskStatus = Message::ERiskStatusType::ECHECKED_NOPASS;
                strncpy(msg.ActionRequest.RiskID, m_XRiskJudgeConfig.RiskID.c_str(), sizeof(msg.ActionRequest.RiskID));
            }
            Message::PackMessage message;
            memset(&message, 0, sizeof(message));
            message.MessageType = Message::EMessageType::ERiskReport;
            Message::TRiskReport RiskEvent;
            memset(&RiskEvent, 0, sizeof(RiskEvent));
            RiskEvent.ReportType = Message::ERiskReportType::ERISK_EVENTLOG;
            strncpy(RiskEvent.RiskID, m_XRiskJudgeConfig.RiskID.c_str(), sizeof(RiskEvent.RiskID));
            if(Message::EMessageType::EOrderRequest == msg.MessageType)
            {
                strncpy(RiskEvent.Account, msg.OrderRequest.Account, sizeof(RiskEvent.Account));
                strncpy(RiskEvent.Ticker, msg.OrderRequest.Ticker, sizeof(RiskEvent.Ticker));
                strncpy(RiskEvent.Event, msg.OrderRequest.ErrorMsg, sizeof(RiskEvent.Event));
                sprintf(RiskEvent.Trader, "0X%X", msg.OrderRequest.EngineID);
                strncpy(RiskEvent.UpdateTime, Utils::getCurrentTimeUs(), sizeof(RiskEvent.UpdateTime));
            }
            else if(Message::EMessageType::EActionRequest == msg.MessageType)
            {
                strncpy(RiskEvent.Account, msg.ActionRequest.Account, sizeof(RiskEvent.Account));
                strncpy(RiskEvent.Event, msg.ActionRequest.ErrorMsg, sizeof(RiskEvent.Event));
                sprintf(RiskEvent.Trader, "0X%X", msg.ActionRequest.EngineID);
                strncpy(RiskEvent.UpdateTime, Utils::getCurrentTimeUs(), sizeof(RiskEvent.UpdateTime));
            }
            memcpy(&message.RiskReport, &RiskEvent, sizeof(message.RiskReport));
            // 风控拦截事件报告
            m_RiskResponseQueue.push(message);
        }
        // 风控检查结果
        m_RiskResponseQueue.push(msg);
    }
    int end = Utils::getTimeUs();
    Utils::gLogger->Log->info("RiskEngine::Check Risk Check Latency:{}", end - start);
    return ret;
}

bool RiskEngine::FlowLimited(Message::PackMessage& msg)
{
    bool ret = true;
    static long startTimeStampMs = Utils::getTimeMs();
    long endTimeStampMs = Utils::getTimeMs();
    long diff = endTimeStampMs - startTimeStampMs;
    std::string Account;
    if(Message::EMessageType::EOrderRequest == msg.MessageType)
    {
        Account = msg.OrderRequest.Account;
    }
    else if(Message::EMessageType::EActionRequest == msg.MessageType)
    {
        Account = msg.ActionRequest.Account;
    }
    // last 1000 ms
    if(diff <= 1000)
    {
        m_AccountFlowLimitedMap[Account] += 1;
        if(m_AccountFlowLimitedMap[Account] > m_XRiskLimit.FlowLimit)
        {
            ret = false;
        }
    }
    else
    {
        startTimeStampMs = Utils::getTimeMs();
        // reset counter when Timeout
        for (auto it = m_AccountFlowLimitedMap.begin(); m_AccountFlowLimitedMap.end() != it; it++)
        {
            it->second = 0;
        }
        m_AccountFlowLimitedMap[Account] = 1;
    }
    if(!ret)
    {
        char errorString[256] = {0};
        if(Message::EMessageType::EOrderRequest == msg.MessageType)
        {
            msg.OrderRequest.ErrorID =  Message::ERiskRejectedType::EFLOW_LIMITED;
            msg.OrderRequest.RiskStatus = Message::ERiskStatusType::ECHECKED_NOPASS;
            sprintf(errorString, "FlowLimited, ErrorID:0X%X flow:%d limit:%d Product:%s Account:%s EngineID:0X%X",
                    msg.OrderRequest.ErrorID, m_AccountFlowLimitedMap[Account], m_XRiskLimit.FlowLimit,
                    msg.OrderRequest.Product, msg.OrderRequest.Account, msg.OrderRequest.EngineID);
            memcpy(msg.OrderRequest.ErrorMsg, errorString, sizeof(msg.OrderRequest.ErrorMsg));
        }
        else if(Message::EMessageType::EActionRequest == msg.MessageType)
        {
            msg.ActionRequest.ErrorID =  Message::ERiskRejectedType::EFLOW_LIMITED;
            msg.ActionRequest.RiskStatus = Message::ERiskStatusType::ECHECKED_NOPASS;
            sprintf(errorString, "FlowLimited, ErrorID:0X%X flow:%d limit:%d Account:%s EngineID:0X%X",
                    msg.ActionRequest.ErrorID, m_AccountFlowLimitedMap[Account], m_XRiskLimit.FlowLimit,
                    msg.ActionRequest.Account, msg.ActionRequest.EngineID);
            memcpy(msg.ActionRequest.ErrorMsg, errorString, sizeof(msg.ActionRequest.ErrorMsg));
        }
        Utils::gLogger->Log->warn("RiskEngine::FlowLimited Check failed, {}", errorString);
    }
    return ret;
}

bool RiskEngine::AccountLocked(Message::PackMessage& msg)
{
    bool ret = true;
    std::string Account;
    std::string LockedSide;
    if(Message::EMessageType::EOrderRequest == msg.MessageType)
    {
        Account = msg.OrderRequest.Account;
    }
    else if(Message::EMessageType::EActionRequest == msg.MessageType)
    {
        return true;
    }
    auto it = m_AccountLockedStatusMap.find(Account);
    if(m_AccountLockedStatusMap.end() != it)
    {
        if(Message::ERiskLockedSide::EUNLOCK != it->second.LockedSide)
        {
            if(Message::ERiskLockedSide::ELOCK_ACCOUNT == it->second.LockedSide)
            {
                ret = false;
                LockedSide = "LockedSide:All";
            }
            else if(Message::ERiskLockedSide::ELOCK_BUY == it->second.LockedSide && Message::EOrderDirection::EBUY == msg.OrderRequest.Direction)
            {
                std::string Ticker = it->second.Ticker;
                if(Ticker.empty())
                {
                    ret = false;
                    LockedSide = "LockedSide:Buy Ticker:All";
                }
                else if(Ticker == msg.OrderRequest.Ticker)
                {
                    ret = false;
                    LockedSide = "LockedSide:Buy Ticker:" + Ticker;
                }
            }
            else if(Message::ERiskLockedSide::ELOCK_SELL == it->second.LockedSide && Message::EOrderDirection::ESELL == msg.OrderRequest.Direction)
            {
                std::string Ticker = it->second.Ticker;
                if(Ticker.empty())
                {
                    ret = false;
                    LockedSide = "LockedSide:Sell Ticker:All";
                }
                else if(Ticker == msg.OrderRequest.Ticker)
                {
                    ret = false;
                    LockedSide = "LockedSide:Sell Ticker:" + Ticker;
                }
            }
        }
    }
    if(!ret)
    {
        char errorString[256] = {0};
        if(Message::EMessageType::EOrderRequest == msg.MessageType)
        {
            msg.OrderRequest.ErrorID =  Message::ERiskRejectedType::EACCOUNT_LOCKED;
            msg.OrderRequest.RiskStatus = Message::ERiskStatusType::ECHECKED_NOPASS;
            sprintf(errorString, "AccountLocked %s, ErrorID:0X%X Product:%s Account:%s EngineID:0X%X",
                    LockedSide.c_str(), msg.OrderRequest.ErrorID, msg.OrderRequest.Product, msg.OrderRequest.Account,
                    msg.OrderRequest.EngineID);
            memcpy(msg.OrderRequest.ErrorMsg, errorString, sizeof(msg.OrderRequest.ErrorMsg));
            Utils::gLogger->Log->warn("RiskEngine::AccountLocked Check failed, {}", errorString);
        }
    }
    return ret;
}

bool RiskEngine::SelfMatched(Message::PackMessage& msg)
{
    bool ret = true;
    if(Message::EMessageType::EOrderRequest == msg.MessageType)
    {
        std::string Ticker = msg.OrderRequest.Ticker;
        std::list<Message::TOrderStatus>& orderList = m_TickerPendingOrderListMap[Ticker];
        Utils::gLogger->Log->info("RiskEngine::SelfMatched Check, Ticker:{}, Pending Order Number:{}",
                                  msg.OrderRequest.Ticker, orderList.size());
        for (auto it = orderList.begin(); orderList.end() != it; it++)
        {
            switch (msg.OrderRequest.Direction)
            {
            // Buy
            case Message::EOrderDirection::EBUY:
            {
                bool sideMatched = Message::EOrderSide::ECLOSE_YD_LONG == it->OrderSide ||
                                   Message::EOrderSide::ECLOSE_TD_LONG == it->OrderSide ||
                                   Message::EOrderSide::EOPEN_SHORT == it->OrderSide;
                if(sideMatched && msg.OrderRequest.Price >= it->SendPrice)
                {
                    ret = false;
                }
            }
            break;
            // Sell
            case Message::EOrderDirection::ESELL:
            {
                bool sideMatched = Message::EOrderSide::EOPEN_LONG == it->OrderSide ||
                                   Message::EOrderSide::ECLOSE_YD_SHORT == it->OrderSide ||
                                   Message::EOrderSide::ECLOSE_TD_SHORT == it->OrderSide;
                if(sideMatched && msg.OrderRequest.Price <= it->SendPrice)
                {
                    ret = false;
                }
            }
            break;
            }
            // Self Matched
            if(!ret)
            {
                msg.OrderRequest.ErrorID = Message::ERiskRejectedType::ESELF_MATCHED;
                msg.OrderRequest.RiskStatus = Message::ERiskStatusType::ECHECKED_NOPASS;
                char errorString[256] = {0};
                sprintf(errorString, "SelfMatched, ErrorID:0X%X Product:%s Account:%s OrderToken:%d, Matched OrderRef:%s",
                        msg.OrderRequest.ErrorID, msg.OrderRequest.Product, msg.OrderRequest.Account, msg.OrderRequest.OrderToken, it->OrderRef);
                memcpy(msg.OrderRequest.ErrorMsg, errorString, sizeof(msg.OrderRequest.ErrorMsg));
                Utils::gLogger->Log->warn("RiskEngine::SelfMatched Check failed, {}", errorString);
                break;
            }
        }
    }
    return ret;
}

bool RiskEngine::CancelLimited(Message::PackMessage& msg)
{
    bool ret = true;
    if(Message::EMessageType::EActionRequest == msg.MessageType)
    {
        std::string OrderRef = msg.ActionRequest.OrderRef;
        auto it = m_PendingOrderMap.find(OrderRef);
        if(m_PendingOrderMap.end() != it)
        {
            // Ticker Cancelled Limit
            if(ret)
            {
                std::string Product = it->second.Product;
                std::string Ticker = it->second.Ticker;
                std::string Key = Product + ":" + Ticker;
                int CancelRequestCount = 0;
                auto it1 = m_TickerCancelledCounterMap.find(Key);
                if(it1 != m_TickerCancelledCounterMap.end())
                {
                    CancelRequestCount = it1->second.CancelledCount;
                }
                CancelRequestCount += 1;
                if(CancelRequestCount > m_XRiskLimit.TickerCancelLimit)
                {
                    msg.ActionRequest.ErrorID = Message::ERiskRejectedType::ETICKER_ACTION_LIMITED;
                    msg.ActionRequest.RiskStatus = Message::ERiskStatusType::ECHECKED_NOPASS;
                    ret = false;
                    char errorString[256] = {0};
                    sprintf(errorString, "CancelLimited, ErrorID:0X%X Product:%s Account:%s OrderRef:%s, CancelRequestCount:%d Ticker Cancelled Limit:%d",
                            msg.ActionRequest.ErrorID, it->second.Product, it->second.Account, it->second.OrderRef,
                            CancelRequestCount, m_XRiskLimit.TickerCancelLimit);
                    memcpy(msg.ActionRequest.ErrorMsg, errorString, sizeof(msg.ActionRequest.ErrorMsg));
                    Utils::gLogger->Log->warn("RiskEngine::CancelLimited Check failed, {}", errorString);
                }
            }
            // Order Cancelled Limit
            if(ret)
            {
                int& CancelRequestCount = m_OrderCancelledCounterMap[OrderRef];
                CancelRequestCount += 1;
                if(CancelRequestCount > m_XRiskLimit.OrderCancelLimit)
                {
                    msg.ActionRequest.ErrorID = Message::ERiskRejectedType::EORDER_ACTION_LIMITED;
                    msg.ActionRequest.RiskStatus = Message::ERiskStatusType::ECHECKED_NOPASS;
                    ret = false;
                    char errorString[256] = {0};
                    sprintf(errorString, "CancelLimited, ErrorID:0X%X Product:%s Account:%s OrderRef:%s, CancelRequestCount:%d Order Cancelled Limit:%d",
                            msg.ActionRequest.ErrorID, it->second.Product, it->second.Account, it->second.OrderRef,
                            CancelRequestCount, m_XRiskLimit.OrderCancelLimit);
                    memcpy(msg.ActionRequest.ErrorMsg, errorString, sizeof(msg.ActionRequest.ErrorMsg));
                    Utils::gLogger->Log->warn("RiskEngine::CancelLimited Check failed, {}", errorString);
                }
            }
        }
    }
    return ret;
}

void RiskEngine::PrintOrderRequest(const Message::TOrderRequest& req, const std::string& op)
{
    Utils::gLogger->Log->debug("RiskEngine::PrintOrderRequest, {} Product:{} Account:{} Ticker:{} OrderType:{}\n"
                              "\t\t\t\t\t\tDirection:{} Offset:{} Price:{} Volume:{} EngineID:{} RiskStatus:{}\n"
                              "\t\t\t\t\t\tSendTime:{} UpdateTime:{} ErrorID:{} ErrorMsg:{} RiskID:{}", op.c_str(),
                              req.Product, req.Account, req.Ticker, req.OrderType, req.Direction,
                              req.Offset, req.Price, req.Volume, req.EngineID, req.RiskStatus,
                              req.SendTime, req.UpdateTime, req.ErrorID, req.ErrorMsg, req.RiskID);
}

void RiskEngine::PrintActionRequest(const Message::TActionRequest& req, const std::string& op)
{
    Utils::gLogger->Log->debug("RiskEngine::PrintActionRequest, {} Account:{} OrderRef:{} EngineID:{}\n"
                              "\t\t\t\t\t\tRiskStatus:{} UpdateTime:{} ErrorID:{} ErrorMsg:{} RiskID:{}",
                              op.c_str(), req.Account, req.OrderRef, req.EngineID, req.RiskStatus,
                              req.UpdateTime, req.ErrorID, req.ErrorMsg, req.RiskID);
}
