#include "RiskEngine.h"


Utils::LockFreeQueue<Message::PackMessage> RiskEngine::m_RiskResponseQueue(1 << 12);
XRiskLimit RiskEngine::m_XRiskLimit;
std::unordered_map<std::string, Message::TRiskReport> RiskEngine::m_TickerCancelledCounterMap;
std::unordered_map<std::string, Message::TRiskReport> RiskEngine::m_AccountLockedStatusMap;
std::unordered_map<std::string, Message::TRiskReport> RiskEngine::m_RiskLimitMap;

RiskEngine::RiskEngine()
{
    m_RiskJudgeServer = NULL;
    m_HPPackClient = NULL;
    m_WorkThread = NULL;

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
        FMTLOG(fmtlog::WRN, "RiskEngine::LoadXRiskJudgeConfig failed, {}", errorString);
    }
    else
    {
        FMTLOG(fmtlog::INF, "RiskEngine::LoadConfig successed, LoadXRiskJudgeConfig {}", yml);
    }
    m_RiskDBManager = Utils::Singleton<RiskDBManager>::GetInstance();
    ret = m_RiskDBManager->LoadDataBase(m_XRiskJudgeConfig.RiskDBPath, errorString);
    if(!ret)
    {
        FMTLOG(fmtlog::WRN, "RiskEngine::LoadConfig LoadDataBase {} failed, {}", m_XRiskJudgeConfig.RiskDBPath, errorString);
    }
    else
    {
        FMTLOG(fmtlog::INF, "RiskEngine::LoadConfig LoadDataBase successed, {}", m_XRiskJudgeConfig.RiskDBPath);
    }
    // Select RiskLimitTable
    QueryRiskLimit();
    // Select CancelledCountTable
    QueryCancelledCount();
    // Select LockedAccountTable
    QueryLockedAccount();
}

void RiskEngine::SetCommand(const std::string& cmd)
{
    m_Command = cmd;
    FMTLOG(fmtlog::INF, "RiskEngine::SetCommand cmd:{}", m_Command);
}

void RiskEngine::Start()
{
    // 登陆注册XWatcher
    RegisterClient(m_XRiskJudgeConfig.XWatcherIP.c_str(), m_XRiskJudgeConfig.XWatcherPort);

    FMTLOG(fmtlog::INF, "RiskEngine::Start {} Server", m_XRiskJudgeConfig.RiskServerName);
    m_RiskJudgeServer = new RiskJudgeServer();
    m_RiskJudgeServer->Start(m_XRiskJudgeConfig.RiskServerName);

    // Update App Status
    InitAppStatus();
    
    m_WorkThread = new std::thread(&RiskEngine::WorkThreadFunc, this);
    m_RiskJudgeServer->Join();
    m_WorkThread->join();
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

void RiskEngine::WorkThreadFunc()
{
    FMTLOG(fmtlog::INF, "RiskEngine::WorkThreadFunc Risk Service {} Running", m_XRiskJudgeConfig.RiskID);
    Message::PackMessage message;
    while (true)
    {
        bool ret = m_RiskJudgeServer->Pop(message);
        if(ret)
        {
            HandleRequest(message);
        }
        ret = m_RiskResponseQueue.Pop(message);
        if(ret)
        {
            HandleResponse(message);
        }
        ret = m_HPPackClient->m_PackMessageQueue.Pop(message);
        if(ret)
        {
            if(message.MessageType == Message::EMessageType::ECommand)
            {
                HandleCommand(message);
            }
        }
        static long CurrentTimeStamp = 0;
        long Sec = Utils::getTimeStampMs(Utils::getCurrentTimeMs() + 11) / 1000;
        if(CurrentTimeStamp < Sec)
        {
            CurrentTimeStamp = Sec;
        }
        if(CurrentTimeStamp % 10 == 0)
        {
            m_HPPackClient->ReConnect();
            CurrentTimeStamp += 1;
        }
    }
}

void RiskEngine::HandleRequest(Message::PackMessage& msg)
{
    FMTLOG(fmtlog::INF, "RiskEngine::HandleRequestMessage receive message {:#X} ChannelID:{}", msg.MessageType, msg.ChannelID);
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
    case Message::EMessageType::EAccountFund:
        break;
    case Message::EMessageType::EAccountPosition:
        break;
    case Message::EMessageType::ELoginRequest:
        break;
    case Message::EMessageType::EEventLog:
        m_HPPackClient->SendData((const unsigned char*)&msg, sizeof(msg));
        break;
    default:
        FMTLOG(fmtlog::WRN, "RiskEngine::HandleRequestMessage Unkown Message Type:{:#X}", msg.MessageType);
        break;
    }
}

void RiskEngine::HandleResponse(const Message::PackMessage& msg)
{
    switch (msg.MessageType)
    {
    case Message::EMessageType::EOrderRequest:
    case Message::EMessageType::EActionRequest:
    {
        m_RiskJudgeServer->Push(msg);
        FMTLOG(fmtlog::INF, "RiskEngine::HandleResponse send msg to ChannelID:{}", msg.ChannelID);
        break;
    }
    case Message::EMessageType::ERiskReport:
        m_HPPackClient->SendData((const unsigned char*)&msg, sizeof(msg));
        break;
    default:
        FMTLOG(fmtlog::WRN, "RiskEngine::HandleResponseMessage Unkown Message Type:{:#X}", msg.MessageType);
        break;
    }
}

void RiskEngine::HandleCommand(const Message::PackMessage& msg)
{
    FMTLOG(fmtlog::INF, "RiskEngine::HandleCommand Command:{}", msg.Command.Command);
    // Handle Risk Command
    HandleRiskCommand(msg.Command);
}

void RiskEngine::HandleOrderStatus(const Message::PackMessage& msg)
{
    const Message::TOrderStatus& OrderStatus = msg.OrderStatus;
    // Add Pending Order
    {
        if(Message::EOrderStatusType::EPARTTRADED == OrderStatus.OrderStatus ||
                Message::EOrderStatusType::EEXCHANGE_ACK == OrderStatus.OrderStatus ||
                Message::EOrderStatusType::EORDER_SENDED == OrderStatus.OrderStatus)
        {
            std::string OrderRef = OrderStatus.OrderRef;
            auto it = m_PendingOrderMap.find(OrderRef);
            if(m_PendingOrderMap.end() == it)
            {
                m_PendingOrderMap[OrderRef] = OrderStatus;
                std::list<Message::TOrderStatus>& orderList = m_TickerPendingOrderListMap[OrderStatus.Ticker];
                orderList.push_back(OrderStatus);
                FMTLOG(fmtlog::INF, "RiskEngine::HandleOrderStatus, Add Pending Order, Product:{} Account:{} Ticker:{} OrderRef:{} Pending Number:{}",
                        OrderStatus.Product, OrderStatus.Account, OrderStatus.Ticker, OrderStatus.OrderRef, orderList.size());
            }
            return;
        }
    }
    // Remove Pending Order when Order end
    {
        if(Message::EOrderStatusType::EALLTRADED == OrderStatus.OrderStatus ||
                Message::EOrderStatusType::EPARTTRADED_CANCELLED == OrderStatus.OrderStatus ||
                Message::EOrderStatusType::ECANCELLED == OrderStatus.OrderStatus ||
                Message::EOrderStatusType::EBROKER_ERROR == OrderStatus.OrderStatus ||
                Message::EOrderStatusType::EEXCHANGE_ERROR)
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
                    FMTLOG(fmtlog::DBG, "RiskEngine::HandleOrderStatus Pengding Order Ticker:{} OrderRef:{}", it->Ticker, it->OrderRef);
                }
                FMTLOG(fmtlog::INF, "RiskEngine::HandleOrderStatus, Remove Pending Order, Product:{} Account:{} Ticker:{} OrderRef:{} Pengding Number:{}",
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
        bool cancelled = Message::EOrderStatusType::EPARTTRADED_CANCELLED == OrderStatus.OrderStatus ||
                         Message::EOrderStatusType::ECANCELLED == OrderStatus.OrderStatus ||
                         Message::EOrderStatusType::EEXCHANGE_ERROR == OrderStatus.OrderStatus;
        if(OrderStatus.OrderType == Message::EOrderType::ELIMIT && cancelled)
        {
            std::string Account = OrderStatus.Account;
            std::string Ticker = OrderStatus.Ticker;
            std::string Key = Account + ":" + Ticker;
            int CancelledCount = 0;
            auto it = m_TickerCancelledCounterMap.find(Key);
            if(m_TickerCancelledCounterMap.end() != it)
            {
                Message::TRiskReport& report = it->second;
                report.CancelledCount += 1;
                CancelledCount = report.CancelledCount;
                strncpy(report.UpdateTime, Utils::getCurrentTimeUs(), sizeof(report.UpdateTime));
                strncpy(report.RiskID, m_XRiskJudgeConfig.RiskID.c_str(), sizeof(report.RiskID));
                strncpy(report.Account, OrderStatus.Account, sizeof(report.Account));
                strncpy(report.Ticker, OrderStatus.Ticker, sizeof(report.Ticker));
                // Update SQLite CancelledCountTable
                std::string SQL = fmt::format("UPDATE CancelledCountTable SET CancelledCount={}, Trader='{}', UpdateTime='{}' WHERE RiskID='{}' AND Account='{}' AND Ticker='{}';",
                                            report.CancelledCount, report.Trader, report.UpdateTime, report.RiskID, report.Account, report.Ticker);
                std::string errorString;
                bool ok = m_RiskDBManager->UpdateCancelledCountTable(SQL, "UPDATE", &RiskEngine::sqlite3_callback_CancelledCount, errorString);
                strncpy(report.Event, errorString.c_str(), sizeof(report.Event));
            }
            else
            {
                Message::TRiskReport& report = m_TickerCancelledCounterMap[Key];
                report.UpperLimit = m_XRiskLimit.TickerCancelLimit;
                report.CancelledCount += 1;
                CancelledCount = report.CancelledCount;
                report.ReportType = Message::ERiskReportType::ERISK_TICKER_CANCELLED;
                strncpy(report.RiskID, m_XRiskJudgeConfig.RiskID.c_str(), sizeof(report.RiskID));
                strncpy(report.Account, OrderStatus.Account, sizeof(report.Account));
                strncpy(report.Ticker, OrderStatus.Ticker, sizeof(report.Ticker));
                strncpy(report.UpdateTime, Utils::getCurrentTimeUs(), sizeof(report.UpdateTime));
                // Update SQLite CancelledCountTable
                std::string SQL = fmt::format("INSERT INTO CancelledCountTable(RiskID,Account,Ticker,CancelledCount,UpperLimit,Trader,UpdateTime) VALUES ('{}', '{}', '{}', {}, {}, '{}', '{}');",
                                            report.RiskID, report.Account, report.Ticker, report.CancelledCount, report.UpperLimit, report.Trader, report.UpdateTime);
                std::string errorString;
                bool ok = m_RiskDBManager->UpdateCancelledCountTable(SQL, "INSERT", &RiskEngine::sqlite3_callback_CancelledCount, errorString);
                strncpy(report.Event, errorString.c_str(), sizeof(report.Event));
            }
            // Update Risk to Monitor
            {
                Message::PackMessage message;
                memset(&message, 0, sizeof(message));
                message.MessageType = Message::EMessageType::ERiskReport;
                memcpy(&message.RiskReport, &m_TickerCancelledCounterMap[Key], sizeof(message.RiskReport));
                while(!m_RiskResponseQueue.Push(message));
            }
            FMTLOG(fmtlog::INF, "RiskEngine::HandleOrderStatus, Update Cancelled Order Counter, Product:{} Account:{} Ticker:{} "
                                "OrderRef:{} OrderStatus:{} OrderType:{} CancelledCount:{}",
                    OrderStatus.Product, OrderStatus.Account, OrderStatus.Ticker, OrderStatus.OrderRef, OrderStatus.OrderStatus, OrderStatus.OrderType, CancelledCount);
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
        while(!m_RiskResponseQueue.Push(msg));
        FMTLOG(fmtlog::INF, "RiskEngine::HandleOrderRequest Risk Check Init, Ticker:{} Account:{} ChannelID:{}", 
                msg.OrderRequest.Ticker, msg.OrderRequest.Account, msg.ChannelID);
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
                fmt::format_to_n(RiskEvent.Trader, sizeof(RiskEvent.Trader), "{:#X}", msg.OrderRequest.EngineID);
                strncpy(RiskEvent.UpdateTime, Utils::getCurrentTimeUs(), sizeof(RiskEvent.UpdateTime));
            }
            else if(Message::EMessageType::EActionRequest == msg.MessageType)
            {
                strncpy(RiskEvent.Account, msg.ActionRequest.Account, sizeof(RiskEvent.Account));
                strncpy(RiskEvent.Event, msg.ActionRequest.ErrorMsg, sizeof(RiskEvent.Event));
                fmt::format_to_n(RiskEvent.Trader, sizeof(RiskEvent.Trader), "{:#X}", msg.ActionRequest.EngineID);
                strncpy(RiskEvent.UpdateTime, Utils::getCurrentTimeUs(), sizeof(RiskEvent.UpdateTime));
            }
            memcpy(&message.RiskReport, &RiskEvent, sizeof(message.RiskReport));
            // 风控拦截事件报告
            while(!m_RiskResponseQueue.Push(message));
        }
        // 风控检查结果
        while(!m_RiskResponseQueue.Push(msg));
    }
    int end = Utils::getTimeUs();
    FMTLOG(fmtlog::INF, "RiskEngine::Check Risk Check Latency:{}", end - start);
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
            fmt::format_to_n(msg.OrderRequest.ErrorMsg, sizeof(msg.OrderRequest.ErrorMsg), 
                            "FlowLimited, ErrorID:{:#X} flow:{} limit:{} Product:{} Account:{} EngineID:{:#X}",
                            msg.OrderRequest.ErrorID, m_AccountFlowLimitedMap[Account], m_XRiskLimit.FlowLimit,
                            msg.OrderRequest.Product, msg.OrderRequest.Account, msg.OrderRequest.EngineID);
            FMTLOG(fmtlog::WRN, "RiskEngine::FlowLimited Check failed, {}", msg.OrderRequest.ErrorMsg);
        }
        else if(Message::EMessageType::EActionRequest == msg.MessageType)
        {
            msg.ActionRequest.ErrorID =  Message::ERiskRejectedType::EFLOW_LIMITED;
            msg.ActionRequest.RiskStatus = Message::ERiskStatusType::ECHECKED_NOPASS;
            fmt::format_to_n(msg.ActionRequest.ErrorMsg, sizeof(msg.ActionRequest.ErrorMsg), 
                            "FlowLimited, ErrorID:{:#X} flow:{} limit:{} Account:{} EngineID:{:#X}",
                            msg.ActionRequest.ErrorID, m_AccountFlowLimitedMap[Account], m_XRiskLimit.FlowLimit,
                            msg.ActionRequest.Account, msg.ActionRequest.EngineID);
            FMTLOG(fmtlog::WRN, "RiskEngine::FlowLimited Check failed, {}", msg.ActionRequest.ErrorMsg);
        }
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
                else if(Ticker.find(msg.OrderRequest.Ticker) != std::string::npos)
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
                else if(Ticker.find(msg.OrderRequest.Ticker) != std::string::npos)
                {
                    ret = false;
                    LockedSide = "LockedSide:Sell Ticker:" + Ticker;
                }
            }
        }
    }
    if(!ret)
    {
        if(Message::EMessageType::EOrderRequest == msg.MessageType)
        {
            msg.OrderRequest.ErrorID =  Message::ERiskRejectedType::EACCOUNT_LOCKED;
            msg.OrderRequest.RiskStatus = Message::ERiskStatusType::ECHECKED_NOPASS;
            fmt::format_to_n(msg.OrderRequest.ErrorMsg, sizeof(msg.OrderRequest.ErrorMsg), 
                            "AccountLocked {}, ErrorID:{:#X} Product:{} Account:{} EngineID:{:#X}",
                            LockedSide, msg.OrderRequest.ErrorID, msg.OrderRequest.Product, msg.OrderRequest.Account,
                            msg.OrderRequest.EngineID);
            FMTLOG(fmtlog::WRN, "RiskEngine::AccountLocked Check failed, {}", msg.OrderRequest.ErrorMsg);
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
        FMTLOG(fmtlog::INF, "RiskEngine::SelfMatched Check, Ticker:{}, Pending Order Number:{}",
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
                fmt::format_to_n(msg.OrderRequest.ErrorMsg, sizeof(msg.OrderRequest.ErrorMsg),
                                "SelfMatched, ErrorID:{:#X} Product:{} Account:{} OrderToken:{}, Matched OrderRef:{}",
                                msg.OrderRequest.ErrorID, msg.OrderRequest.Product, msg.OrderRequest.Account, msg.OrderRequest.OrderToken, it->OrderRef);
                FMTLOG(fmtlog::WRN, "RiskEngine::SelfMatched Check failed, {}", msg.OrderRequest.ErrorMsg);
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
                std::string Account = it->second.Account;
                std::string Ticker = it->second.Ticker;
                std::string Key = Account + ":" + Ticker;
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
                    fmt::format_to_n(msg.ActionRequest.ErrorMsg, sizeof(msg.ActionRequest.ErrorMsg), 
                                    "CancelLimited, ErrorID:{:#X} Product:{} Account:{} OrderRef:{}, CancelRequestCount:{} Ticker Cancelled Limit:{}",
                                    msg.ActionRequest.ErrorID, it->second.Product, it->second.Account, it->second.OrderRef,
                                    CancelRequestCount, m_XRiskLimit.TickerCancelLimit);
                    FMTLOG(fmtlog::WRN, "RiskEngine::CancelLimited Check failed, {}", msg.ActionRequest.ErrorMsg);
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
                    fmt::format_to_n(msg.ActionRequest.ErrorMsg, sizeof(msg.ActionRequest.ErrorMsg), 
                                    "CancelLimited, ErrorID:{:#X} Product:{} Account:{} OrderRef:{}, CancelRequestCount:{} Order Cancelled Limit:{}",
                                    msg.ActionRequest.ErrorID, it->second.Product, it->second.Account, it->second.OrderRef,
                                    CancelRequestCount, m_XRiskLimit.OrderCancelLimit);
                    FMTLOG(fmtlog::WRN, "RiskEngine::CancelLimited Check failed, {}", msg.ActionRequest.ErrorMsg);
                }
            }
        }
    }
    return ret;
}

void RiskEngine::PrintOrderRequest(const Message::TOrderRequest& req, const std::string& op)
{
    FMTLOG(fmtlog::DBG, "RiskEngine::PrintOrderRequest, {} Product:{} Account:{} Ticker:{} OrderType:{} Direction:{} Offset:{} "
                        "Price:{} Volume:{} EngineID:{} RiskStatus:{} SendTime:{} UpdateTime:{} ErrorID:{} ErrorMsg:{} RiskID:{}", 
            op, req.Product, req.Account, req.Ticker, req.OrderType, req.Direction, req.Offset, req.Price, req.Volume, req.EngineID, req.RiskStatus,
            req.SendTime, req.UpdateTime, req.ErrorID, req.ErrorMsg, req.RiskID);
}

void RiskEngine::PrintActionRequest(const Message::TActionRequest& req, const std::string& op)
{
    FMTLOG(fmtlog::DBG, "RiskEngine::PrintActionRequest, {} Account:{} OrderRef:{} EngineID:{} RiskStatus:{} UpdateTime:{} ErrorID:{} ErrorMsg:{} RiskID:{}",
            op, req.Account, req.OrderRef, req.EngineID, req.RiskStatus, req.UpdateTime, req.ErrorID, req.ErrorMsg, req.RiskID);
}

bool RiskEngine::QueryRiskLimit()
{
    std::string errorString;
    bool ret = m_RiskDBManager->QueryRiskLimit(&RiskEngine::sqlite3_callback_RiskLimit, errorString);
    if(!ret)
    {
        FMTLOG(fmtlog::WRN, "RiskEngine::QueryRiskLimit failed, {}", errorString);
    }
    else
    {
        for (auto it = m_RiskLimitMap.begin(); m_RiskLimitMap.end() != it; it++)
        {
            Message::PackMessage message;
            memset(&message, 0, sizeof(message));
            message.MessageType = Message::EMessageType::ERiskReport;
            memcpy(&message.RiskReport, &it->second, sizeof(message.RiskReport));
            while(!m_RiskResponseQueue.Push(message));
        }
    }
    return ret;
}

bool RiskEngine::QueryLockedAccount()
{
    std::string errorString;
    bool ret = m_RiskDBManager->QueryLockedAccount(&RiskEngine::sqlite3_callback_LockedAccount, errorString);
    if(!ret)
    {
        FMTLOG(fmtlog::WRN, "RiskEngine::QueryLockedAccount failed, {}", errorString);
    }
    else
    {
        for (auto it = m_AccountLockedStatusMap.begin(); m_AccountLockedStatusMap.end() != it; it++)
        {
            Message::PackMessage message;
            memset(&message, 0, sizeof(message));
            message.MessageType = Message::EMessageType::ERiskReport;
            memcpy(&message.RiskReport, &it->second, sizeof(message.RiskReport));
            while(!m_RiskResponseQueue.Push(message));
        }
    }
    return ret;
}

bool RiskEngine::QueryCancelledCount()
{
    std::string errorString;
    bool ret = m_RiskDBManager->QueryCancelledCount(&RiskEngine::sqlite3_callback_CancelledCount, errorString);
    if(!ret)
    {
        FMTLOG(fmtlog::WRN, "RiskEngine::QueryCancelledCount failed, {}", errorString);
    }
    else
    {
        for (auto it = m_TickerCancelledCounterMap.begin(); m_TickerCancelledCounterMap.end() != it; it++)
        {
            Message::PackMessage message;
            memset(&message, 0, sizeof(message));
            message.MessageType = Message::EMessageType::ERiskReport;
            memcpy(&message.RiskReport, &it->second, sizeof(message.RiskReport));
            while(!m_RiskResponseQueue.Push(message));
        }
    }
    return ret;
}

int RiskEngine::sqlite3_callback_RiskLimit(void *data, int argc, char **argv, char **azColName)
{
    for(int i = 0; i < argc; i++)
    {
        FMTLOG(fmtlog::INF, "RiskEngine::sqlite3_callback_RiskLimit, {} {} = {}", (char*)data, azColName[i], argv[i]);
        std::string colName = azColName[i];
        std::string value = argv[i];
        static std::string RiskID;
        static int flowLimit = 0;
        static int cancelledLimit = 0;
        static int orderCancelLimit = 0;
        static std::string Trader;

        if(Utils::equalWith(colName, "RiskID"))
        {
            RiskID = value;
        }
        if(Utils::equalWith(colName, "FlowLimit"))
        {
            flowLimit = atoi(value.c_str());
            m_XRiskLimit.FlowLimit = flowLimit;
        }
        if(Utils::equalWith(colName, "TickerCancelLimit"))
        {
            cancelledLimit = atoi(value.c_str());
            m_XRiskLimit.TickerCancelLimit = cancelledLimit;
        }
        if(Utils::equalWith(colName, "OrderCancelLimit"))
        {
            orderCancelLimit = atoi(value.c_str());
            m_XRiskLimit.OrderCancelLimit = orderCancelLimit;
        }
        if(Utils::equalWith(colName, "Trader"))
        {
            std::string Trader = value;
        }
        if(Utils::equalWith(colName, "UpdateTime"))
        {
            std::string UpdateTime = value;
            Message::TRiskReport& report = m_RiskLimitMap[RiskID];
            report.ReportType = Message::ERiskReportType::ERISK_LIMIT;
            strncpy(report.RiskID, RiskID.c_str(), sizeof(report.RiskID));
            report.FlowLimit = flowLimit;
            report.TickerCancelLimit = cancelledLimit;
            report.OrderCancelLimit = orderCancelLimit;
            strncpy(report.Trader, Trader.c_str(), sizeof(report.Trader));
            strncpy(report.UpdateTime, UpdateTime.c_str(), sizeof(report.UpdateTime));
        }
    }
    return 0;
}

int RiskEngine::sqlite3_callback_LockedAccount(void *data, int argc, char **argv, char **azColName)
{
    for(int i = 0; i < argc; i++)
    {
        FMTLOG(fmtlog::INF, "RiskEngine::sqlite3_callback_LockedAccount, {} {} = {}", (char*)data,  azColName[i], argv[i]);
        std::string colName = azColName[i];
        std::string value = argv[i];
        static std::string RiskID;
        static std::string Account;
        static std::string Ticker;
        static std::string Trader;
        static int LockedSide = 0;
        if(Utils::equalWith(colName, "RiskID"))
        {
            RiskID = value;
        }
        if(Utils::equalWith(colName, "Account"))
        {
            Account = value;
        }
        if(Utils::equalWith(colName, "Ticker"))
        {
            Ticker = value;
        }
        if(Utils::equalWith(colName, "LockedSide"))
        {
            LockedSide = atoi(value.c_str());
        }
        if(Utils::equalWith(colName, "Trader"))
        {
            Trader = value;
        }
        if(Utils::equalWith(colName, "UpdateTime"))
        {
            std::string UpdateTime = value;
            Message::TRiskReport& report = m_AccountLockedStatusMap[Account];
            strncpy(report.RiskID, RiskID.c_str(), sizeof(report.RiskID));
            strncpy(report.Account, Account.c_str(), sizeof(report.Account));
            report.LockedSide = LockedSide;
            report.ReportType = Message::ERiskReportType::ERISK_ACCOUNT_LOCKED;
            strncpy(report.Ticker, Ticker.c_str(), sizeof(report.Ticker));
            strncpy(report.Trader, Trader.c_str(), sizeof(report.Trader));
            strncpy(report.UpdateTime, UpdateTime.c_str(), sizeof(report.UpdateTime));
        }
    }
    return 0;
}

int RiskEngine::sqlite3_callback_CancelledCount(void *data, int argc, char **argv, char **azColName)
{
    for(int i = 0; i < argc; i++)
    {
        FMTLOG(fmtlog::INF, "RiskEngine::sqlite3_callback_CancelledCount, {} {} = {}", (char*)data, azColName[i], argv[i]);
        std::string colName = azColName[i];
        std::string value = argv[i];
        static std::string RiskID;
        static std::string Account;
        static std::string Ticker;
        static int cancelCount = 0;
        static int UpperLimit = 0;
        static std::string Trader;

        if(Utils::equalWith(colName, "RiskID"))
        {
            RiskID = value;
        }
        if(Utils::equalWith(colName, "Account"))
        {
            Account = value;
        }
        if(Utils::equalWith(colName, "Ticker"))
        {
            Ticker = value;
        }
        if(Utils::equalWith(colName, "CancelledCount"))
        {
            cancelCount = atoi(value.c_str());
        }
        if(Utils::equalWith(colName, "UpperLimit"))
        {
            UpperLimit = atoi(value.c_str());
        }
        if(Utils::equalWith(colName, "Trader"))
        {
            Trader = value;
        }
        if(Utils::equalWith(colName, "UpdateTime"))
        {
            std::string UpdateTime = value;
            std::string Key = Account + ":" + Ticker;
            Message::TRiskReport& report = m_TickerCancelledCounterMap[Key];
            report.ReportType = Message::ERiskReportType::ERISK_TICKER_CANCELLED;
            strncpy(report.RiskID, RiskID.c_str(), sizeof(report.RiskID));
            strncpy(report.Account, Account.c_str(), sizeof(report.Account));
            strncpy(report.Ticker, Ticker.c_str(), sizeof(report.Ticker));
            report.CancelledCount = cancelCount;
            report.UpperLimit = UpperLimit;
            strncpy(report.Trader, Trader.c_str(), sizeof(report.Trader));
            strncpy(report.UpdateTime, UpdateTime.c_str(), sizeof(report.UpdateTime));
        }
    }
    return 0;
}

void RiskEngine::HandleRiskCommand(const Message::TCommand& command)
{
    FMTLOG(fmtlog::INF, "RiskEngine::HandleRiskCommand CmdType:{}, Command:{}", command.CmdType, command.Command);
    std::string cmd = command.Command;
    Message::TRiskReport RiskEvent;
    memset(&RiskEvent, 0, sizeof(RiskEvent));
    RiskEvent.ReportType = Message::ERiskReportType::ERISK_EVENTLOG;

    if(Message::ECommandType::EUPDATE_RISK_LIMIT == command.CmdType)
    {
        std::string sql, op;
        if(ParseUpdateRiskLimitCommand(cmd, sql, op, RiskEvent))
        {
            std::string errorString;
            bool ok = m_RiskDBManager->UpdateRiskLimitTable(sql, op, &RiskEngine::sqlite3_callback_RiskLimit, errorString);
            strncpy(RiskEvent.Event, errorString.c_str(), sizeof(RiskEvent.Event));
            if(ok)
            {
                // Update CancelledCountTable
                std::string SQL = fmt::format("UPDATE CancelledCountTable SET UpperLimit={}, Trader='{}', UpdateTime='{}' WHERE RiskID='{}';",
                                                RiskEvent.TickerCancelLimit, RiskEvent.Trader, RiskEvent.UpdateTime, RiskEvent.RiskID);
                bool ret = m_RiskDBManager->UpdateCancelledCountTable(SQL, "UPDATE", &RiskEngine::sqlite3_callback_CancelledCount, errorString);
                if(!ret)
                {
                    FMTLOG(fmtlog::WRN, "RiskEngine::Update CancelledCountTable failed, {}, sql:{}", errorString, SQL);
                }
                else
                {
                    QueryCancelledCount();
                }
            }
            QueryRiskLimit();
        }
        {
            Message::PackMessage message;
            memset(&message, 0, sizeof(message));
            message.MessageType = Message::EMessageType::ERiskReport;
            memcpy(&message.RiskReport, &RiskEvent, sizeof(message.RiskReport));
            while(!m_RiskResponseQueue.Push(message));
        }
    }
    else if(Message::ECommandType::EUPDATE_RISK_ACCOUNT_LOCKED == command.CmdType)
    {
        std::string sql, op;
        FMTLOG(fmtlog::WRN, "RiskEngine::ParseUpdateLockedAccountCommand start size:{}", m_AccountLockedStatusMap.size());
        if(ParseUpdateLockedAccountCommand(cmd, sql, op, RiskEvent))
        {
            std::string errorString;
            bool ok = m_RiskDBManager->UpdateLockedAccountTable(sql, op, &RiskEngine::sqlite3_callback_LockedAccount, errorString);
            strncpy(RiskEvent.Event, errorString.c_str(), sizeof(RiskEvent.Event));
            std::list<std::string> accounts;
            for (auto it = m_AccountLockedStatusMap.begin(); m_AccountLockedStatusMap.end() != it; it++)
            {
                if(ok && Utils::equalWith(op, "DELETE") && sql.find(it->first) != std::string::npos)
                {
                    it->second.LockedSide = Message::ERiskLockedSide::EUNLOCK;
                    accounts.push_back(it->first);
                }
            }
            QueryLockedAccount();
            for (auto it = accounts.begin(); it != accounts.end(); it++)
            {
                m_AccountLockedStatusMap.erase(*it);
            }
            FMTLOG(fmtlog::WRN, "RiskEngine::ParseUpdateLockedAccountCommand end size:{}", m_AccountLockedStatusMap.size());
        }
        {
            Message::PackMessage message;
            memset(&message, 0, sizeof(message));
            message.MessageType = Message::EMessageType::ERiskReport;
            memcpy(&message.RiskReport, &RiskEvent, sizeof(message.RiskReport));
            while(!m_RiskResponseQueue.Push(message));
        }
    }
}

bool RiskEngine::ParseUpdateLockedAccountCommand(const std::string& cmd, std::string& sql, std::string& op, Message::TRiskReport& event)
{
    bool ret = true;
    sql.clear();
    std::vector<std::string> items;
    Utils::Split(cmd, ",", items);
    if(5 == items.size())
    {
        std::vector<std::string> keyValue;
        Utils::Split(items[0], ":", keyValue);
        std::string RiskID = keyValue[1];
        strncpy(event.RiskID, RiskID.c_str(), sizeof(event.RiskID));

        keyValue.clear();
        Utils::Split(items[1], ":", keyValue);
        std::string Account = keyValue[1];
        strncpy(event.Account, Account.c_str(), sizeof(event.Account));

        keyValue.clear();
        Utils::Split(items[2], ":", keyValue);
        std::string Ticker = keyValue[1];
        strncpy(event.Ticker, Ticker.c_str(), sizeof(event.Ticker));

        keyValue.clear();
        Utils::Split(items[3], ":", keyValue);
        int LockSide = atoi(keyValue[1].c_str());

        keyValue.clear();
        Utils::Split(items[4], ":", keyValue);
        std::string Trader = keyValue[1];
        strncpy(event.Trader, Trader.c_str(), sizeof(event.Trader));
        std::string CurrentTime = Utils::getCurrentTimeUs();
        strncpy(event.UpdateTime, CurrentTime.c_str(), sizeof(event.UpdateTime));
        auto it = m_AccountLockedStatusMap.find(Account);
        if(m_AccountLockedStatusMap.end() == it)
        {
            // Insert
            if(Message::ERiskLockedSide::EUNLOCK != LockSide)
            {
                sql = fmt::format("INSERT INTO LockedAccountTable(RiskID, Account, Ticker, LockedSide, Trader, UpdateTime) VALUES ('{}', '{}', '{}', {}, '{}', '{}');",
                                RiskID, Account, Ticker, LockSide, Trader, CurrentTime);
                op = "INSERT";
            }
            else
            {
                ret= false;
                fmt::format_to_n(event.Event, sizeof(event.Event), "Account:{} not found, can't UnLock. invalid command:{}", Account, cmd);
                FMTLOG(fmtlog::WRN, "RiskEngine::ParseUpdateAccountLockedCommand invalid command, Account:{} not found, can't UnLock. cmd:{}",
                        Account, cmd);
            }
        }
        else
        {
            // Update
            if(Message::ERiskLockedSide::EUNLOCK == LockSide)
            {
                if(Message::ERiskLockedSide::EUNLOCK != it->second.LockedSide)
                {
                    sql = fmt::format("DELETE FROM LockedAccountTable WHERE RiskID='{}' AND Account='{}';", RiskID, Account);
                    op = "DELETE";
                }
                else
                {
                    ret= false;
                    fmt::format_to_n(event.Event, sizeof(event.Event), "Account:{} UnLocked, can't UnLock. invalid command:{}", Account, cmd);
                }
            }
            else
            {
                sql = fmt::format("UPDATE LockedAccountTable SET Ticker='{}', LockedSide={}, Trader='{}', UpdateTime='{}' WHERE RiskID='{}' AND Account='{}';",
                                Ticker, LockSide, Trader, Utils::getCurrentTimeUs(), RiskID, Account);
                op = "UPDATE";
            }
        }
        FMTLOG(fmtlog::WRN, "RiskEngine::ParseUpdateLockedAccountCommand, RiskID:{} Account:{} Ticker:{} LockSide:{} MapSize:{}",
                RiskID, Account, Ticker, LockSide, m_AccountLockedStatusMap.size());
    }
    else
    {
        ret = false;
        fmt::format_to_n(event.Event, sizeof(event.Event), "invalid command:{}", cmd);
        FMTLOG(fmtlog::WRN, "RiskEngine::ParseUpdateAccountLockedCommand invalid command, {}", cmd);
    }
    return ret;
}

bool RiskEngine::ParseUpdateRiskLimitCommand(const std::string& cmd, std::string& sql, std::string& op, Message::TRiskReport& event)
{
    bool ret = true;
    sql.clear();
    std::vector<std::string> items;
    Utils::Split(cmd, ",", items);
    if(5 == items.size())
    {
        std::vector<std::string> keyValue;
        Utils::Split(items[0], ":", keyValue);
        std::string RiskID = keyValue[1];
        strncpy(event.RiskID, RiskID.c_str(), sizeof(event.RiskID));

        keyValue.clear();
        Utils::Split(items[1], ":", keyValue);
        int FlowLimit = atoi(keyValue[1].c_str());
        event.FlowLimit = FlowLimit;

        keyValue.clear();
        Utils::Split(items[2], ":", keyValue);
        int TickerCancelLimit = atoi(keyValue[1].c_str());
        event.TickerCancelLimit = TickerCancelLimit;

        keyValue.clear();
        Utils::Split(items[3], ":", keyValue);
        int OrderCancelLimit = atoi(keyValue[1].c_str());
        event.OrderCancelLimit = OrderCancelLimit;

        keyValue.clear();
        Utils::Split(items[4], ":", keyValue);
        std::string Trader = keyValue[1];
        strncpy(event.Trader, Trader.c_str(), sizeof(event.Trader));
        std::string CurrentTime = Utils::getCurrentTimeUs();
        strncpy(event.UpdateTime, CurrentTime.c_str(), sizeof(event.UpdateTime));

        auto it = m_RiskLimitMap.find(RiskID);
        if(m_RiskLimitMap.end() == it)
        {
            // Insert
            sql = fmt::format("INSERT INTO RiskLimitTable(RiskID,FlowLimit,TickerCancelLimit,OrderCancelLimit,Trader,UpdateTime) VALUES('{}',{},{},{},'{}','{}');",
                            RiskID, FlowLimit, TickerCancelLimit, OrderCancelLimit, Trader, CurrentTime);
            op = "INSERT";
        }
        else
        {
            // Update
            sql = fmt::format("UPDATE RiskLimitTable SET FlowLimit={},TickerCancelLimit={},OrderCancelLimit={},Trader='{}',UpdateTime='{}' WHERE RiskID='{}';",
                            FlowLimit, TickerCancelLimit, OrderCancelLimit, Trader, CurrentTime, RiskID);
            op = "UPDATE";
        }
        FMTLOG(fmtlog::INF, "RiskEngine::ParseUpdateRiskLimitCommand, RiskID:{} FlowLimit:{} TickerCancelLimit:{} OrderCancelLimit:{} MapSize:{}",
                RiskID, FlowLimit, TickerCancelLimit, OrderCancelLimit, m_RiskLimitMap.size());
    }
    else
    {
        ret = false;
        fmt::format_to_n(event.Event, sizeof(event.Event), "invalid command:{}", cmd);
        FMTLOG(fmtlog::WRN, "RiskEngine::ParseUpdateRiskLimitCommand invalid command, {}", cmd);
    }
    return ret;
}

void RiskEngine::InitAppStatus()
{
    Message::PackMessage message;
    message.MessageType = Message::EMessageType::EAppStatus;
    RiskEngine::UpdateAppStatus(m_Command, message.AppStatus);
    m_HPPackClient->SendData((const unsigned char*)&message, sizeof(message));
}

void RiskEngine::UpdateAppStatus(const std::string& cmd, Message::TAppStatus& AppStatus)
{
    std::vector<std::string> ItemVec;
    Utils::Split(cmd, " ", ItemVec);
    std::string Account;
    for(int i = 0; i < ItemVec.size(); i++)
    {
        if(Utils::equalWith(ItemVec.at(i), "-a"))
        {
            Account = ItemVec.at(i + 1);
            break;
        }
    }
    strncpy(AppStatus.Account, Account.c_str(), sizeof(AppStatus.Account));

    std::vector<std::string> Vec;
    Utils::Split(ItemVec.at(0), "/", Vec);
    std::string AppName = Vec.at(Vec.size() - 1);
    strncpy(AppStatus.AppName, AppName.c_str(), sizeof(AppStatus.AppName));
    AppStatus.PID = getpid();
    strncpy(AppStatus.Status, "Start", sizeof(AppStatus.Status));

    char command[256] = {0};
    std::string AppLogPath;
    char* p = getenv("APP_LOG_PATH");
    if(p == NULL)
    {
        AppLogPath = "./log/";
    }
    else
    {
        AppLogPath = p;
    }
    fmt::format_to_n(AppStatus.StartScript, sizeof(AppStatus.StartScript), "nohup {} > {}/{}_{}_run.log 2>&1 &", 
                    cmd, AppLogPath, AppName, AppStatus.Account);
    strncpy(AppStatus.CommitID, APP_COMMITID, sizeof(AppStatus.CommitID));
    strncpy(AppStatus.UtilsCommitID, UTILS_COMMITID, sizeof(AppStatus.UtilsCommitID));
    strncpy(AppStatus.APIVersion, API_VERSION, sizeof(AppStatus.APIVersion));
    strncpy(AppStatus.StartTime, Utils::getCurrentTimeUs(), sizeof(AppStatus.StartTime));
    strncpy(AppStatus.LastStartTime, Utils::getCurrentTimeUs(), sizeof(AppStatus.LastStartTime));
    strncpy(AppStatus.UpdateTime, Utils::getCurrentTimeUs(), sizeof(AppStatus.UpdateTime));
}
