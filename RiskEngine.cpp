#include "RiskEngine.h"


Utils::LockFreeQueue<Message::PackMessage> RiskEngine::m_RiskResponseQueue(1 << 12);
std::unordered_map<RiskLimitKey, Message::TRiskReport, RiskLimitKeyHash> RiskEngine::m_RiskLimitMap;
std::unordered_map<VirtualPositionKey, Message::TRiskReport, VirtualPositionKeyHash> RiskEngine::m_StrategyPositionLimitMap;
std::unordered_map<RiskLimitKey, Message::TRiskReport, RiskLimitKeyHash> RiskEngine::m_AccountLockedMap;


RiskEngine::RiskEngine()
{
    m_RiskJudgeServer = NULL;
    m_HPPackClient = NULL;
    m_WorkThread = NULL;
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
        std::vector<Utils::TickerProperty> TickerPropertyList;
        std::string errorString;
        bool ok = Utils::LoadTickerList(m_XRiskJudgeConfig.TickerListPath.c_str(), TickerPropertyList, errorString);
        if(ok)
        {
            FMTLOG(fmtlog::INF, "RiskEngine LoadTickerList {} successed", m_XRiskJudgeConfig.TickerListPath);
            for (auto it = TickerPropertyList.begin(); it != TickerPropertyList.end(); it++)
            {
                m_TickerPropertyMap[it->Ticker] = *it;
            }
        }
        else
        {
            FMTLOG(fmtlog::WRN, "RiskEngine LoadTickerList {} failed, {}", m_XRiskJudgeConfig.TickerListPath, errorString);
        }
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
    // Select PositionLimitTable
    QueryPositionLimit();
    // Select AccountLockedTable
    QueryAccountLocked();
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

    FMTLOG(fmtlog::INF, "RiskEngine::Start {} Server RiskServerName:{}", m_XRiskJudgeConfig.RiskID, m_XRiskJudgeConfig.RiskServerName);
    m_RiskJudgeServer = new RiskJudgeServer();
    m_RiskJudgeServer->Start(m_XRiskJudgeConfig.RiskServerName, m_XRiskJudgeConfig.CPUSET.at(1));

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
    bool ret = Utils::ThreadBind(pthread_self(), m_XRiskJudgeConfig.CPUSET.at(0));
    FMTLOG(fmtlog::INF, "RiskEngine::WorkThreadFunc Risk Service {} Running CPU:{} ret:{}", m_XRiskJudgeConfig.RiskID, m_XRiskJudgeConfig.CPUSET.at(0), ret);

    Message::PackMessage message;
    memset(&message, 0, sizeof(message));
    message.MessageType = Message::EMessageType::EEventLog;
    message.EventLog.Level = Message::EEventLogLevel::EINFO;
    strncpy(message.EventLog.App, APP_NAME, sizeof(message.EventLog.App));
    fmt::format_to_n(message.EventLog.Event, sizeof(message.EventLog.Event), 
                    "Risk Service {} Start, RiskServerName:{}", 
                    m_XRiskJudgeConfig.RiskID, m_XRiskJudgeConfig.RiskServerName);
    strncpy(message.EventLog.UpdateTime, Utils::getCurrentTimeUs(), sizeof(message.EventLog.UpdateTime));
    HandleRequest(message);

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
        HandleAccountPosition(msg);
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
    FMTLOG(fmtlog::INF, "RiskEngine::HandleOrderStatus, Product:{} Account:{} Ticker:{} OrderRef:{} OrderStatus:{}",
            OrderStatus.Product, OrderStatus.Account, OrderStatus.Ticker, OrderStatus.OrderRef, OrderStatus.OrderStatus);
    // Add Pending Order
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
    }
    // Remove Pending Order when Order end
    else if(Message::EOrderStatusType::EALLTRADED == OrderStatus.OrderStatus ||
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
    // 更新策略虚拟持仓
    {
        if(OrderStatus.OrderStatus == Message::EOrderStatusType::EALLTRADED ||
            OrderStatus.OrderStatus == Message::EOrderStatusType::EPARTTRADED_CANCELLED) 
        {
            UpdateStrategyPosition(OrderStatus);
            // UpdateAccountPosition(OrderStatus);
        }
    }
    // 更新报单、撤单计数
    {
        UpdateTickerRiskLimit(OrderStatus);
        UpdateAccountRiskLimit(OrderStatus);
    }
}

void RiskEngine::HandleAccountPosition(const Message::PackMessage& msg)
{
    VirtualPositionKey key;
    key.Account = msg.AccountPosition.Account;   
    key.Ticker = msg.AccountPosition.Ticker; 
    key.EngineID = 0;
    
    auto it = m_StrategyPositionLimitMap.find(key);
    if (it == m_StrategyPositionLimitMap.end())
        return ; // 未配置限制，通过

    Message::TRiskReport& accountPos = it->second;
    if(msg.AccountPosition.BusinessType == Message::EBusinessType::EFUTURE)
    {
        accountPos.LongVolume = msg.AccountPosition.FuturePosition.LongTdVolume + msg.AccountPosition.FuturePosition.LongYdVolume;
        accountPos.ShortVolume = msg.AccountPosition.FuturePosition.ShortTdVolume + msg.AccountPosition.FuturePosition.ShortYdVolume;
    }
    else
    {
        accountPos.LongVolume = msg.AccountPosition.StockPosition.LongPosition;
        accountPos.ShortVolume = msg.AccountPosition.StockPosition.ShortPosition;
    }

    fmt::format_to_n(accountPos.RiskID, sizeof(accountPos.RiskID), "{}", m_XRiskJudgeConfig.RiskID);
    fmt::format_to_n(accountPos.UpdateTime, sizeof(accountPos.UpdateTime), "{}", Utils::getCurrentTimeUs());
    // Update SQLite PositionLimitTable
    std::string SQL;
    std::string errorString;
    SQL = fmt::format("UPDATE PositionLimitTable SET RiskID='{}', LongVolume='{}', ShortVolume='{}', UpdateTime='{}' WHERE Account='{}' AND Ticker='{}' AND EngineID='{}';",
                                accountPos.RiskID, accountPos.LongVolume, accountPos.ShortVolume, accountPos.UpdateTime,
                                accountPos.Account, accountPos.Ticker, accountPos.EngineID);
    m_RiskDBManager->UpdatePositionLimitTable(SQL, "UPDATE", &RiskEngine::sqlite3_callback_PositionLimit, errorString);

    FMTLOG(fmtlog::INF, "HandleAccountPosition Account:{} Ticker:{} EngineID:{} Long:{} Short:{} LongLimit:{} ShortLimit:{}",
           key.Account, key.Ticker, key.EngineID, accountPos.LongVolume, accountPos.ShortVolume, accountPos.LongLimit, accountPos.ShortLimit);

    Message::PackMessage report;
    report.MessageType = Message::EMessageType::ERiskReport;
    memcpy(&report.RiskReport, &accountPos, sizeof(report.RiskReport));
    m_RiskResponseQueue.Push(report);
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
        // 账户锁定检查
        if(ret && !AccountLocked(msg))
        {
            ret = false;
        }
        // 交易指令检查
        if(ret && !TransactionCommand(msg))
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
        // Strategy Position Limit  Check
        if (ret && !StrategyPositionLimitCheck(msg)) 
        {
            ret = false;
        }
        // Account Position Limit  Check
        if (ret && !AccountPositionLimitCheck(msg)) 
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

    RiskLimitKey key;
    key.Account = Account;
    key.Ticker = "";
    auto limitIt = m_RiskLimitMap.find(key);
    if(m_RiskLimitMap.end() != limitIt)
    {
        // last 1000 ms
        if(diff <= 1000)
        {
            m_AccountFlowLimitedMap[Account] += 1;
            if(m_AccountFlowLimitedMap[Account] > limitIt->second.FlowLimit)
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
            if(Message::EMessageType::EOrderRequest == msg.MessageType)
            {
                msg.OrderRequest.ErrorID =  Message::ERiskRejectedType::EFLOW_LIMITED;
                msg.OrderRequest.RiskStatus = Message::ERiskStatusType::ECHECKED_NOPASS;
                fmt::format_to_n(msg.OrderRequest.ErrorMsg, sizeof(msg.OrderRequest.ErrorMsg), 
                                "OrderRequest FlowLimited, ErrorID:{:#X} flow:{} limit:{} Product:{} Account:{} EngineID:{:#X}",
                                msg.OrderRequest.ErrorID, m_AccountFlowLimitedMap[Account], limitIt->second.FlowLimit,
                                msg.OrderRequest.Product, msg.OrderRequest.Account, msg.OrderRequest.EngineID);
                FMTLOG(fmtlog::WRN, "RiskEngine::FlowLimited Check failed, {}", msg.OrderRequest.ErrorMsg);
            }
            else if(Message::EMessageType::EActionRequest == msg.MessageType)
            {
                msg.ActionRequest.ErrorID =  Message::ERiskRejectedType::EFLOW_LIMITED;
                msg.ActionRequest.RiskStatus = Message::ERiskStatusType::ECHECKED_NOPASS;
                fmt::format_to_n(msg.ActionRequest.ErrorMsg, sizeof(msg.ActionRequest.ErrorMsg), 
                                "ActionRequest FlowLimited, ErrorID:{:#X} flow:{} limit:{} Account:{} EngineID:{:#X}",
                                msg.ActionRequest.ErrorID, m_AccountFlowLimitedMap[Account], limitIt->second.FlowLimit,
                                msg.ActionRequest.Account, msg.ActionRequest.EngineID);
                FMTLOG(fmtlog::WRN, "RiskEngine::FlowLimited Check failed, {}", msg.ActionRequest.ErrorMsg);
            }
        }
        else
        {
            if(limitIt->second.OrderCount + 1 > limitIt->second.OrderLimit)
            {
                ret = false;
            }
            if(!ret)
            {
                if(Message::EMessageType::EOrderRequest == msg.MessageType)
                {
                    msg.OrderRequest.ErrorID =  Message::ERiskRejectedType::EREQUEST_LIMITED;
                    msg.OrderRequest.RiskStatus = Message::ERiskStatusType::ECHECKED_NOPASS;
                    fmt::format_to_n(msg.OrderRequest.ErrorMsg, sizeof(msg.OrderRequest.ErrorMsg), 
                                    "OrderRequest FlowLimited, Account:{} Ticker:{} OrderCount:{} OrderLimit:{}",
                                    msg.OrderRequest.Account, msg.OrderRequest.Ticker, limitIt->second.OrderCount + 1, limitIt->second.OrderLimit);
                    FMTLOG(fmtlog::WRN, "RiskEngine::FlowLimited Check failed, {}", msg.OrderRequest.ErrorMsg);
                }
                else if(Message::EMessageType::EActionRequest == msg.MessageType)
                {
                    msg.ActionRequest.ErrorID =  Message::ERiskRejectedType::EREQUEST_LIMITED;
                    msg.ActionRequest.RiskStatus = Message::ERiskStatusType::ECHECKED_NOPASS;
                    fmt::format_to_n(msg.ActionRequest.ErrorMsg, sizeof(msg.ActionRequest.ErrorMsg), 
                                    "ActionRequest FlowLimited, Account:{} OrderRef:{} OrderCount:{} OrderLimit:{}",
                                    msg.ActionRequest.Account, msg.ActionRequest.OrderRef, limitIt->second.OrderCount + 1, limitIt->second.OrderLimit);
                    FMTLOG(fmtlog::WRN, "RiskEngine::FlowLimited Check failed, {}", msg.ActionRequest.ErrorMsg);
                }
            }
        }
    }
    else
    {
        ret = false;
        if(Message::EMessageType::EOrderRequest == msg.MessageType)
        {
            msg.OrderRequest.ErrorID =  Message::ERiskRejectedType::EACCOUNT_NOT_FOUND;
            msg.OrderRequest.RiskStatus = Message::ERiskStatusType::ECHECKED_NOPASS;
            fmt::format_to_n(msg.OrderRequest.ErrorMsg, sizeof(msg.OrderRequest.ErrorMsg), 
                            "OrderRequest FlowLimited, Account:{} not found FlowLimit",
                            msg.OrderRequest.Product, msg.OrderRequest.Account);
            FMTLOG(fmtlog::WRN, "RiskEngine::FlowLimited Check failed, {}", msg.OrderRequest.ErrorMsg);
        }
        else if(Message::EMessageType::EActionRequest == msg.MessageType)
        {
            msg.ActionRequest.ErrorID =  Message::ERiskRejectedType::EACCOUNT_NOT_FOUND;
            msg.ActionRequest.RiskStatus = Message::ERiskStatusType::ECHECKED_NOPASS;
            fmt::format_to_n(msg.ActionRequest.ErrorMsg, sizeof(msg.ActionRequest.ErrorMsg), 
                            "ActionRequest FlowLimited, Account:{} not found FlowLimit", msg.ActionRequest.Account);
            FMTLOG(fmtlog::WRN, "RiskEngine::FlowLimited Check failed, {}", msg.ActionRequest.ErrorMsg);
        }
    }
    return ret;
}

bool RiskEngine::AccountLocked(Message::PackMessage& msg)
{
    bool ret = true;
    if(Message::EMessageType::EOrderRequest == msg.MessageType)
    {
        RiskLimitKey key;
        key.Account = msg.OrderRequest.Account;
        key.Ticker = "";
        auto accountIt = m_AccountLockedMap.find(key);
        if(accountIt != m_AccountLockedMap.end())
        {
            if(accountIt->second.LockSide == Message::EAccountLockSide::ELOCK_ACCOUNT)
            {
                ret = false;
                msg.OrderRequest.ErrorID = Message::ERiskRejectedType::EACCOUNT_LOCKED_ALL;
                msg.OrderRequest.RiskStatus = Message::ERiskStatusType::ECHECKED_NOPASS;
                fmt::format_to_n(msg.OrderRequest.ErrorMsg, sizeof(msg.OrderRequest.ErrorMsg),
                                "AccountLocked, Account:{} ban BUY/SELL", msg.OrderRequest.Account);
                FMTLOG(fmtlog::WRN, "RiskEngine::AccountLocked Check failed, {}", msg.OrderRequest.ErrorMsg);
                return ret;
            }
            else if(msg.OrderRequest.Offset == Message::EOrderOffset::EOPEN && accountIt->second.LockSide == Message::EAccountLockSide::ELOCK_OPEN)
            {
                ret = false;
                msg.OrderRequest.ErrorID = Message::ERiskRejectedType::EACCOUNT_LOCKED_OPEN;
                msg.OrderRequest.RiskStatus = Message::ERiskStatusType::ECHECKED_NOPASS;
                fmt::format_to_n(msg.OrderRequest.ErrorMsg, sizeof(msg.OrderRequest.ErrorMsg),
                                "AccountLocked, Account:{} ban OPEN", msg.OrderRequest.Account);
                FMTLOG(fmtlog::WRN, "RiskEngine::AccountLocked Check failed, {}", msg.OrderRequest.ErrorMsg);
                return ret;
            }
            else if(msg.OrderRequest.Offset != Message::EOrderOffset::EOPEN && accountIt->second.LockSide == Message::EAccountLockSide::ELOCK_CLOSE)
            {
                ret = false;
                msg.OrderRequest.ErrorID = Message::ERiskRejectedType::EACCOUNT_LOCKED_CLOSE;
                msg.OrderRequest.RiskStatus = Message::ERiskStatusType::ECHECKED_NOPASS;
                fmt::format_to_n(msg.OrderRequest.ErrorMsg, sizeof(msg.OrderRequest.ErrorMsg),
                                "AccountLocked, Account:{} ban CLOSE", msg.OrderRequest.Account);
                FMTLOG(fmtlog::WRN, "RiskEngine::AccountLocked Check failed, {}", msg.OrderRequest.ErrorMsg);
                return ret;
            }
            else if(msg.OrderRequest.Direction == Message::EOrderDirection::EBUY)
            {
                if(accountIt->second.LockSide == Message::EAccountLockSide::ELOCK_BUY)
                {
                    ret = false;
                    msg.OrderRequest.ErrorID = Message::ERiskRejectedType::EACCOUNT_LOCKED_BUY;
                    msg.OrderRequest.RiskStatus = Message::ERiskStatusType::ECHECKED_NOPASS;
                    fmt::format_to_n(msg.OrderRequest.ErrorMsg, sizeof(msg.OrderRequest.ErrorMsg),
                                    "AccountLocked, Account:{} ban BUY", msg.OrderRequest.Account);
                    FMTLOG(fmtlog::WRN, "RiskEngine::AccountLocked Check failed, {}", msg.OrderRequest.ErrorMsg);
                    return ret;
                }
                else if(msg.OrderRequest.Offset == Message::EOrderOffset::EOPEN && accountIt->second.LockSide == Message::EAccountLockSide::ELOCK_BUY_OPEN)
                {
                    ret = false;
                    msg.OrderRequest.ErrorID = Message::ERiskRejectedType::EACCOUNT_LOCKED_BUY_OPEN;
                    msg.OrderRequest.RiskStatus = Message::ERiskStatusType::ECHECKED_NOPASS;
                    fmt::format_to_n(msg.OrderRequest.ErrorMsg, sizeof(msg.OrderRequest.ErrorMsg),
                                    "AccountLocked, Account:{} ban BUY OPEN", msg.OrderRequest.Account);
                    FMTLOG(fmtlog::WRN, "RiskEngine::AccountLocked Check failed, {}", msg.OrderRequest.ErrorMsg);
                    return ret;
                }
                else if(msg.OrderRequest.Offset == Message::EOrderOffset::ECLOSE && accountIt->second.LockSide == Message::EAccountLockSide::ELOCK_BUY_CLOSE)
                {
                    ret = false;
                    msg.OrderRequest.ErrorID = Message::ERiskRejectedType::EACCOUNT_LOCKED_BUY_CLOSE;
                    msg.OrderRequest.RiskStatus = Message::ERiskStatusType::ECHECKED_NOPASS;
                    fmt::format_to_n(msg.OrderRequest.ErrorMsg, sizeof(msg.OrderRequest.ErrorMsg),
                                    "AccountLocked, Account:{} ban BUY CLOSE", msg.OrderRequest.Account);
                    FMTLOG(fmtlog::WRN, "RiskEngine::AccountLocked Check failed, {}", msg.OrderRequest.ErrorMsg);
                    return ret;
                }
                else if(msg.OrderRequest.Offset == Message::EOrderOffset::ECLOSE_TODAY && accountIt->second.LockSide == Message::EAccountLockSide::ELOCK_BUY_CLOSE_TODAY)
                {
                    ret = false;
                    msg.OrderRequest.ErrorID = Message::ERiskRejectedType::EACCOUNT_LOCKED_BUY_CLOSE_TODAY;
                    msg.OrderRequest.RiskStatus = Message::ERiskStatusType::ECHECKED_NOPASS;
                    fmt::format_to_n(msg.OrderRequest.ErrorMsg, sizeof(msg.OrderRequest.ErrorMsg),
                                    "AccountLocked, Account:{} ban BUY CLOSE TODAY", msg.OrderRequest.Account);
                    FMTLOG(fmtlog::WRN, "RiskEngine::AccountLocked Check failed, {}", msg.OrderRequest.ErrorMsg);
                    return ret;
                }
                else if(msg.OrderRequest.Offset == Message::EOrderOffset::ECLOSE_YESTODAY && accountIt->second.LockSide == Message::EAccountLockSide::ELOCK_BUY_CLOSE_YESTODAY)
                {
                    ret = false;
                    msg.OrderRequest.ErrorID = Message::ERiskRejectedType::EACCOUNT_LOCKED_BUY_CLOSE_YESTODAY;
                    msg.OrderRequest.RiskStatus = Message::ERiskStatusType::ECHECKED_NOPASS;
                    fmt::format_to_n(msg.OrderRequest.ErrorMsg, sizeof(msg.OrderRequest.ErrorMsg),
                                    "AccountLocked, Account:{} ban BUY CLOSE YESTODAY", msg.OrderRequest.Account);
                    FMTLOG(fmtlog::WRN, "RiskEngine::AccountLocked Check failed, {}", msg.OrderRequest.ErrorMsg);
                    return ret;
                }
            }
            else if(msg.OrderRequest.Direction == Message::EOrderDirection::ESELL)
            {
                if(accountIt->second.LockSide == Message::EAccountLockSide::ELOCK_SELL)
                {
                    ret = false;
                    msg.OrderRequest.ErrorID = Message::ERiskRejectedType::EACCOUNT_LOCKED_SELL;
                    msg.OrderRequest.RiskStatus = Message::ERiskStatusType::ECHECKED_NOPASS;
                    fmt::format_to_n(msg.OrderRequest.ErrorMsg, sizeof(msg.OrderRequest.ErrorMsg),
                                    "AccountLocked, Account:{} ban SELL", msg.OrderRequest.Account);
                    FMTLOG(fmtlog::WRN, "RiskEngine::AccountLocked Check failed, {}", msg.OrderRequest.ErrorMsg);
                    return ret;
                }
                else if(msg.OrderRequest.Offset == Message::EOrderOffset::EOPEN && accountIt->second.LockSide == Message::EAccountLockSide::ELOCK_SELL_OPEN)
                {
                    ret = false;
                    msg.OrderRequest.ErrorID = Message::ERiskRejectedType::EACCOUNT_LOCKED_SELL_OPEN;
                    msg.OrderRequest.RiskStatus = Message::ERiskStatusType::ECHECKED_NOPASS;
                    fmt::format_to_n(msg.OrderRequest.ErrorMsg, sizeof(msg.OrderRequest.ErrorMsg),
                                    "AccountLocked, Account:{} ban SELL OPEN", msg.OrderRequest.Account);
                    FMTLOG(fmtlog::WRN, "RiskEngine::AccountLocked Check failed, {}", msg.OrderRequest.ErrorMsg);
                    return ret;
                }
                else if(msg.OrderRequest.Offset == Message::EOrderOffset::ECLOSE && accountIt->second.LockSide == Message::EAccountLockSide::ELOCK_SELL_CLOSE)
                {
                    ret = false;
                    msg.OrderRequest.ErrorID = Message::ERiskRejectedType::EACCOUNT_LOCKED_SELL_CLOSE;
                    msg.OrderRequest.RiskStatus = Message::ERiskStatusType::ECHECKED_NOPASS;
                    fmt::format_to_n(msg.OrderRequest.ErrorMsg, sizeof(msg.OrderRequest.ErrorMsg),
                                    "AccountLocked, Account:{} ban SELL CLOSE", msg.OrderRequest.Account);
                    FMTLOG(fmtlog::WRN, "RiskEngine::AccountLocked Check failed, {}", msg.OrderRequest.ErrorMsg);
                    return ret;
                }
                else if(msg.OrderRequest.Offset == Message::EOrderOffset::ECLOSE_TODAY && accountIt->second.LockSide == Message::EAccountLockSide::ELOCK_SELL_CLOSE_TODAY)
                {
                    ret = false;
                    msg.OrderRequest.ErrorID = Message::ERiskRejectedType::EACCOUNT_LOCKED_SELL_CLOSE_TODAY;
                    msg.OrderRequest.RiskStatus = Message::ERiskStatusType::ECHECKED_NOPASS;
                    fmt::format_to_n(msg.OrderRequest.ErrorMsg, sizeof(msg.OrderRequest.ErrorMsg),
                                    "AccountLocked, Account:{} ban SELL CLOSE TODAY", msg.OrderRequest.Account);
                    FMTLOG(fmtlog::WRN, "RiskEngine::AccountLocked Check failed, {}", msg.OrderRequest.ErrorMsg);
                    return ret;
                }
                else if(msg.OrderRequest.Offset == Message::EOrderOffset::ECLOSE_YESTODAY && accountIt->second.LockSide == Message::EAccountLockSide::ELOCK_SELL_CLOSE_YESTODAY)
                {
                    ret = false;
                    msg.OrderRequest.ErrorID = Message::ERiskRejectedType::EACCOUNT_LOCKED_SELL_CLOSE_YESTODAY;
                    msg.OrderRequest.RiskStatus = Message::ERiskStatusType::ECHECKED_NOPASS;
                    fmt::format_to_n(msg.OrderRequest.ErrorMsg, sizeof(msg.OrderRequest.ErrorMsg),
                                    "AccountLocked, Account:{} ban SELL CLOSE YESTODAY", msg.OrderRequest.Account);
                    FMTLOG(fmtlog::WRN, "RiskEngine::AccountLocked Check failed, {}", msg.OrderRequest.ErrorMsg);
                    return ret;
                }
            }
        }

        key.Account = msg.OrderRequest.Account;
        key.Ticker = msg.OrderRequest.Ticker;
        auto tickerIt = m_AccountLockedMap.find(key);
        if(tickerIt != m_AccountLockedMap.end())
        {
            if(tickerIt->second.LockSide == Message::EAccountLockSide::EUNLOCK)
            {
                ret = true;
                return ret;
            }
            else if(tickerIt->second.LockSide == Message::EAccountLockSide::ELOCK_ACCOUNT)
            {
                ret = false;
                msg.OrderRequest.ErrorID = Message::ERiskRejectedType::ETICKER_LOCKED_ALL;
                msg.OrderRequest.RiskStatus = Message::ERiskStatusType::ECHECKED_NOPASS;
                fmt::format_to_n(msg.OrderRequest.ErrorMsg, sizeof(msg.OrderRequest.ErrorMsg),
                                "AccountLocked, Account:{} Ticker:{} ban BUY/SELL", msg.OrderRequest.Account, msg.OrderRequest.Ticker);
                FMTLOG(fmtlog::WRN, "RiskEngine::AccountLocked Check failed, {}", msg.OrderRequest.ErrorMsg);
                return ret;
            }
            else if(msg.OrderRequest.Offset == Message::EOrderOffset::EOPEN && tickerIt->second.LockSide == Message::EAccountLockSide::ELOCK_OPEN)
            {
                ret = false;
                msg.OrderRequest.ErrorID = Message::ERiskRejectedType::ETICKER_LOCKED_OPEN;
                msg.OrderRequest.RiskStatus = Message::ERiskStatusType::ECHECKED_NOPASS;
                fmt::format_to_n(msg.OrderRequest.ErrorMsg, sizeof(msg.OrderRequest.ErrorMsg),
                                "AccountLocked, Account:{} Ticker:{} ban OPEN", msg.OrderRequest.Account, msg.OrderRequest.Ticker);
                FMTLOG(fmtlog::WRN, "RiskEngine::AccountLocked Check failed, {}", msg.OrderRequest.ErrorMsg);
                return ret;
            }
            else if(msg.OrderRequest.Offset != Message::EOrderOffset::EOPEN && tickerIt->second.LockSide == Message::EAccountLockSide::ELOCK_CLOSE)
            {
                ret = false;
                msg.OrderRequest.ErrorID = Message::ERiskRejectedType::ETICKER_LOCKED_CLOSE;
                msg.OrderRequest.RiskStatus = Message::ERiskStatusType::ECHECKED_NOPASS;
                fmt::format_to_n(msg.OrderRequest.ErrorMsg, sizeof(msg.OrderRequest.ErrorMsg),
                                "AccountLocked, Account:{} Ticker:{} ban CLOSE", msg.OrderRequest.Account, msg.OrderRequest.Ticker);
                FMTLOG(fmtlog::WRN, "RiskEngine::AccountLocked Check failed, {}", msg.OrderRequest.ErrorMsg);
                return ret;
            }
            else if(msg.OrderRequest.Direction == Message::EOrderDirection::EBUY)
            {
                if(tickerIt->second.LockSide == Message::EAccountLockSide::ELOCK_BUY)
                {
                    ret = false;
                    msg.OrderRequest.ErrorID = Message::ERiskRejectedType::ETICKER_LOCKED_BUY;
                    msg.OrderRequest.RiskStatus = Message::ERiskStatusType::ECHECKED_NOPASS;
                    fmt::format_to_n(msg.OrderRequest.ErrorMsg, sizeof(msg.OrderRequest.ErrorMsg),
                                    "AccountLocked, Account:{} Ticker:{} ban BUY", msg.OrderRequest.Account, msg.OrderRequest.Ticker);
                    FMTLOG(fmtlog::WRN, "RiskEngine::AccountLocked Check failed, {}", msg.OrderRequest.ErrorMsg);
                    return ret;
                }
                else if(msg.OrderRequest.Offset == Message::EOrderOffset::EOPEN && tickerIt->second.LockSide == Message::EAccountLockSide::ELOCK_BUY_OPEN)
                {
                    ret = false;
                    msg.OrderRequest.ErrorID = Message::ERiskRejectedType::ETICKER_LOCKED_BUY_OPEN;
                    msg.OrderRequest.RiskStatus = Message::ERiskStatusType::ECHECKED_NOPASS;
                    fmt::format_to_n(msg.OrderRequest.ErrorMsg, sizeof(msg.OrderRequest.ErrorMsg),
                                    "AccountLocked, Account:{} Ticker:{} ban BUY OPEN", msg.OrderRequest.Account, msg.OrderRequest.Ticker);
                    FMTLOG(fmtlog::WRN, "RiskEngine::AccountLocked Check failed, {}", msg.OrderRequest.ErrorMsg);
                    return ret;
                }
                else if(msg.OrderRequest.Offset == Message::EOrderOffset::ECLOSE && tickerIt->second.LockSide == Message::EAccountLockSide::ELOCK_BUY_CLOSE)
                {
                    ret = false;
                    msg.OrderRequest.ErrorID = Message::ERiskRejectedType::ETICKER_LOCKED_BUY_CLOSE;
                    msg.OrderRequest.RiskStatus = Message::ERiskStatusType::ECHECKED_NOPASS;
                    fmt::format_to_n(msg.OrderRequest.ErrorMsg, sizeof(msg.OrderRequest.ErrorMsg),
                                    "AccountLocked, Account:{} Ticker:{} ban BUY CLOSE", msg.OrderRequest.Account, msg.OrderRequest.Ticker);
                    FMTLOG(fmtlog::WRN, "RiskEngine::AccountLocked Check failed, {}", msg.OrderRequest.ErrorMsg);
                    return ret;
                }
                else if(msg.OrderRequest.Offset == Message::EOrderOffset::ECLOSE_TODAY && tickerIt->second.LockSide == Message::EAccountLockSide::ELOCK_BUY_CLOSE_TODAY)
                {
                    ret = false;
                    msg.OrderRequest.ErrorID = Message::ERiskRejectedType::ETICKER_LOCKED_BUY_CLOSE_TODAY;
                    msg.OrderRequest.RiskStatus = Message::ERiskStatusType::ECHECKED_NOPASS;
                    fmt::format_to_n(msg.OrderRequest.ErrorMsg, sizeof(msg.OrderRequest.ErrorMsg),
                                    "AccountLocked, Account:{} Ticker:{} ban BUY CLOSE TODAY", msg.OrderRequest.Account, msg.OrderRequest.Ticker);
                    FMTLOG(fmtlog::WRN, "RiskEngine::AccountLocked Check failed, {}", msg.OrderRequest.ErrorMsg);
                    return ret;
                }
                else if(msg.OrderRequest.Offset == Message::EOrderOffset::ECLOSE_YESTODAY && tickerIt->second.LockSide == Message::EAccountLockSide::ELOCK_BUY_CLOSE_YESTODAY)
                {
                    ret = false;
                    msg.OrderRequest.ErrorID = Message::ERiskRejectedType::ETICKER_LOCKED_BUY_CLOSE_YESTODAY;
                    msg.OrderRequest.RiskStatus = Message::ERiskStatusType::ECHECKED_NOPASS;
                    fmt::format_to_n(msg.OrderRequest.ErrorMsg, sizeof(msg.OrderRequest.ErrorMsg),
                                    "AccountLocked, Account:{} Ticker:{} ban BUY CLOSE YESTODAY", msg.OrderRequest.Account, msg.OrderRequest.Ticker);
                    FMTLOG(fmtlog::WRN, "RiskEngine::AccountLocked Check failed, {}", msg.OrderRequest.ErrorMsg);
                    return ret;
                }
            }
            else if(msg.OrderRequest.Direction == Message::EOrderDirection::ESELL)
            {
                if(tickerIt->second.LockSide == Message::EAccountLockSide::ELOCK_SELL)
                {
                    ret = false;
                    msg.OrderRequest.ErrorID = Message::ERiskRejectedType::ETICKER_LOCKED_SELL;
                    msg.OrderRequest.RiskStatus = Message::ERiskStatusType::ECHECKED_NOPASS;
                    fmt::format_to_n(msg.OrderRequest.ErrorMsg, sizeof(msg.OrderRequest.ErrorMsg),
                                    "AccountLocked, Account:{} Ticker:{} ban SELL", msg.OrderRequest.Account, msg.OrderRequest.Ticker);
                    FMTLOG(fmtlog::WRN, "RiskEngine::AccountLocked Check failed, {}", msg.OrderRequest.ErrorMsg);
                    return ret;
                }
                else if(msg.OrderRequest.Offset == Message::EOrderOffset::EOPEN && tickerIt->second.LockSide == Message::EAccountLockSide::ELOCK_SELL_OPEN)
                {
                    ret = false;
                    msg.OrderRequest.ErrorID = Message::ERiskRejectedType::ETICKER_LOCKED_SELL_OPEN;
                    msg.OrderRequest.RiskStatus = Message::ERiskStatusType::ECHECKED_NOPASS;
                    fmt::format_to_n(msg.OrderRequest.ErrorMsg, sizeof(msg.OrderRequest.ErrorMsg),
                                    "AccountLocked, Account:{} Ticker:{} ban SELL OPEN", msg.OrderRequest.Account, msg.OrderRequest.Ticker);
                    FMTLOG(fmtlog::WRN, "RiskEngine::AccountLocked Check failed, {}", msg.OrderRequest.ErrorMsg);
                    return ret;
                }
                else if(msg.OrderRequest.Offset == Message::EOrderOffset::ECLOSE && tickerIt->second.LockSide == Message::EAccountLockSide::ELOCK_SELL_CLOSE)
                {
                    ret = false;
                    msg.OrderRequest.ErrorID = Message::ERiskRejectedType::ETICKER_LOCKED_SELL_CLOSE;
                    msg.OrderRequest.RiskStatus = Message::ERiskStatusType::ECHECKED_NOPASS;
                    fmt::format_to_n(msg.OrderRequest.ErrorMsg, sizeof(msg.OrderRequest.ErrorMsg),
                                    "AccountLocked, Account:{} Ticker:{} ban SELL CLOSE", msg.OrderRequest.Account, msg.OrderRequest.Ticker);
                    FMTLOG(fmtlog::WRN, "RiskEngine::AccountLocked Check failed, {}", msg.OrderRequest.ErrorMsg);
                    return ret;
                }
                else if(msg.OrderRequest.Offset == Message::EOrderOffset::ECLOSE_TODAY && tickerIt->second.LockSide == Message::EAccountLockSide::ELOCK_SELL_CLOSE_TODAY)
                {
                    ret = false;
                    msg.OrderRequest.ErrorID = Message::ERiskRejectedType::ETICKER_LOCKED_SELL_CLOSE_TODAY;
                    msg.OrderRequest.RiskStatus = Message::ERiskStatusType::ECHECKED_NOPASS;
                    fmt::format_to_n(msg.OrderRequest.ErrorMsg, sizeof(msg.OrderRequest.ErrorMsg),
                                    "AccountLocked, Account:{} Ticker:{} ban SELL CLOSE TODAY", msg.OrderRequest.Account, msg.OrderRequest.Ticker);
                    FMTLOG(fmtlog::WRN, "RiskEngine::AccountLocked Check failed, {}", msg.OrderRequest.ErrorMsg);
                    return ret;
                }
                else if(msg.OrderRequest.Offset == Message::EOrderOffset::ECLOSE_YESTODAY && tickerIt->second.LockSide == Message::EAccountLockSide::ELOCK_SELL_CLOSE_YESTODAY)
                {
                    ret = false;
                    msg.OrderRequest.ErrorID = Message::ERiskRejectedType::ETICKER_LOCKED_SELL_CLOSE_YESTODAY;
                    msg.OrderRequest.RiskStatus = Message::ERiskStatusType::ECHECKED_NOPASS;
                    fmt::format_to_n(msg.OrderRequest.ErrorMsg, sizeof(msg.OrderRequest.ErrorMsg),
                                    "AccountLocked, Account:{} Ticker:{} ban SELL CLOSE YESTODAY", msg.OrderRequest.Account, msg.OrderRequest.Ticker);
                    FMTLOG(fmtlog::WRN, "RiskEngine::AccountLocked Check failed, {}", msg.OrderRequest.ErrorMsg);
                    return ret;
                }
            }
        }
    }
    return ret;
}

bool RiskEngine::TransactionCommand(Message::PackMessage& msg)
{
    bool ret = true;
    if(Message::EMessageType::EOrderRequest == msg.MessageType)                 
    {
        auto tickerIt = m_TickerPropertyMap.find(msg.OrderRequest.Ticker);
        if(tickerIt != m_TickerPropertyMap.end())
        {
            if(msg.OrderRequest.Volume > tickerIt->second.MaxVolume)
            {
                msg.OrderRequest.ErrorID = Message::ERiskRejectedType::EVOLUME_EXCEEDED;
                msg.OrderRequest.RiskStatus = Message::ERiskStatusType::ECHECKED_NOPASS;
                fmt::format_to_n(msg.OrderRequest.ErrorMsg, sizeof(msg.OrderRequest.ErrorMsg),
                                "TransactionCommand, Account:{} Ticker:{} Volume:{} exceeded MaxVolume:{}",
                                msg.OrderRequest.Account, msg.OrderRequest.Ticker, msg.OrderRequest.Volume, tickerIt->second.MaxVolume);
                FMTLOG(fmtlog::WRN, "RiskEngine::TransactionCommand Check failed, {}", msg.OrderRequest.ErrorMsg);
                ret = false;
            }
            else if(isPriceInvalid(msg.OrderRequest.Price, tickerIt->second.PriceTick)) 
            {
                msg.OrderRequest.ErrorID = Message::ERiskRejectedType::EINVALID_PRICE;
                msg.OrderRequest.RiskStatus = Message::ERiskStatusType::ECHECKED_NOPASS;
                fmt::format_to_n(msg.OrderRequest.ErrorMsg, sizeof(msg.OrderRequest.ErrorMsg),
                                "TransactionCommand, Account:{} Ticker:{} Price:{} invalid PriceTick:{}",
                                msg.OrderRequest.Account, msg.OrderRequest.Ticker, msg.OrderRequest.Price, tickerIt->second.PriceTick);
                FMTLOG(fmtlog::WRN, "RiskEngine::TransactionCommand Check failed, {}", msg.OrderRequest.ErrorMsg);
                ret = false;
            }
        }
        else
        {
            msg.OrderRequest.ErrorID = Message::ERiskRejectedType::ETICKER_NOT_FOUND;
            msg.OrderRequest.RiskStatus = Message::ERiskStatusType::ECHECKED_NOPASS;
            fmt::format_to_n(msg.OrderRequest.ErrorMsg, sizeof(msg.OrderRequest.ErrorMsg),
                            "TransactionCommand, Account:{} Ticker:{} not found",
                            msg.OrderRequest.Account, msg.OrderRequest.Ticker);
            FMTLOG(fmtlog::WRN, "RiskEngine::TransactionCommand Check failed, {}", msg.OrderRequest.ErrorMsg);
            ret = false;
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
            RiskLimitKey key;
            key.Account = it->second.Account;
            key.Ticker = it->second.Ticker;
            int CancelRequestCount = 0;
            auto tickerIt = m_RiskLimitMap.find(key);
            if(tickerIt != m_RiskLimitMap.end())
            {
                CancelRequestCount = tickerIt->second.CancelCount;
                CancelRequestCount += 1;
                if(CancelRequestCount > tickerIt->second.CancelLimit)
                {
                    msg.ActionRequest.ErrorID = Message::ERiskRejectedType::ETICKER_ACTION_LIMITED;
                    msg.ActionRequest.RiskStatus = Message::ERiskStatusType::ECHECKED_NOPASS;
                    ret = false;
                    fmt::format_to_n(msg.ActionRequest.ErrorMsg, sizeof(msg.ActionRequest.ErrorMsg), 
                                    "CancelLimited, ErrorID:{:#X} Product:{} Account:{} Ticker:{} OrderRef:{}, CancelRequestCount:{} Ticker Cancel Limit:{}",
                                    msg.ActionRequest.ErrorID, it->second.Product, it->second.Account, tickerIt->second.Ticker, it->second.OrderRef,
                                    CancelRequestCount, tickerIt->second.CancelLimit);
                    FMTLOG(fmtlog::WRN, "RiskEngine::CancelLimited Check failed, {}", msg.ActionRequest.ErrorMsg);
                }
                // Order Cancelled Limit
                if(ret)
                {
                    int& CancelRequestCount = m_OrderCancelledCounterMap[OrderRef];
                    CancelRequestCount += 1;
                    if(CancelRequestCount > tickerIt->second.OrderCancelLimit)
                    {
                        msg.ActionRequest.ErrorID = Message::ERiskRejectedType::EORDER_ACTION_LIMITED;
                        msg.ActionRequest.RiskStatus = Message::ERiskStatusType::ECHECKED_NOPASS;
                        ret = false;
                        fmt::format_to_n(msg.ActionRequest.ErrorMsg, sizeof(msg.ActionRequest.ErrorMsg), 
                                        "CancelLimited, ErrorID:{:#X} Product:{} Account:{} Ticker:{} OrderRef:{}, CancelRequestCount:{} Order Cancel Limit:{}",
                                        msg.ActionRequest.ErrorID, it->second.Product, it->second.Account, tickerIt->second.Ticker, 
                                        it->second.OrderRef, CancelRequestCount, tickerIt->second.OrderCancelLimit);
                        FMTLOG(fmtlog::WRN, "RiskEngine::CancelLimited Check failed, {}", msg.ActionRequest.ErrorMsg);
                    }
                }
            }
        }
    }
    return ret;
}

bool RiskEngine::StrategyPositionLimitCheck(Message::PackMessage& msg) 
{
    if(msg.MessageType != Message::EMessageType::EOrderRequest)
        return true; // 撤单不检查持仓
    if(msg.OrderRequest.Offset != Message::EOrderOffset::EOPEN)
        return true; // 撤单不检查持仓

    VirtualPositionKey key;
    key.Account =  msg.OrderRequest.Account;   
    key.Ticker =  msg.OrderRequest.Ticker;
    key.EngineID = msg.OrderRequest.EngineID;
    bool ret = true;
    auto positionIt = m_StrategyPositionLimitMap.find(key);
    if(positionIt != m_StrategyPositionLimitMap.end()) 
    {
        int volume = msg.OrderRequest.Volume;
        int direction = msg.OrderRequest.Direction;
        int longVolume = positionIt->second.LongVolume;
        int shortVolume = positionIt->second.ShortVolume;
        
        if(direction == Message::EOrderDirection::EBUY) 
        {
            if(longVolume + volume > positionIt->second.LongLimit) 
            {
                ret = false;
                msg.OrderRequest.ErrorID = Message::ERiskRejectedType::ETICKER_LONG_LIMIT; 
                msg.OrderRequest.RiskStatus = Message::ERiskStatusType::ECHECKED_NOPASS;
                fmt::format_to_n(msg.OrderRequest.ErrorMsg, sizeof(msg.OrderRequest.ErrorMsg),
                                "StrategyPositionLimit exceeded, Account:{} Ticker:{} EngineID:{} Long:{} Volume:{} LongLimit:{}",
                                key.Account,key.Ticker,  key.EngineID, longVolume, volume, positionIt->second.LongLimit);
                FMTLOG(fmtlog::WRN, "StrategyPositionLimitCheck failed, {}", msg.OrderRequest.ErrorMsg);
            }
                
        } 
        else 
        { 
            if(shortVolume + volume > positionIt->second.ShortLimit) 
            {
                ret = false;
                msg.OrderRequest.ErrorID = Message::ERiskRejectedType::ETICKER_SHORT_LIMIT; 
                msg.OrderRequest.RiskStatus = Message::ERiskStatusType::ECHECKED_NOPASS;
                fmt::format_to_n(msg.OrderRequest.ErrorMsg, sizeof(msg.OrderRequest.ErrorMsg),
                                "StrategyPositionLimit exceeded, Account:{} Ticker:{} EngineID:{} Short:{} Volume:{} ShortLimit:{}",
                                key.Account, key.Ticker, key.EngineID, shortVolume, volume, positionIt->second.ShortLimit);
                FMTLOG(fmtlog::WRN, "StrategyPositionLimitCheck failed, {}", msg.OrderRequest.ErrorMsg);
            }
                
        }
    }
    else
    {
        ret = false;
        msg.OrderRequest.ErrorID = Message::ERiskRejectedType::ETICKER_NOT_FOUND; 
        msg.OrderRequest.RiskStatus = Message::ERiskStatusType::ECHECKED_NOPASS;
        fmt::format_to_n(msg.OrderRequest.ErrorMsg, sizeof(msg.OrderRequest.ErrorMsg),
                        "StrategyPositionLimit Account:{} Ticker:{} EngineID:{} not found LongLimit/ShortLimit",
                        key.Account, key.Ticker, key.EngineID);
        FMTLOG(fmtlog::WRN, "StrategyPositionLimitCheck failed, {}", msg.OrderRequest.ErrorMsg);
    }
    return ret;
}


bool RiskEngine::AccountPositionLimitCheck(Message::PackMessage& msg)
{
    if(msg.MessageType != Message::EMessageType::EOrderRequest)
        return true; // 仅对报单请求检查

    VirtualPositionKey key;
    key.Account = msg.OrderRequest.Account;
    key.Ticker = msg.OrderRequest.Ticker;
    key.EngineID = 0;      // 账户维度，不区分策略

    auto it = m_StrategyPositionLimitMap.find(key);
    if (it != m_StrategyPositionLimitMap.end())
    {
        const auto& limit = it->second;
        int volume = msg.OrderRequest.Volume;
        int direction = msg.OrderRequest.Direction;
        int offset = msg.OrderRequest.Offset;
        int longVol = limit.LongVolume;
        int shortVol = limit.ShortVolume;

        int newLong = longVol;
        int newShort = shortVol;

        // 1 计算新持仓并校验平仓是否超量
        if(direction == Message::EOrderDirection::EBUY)
        {
            if(offset == Message::EOrderOffset::EOPEN)
            {
                newLong += volume;
            }
            else 
            {
                // 买入平空头
                if(volume > shortVol)
                {
                    msg.OrderRequest.ErrorID = Message::ERiskRejectedType::ETICKER_SHORT_INSUFFICIENT;
                    msg.OrderRequest.RiskStatus = Message::ERiskStatusType::ECHECKED_NOPASS;
                    fmt::format_to_n(msg.OrderRequest.ErrorMsg, sizeof(msg.OrderRequest.ErrorMsg),
                                    "short position insufficient, Account:{} Ticker:{} ShortPos:{} CloseVol:{}",
                                    key.Account, key.Ticker, shortVol, volume);
                    FMTLOG(fmtlog::WRN, "AccountPositionLimitCheck failed, {}", msg.OrderRequest.ErrorMsg);
                    return false;
                }
                newShort -= volume;
            }
        }
        else if(direction == Message::EOrderDirection::ESELL)
        {
            if(offset == Message::EOrderOffset::EOPEN)
            {
                newShort += volume;
            }
            else 
            {
                // 卖出平多头
                if(volume > longVol)
                {
                    msg.OrderRequest.ErrorID = Message::ERiskRejectedType::ETICKER_LONG_INSUFFICIENT;
                    msg.OrderRequest.RiskStatus = Message::ERiskStatusType::ECHECKED_NOPASS;
                    fmt::format_to_n(msg.OrderRequest.ErrorMsg, sizeof(msg.OrderRequest.ErrorMsg),
                                    "long position insufficient, Account:{} Ticker:{} LongPos:{} CloseVol:{}",
                                    key.Account, key.Ticker, longVol, volume);
                    FMTLOG(fmtlog::WRN, "AccountPositionLimitCheck failed, {}", msg.OrderRequest.ErrorMsg);
                    return false;
                }
                newLong -= volume;
            }
        }
        else if(direction == Message::EOrderDirection::EMARGIN_BUY)
        {
            newLong += volume;
        }
        else if(direction == Message::EOrderDirection::EREPAY_MARGIN_BY_SELL)
        {
            // 卖券还钱平多头
            if(volume > longVol)
            {
                msg.OrderRequest.ErrorID = Message::ERiskRejectedType::ETICKER_LONG_INSUFFICIENT;
                msg.OrderRequest.RiskStatus = Message::ERiskStatusType::ECHECKED_NOPASS;
                fmt::format_to_n(msg.OrderRequest.ErrorMsg, sizeof(msg.OrderRequest.ErrorMsg),
                                    "Margin Sell Close long exceeds position, Account:{} Ticker:{} LongPos:{} CloseVol:{}",
                                    key.Account, key.Ticker, longVol, volume);
                FMTLOG(fmtlog::WRN, "AccountPositionLimitCheck failed, {}", msg.OrderRequest.ErrorMsg);
                return false;
            }
            newLong -= volume;
        }
        else if(direction == Message::EOrderDirection::ESHORT_SELL)
        {
            newShort += volume;
        }
        else if(direction == Message::EOrderDirection::EREPAY_STOCK_BY_BUY)
        {
            // 买入还券平空头
            if(volume > shortVol)
            {
                msg.OrderRequest.ErrorID = Message::ERiskRejectedType::ETICKER_SHORT_INSUFFICIENT;
                msg.OrderRequest.RiskStatus = Message::ERiskStatusType::ECHECKED_NOPASS;
                fmt::format_to_n(msg.OrderRequest.ErrorMsg, sizeof(msg.OrderRequest.ErrorMsg),
                                    "Repay Stock Buy Close short exceeds position, Account:{} Ticker:{} ShortPos:{} CloseVol:{}",
                                    key.Account, key.Ticker, shortVol, volume);
                FMTLOG(fmtlog::WRN, "AccountPositionLimitCheck failed, {}", msg.OrderRequest.ErrorMsg);
                return false;
            }
            newShort -= volume;
        }
        else if(direction == Message::EOrderDirection::EREPAY_STOCK_DIRECT)
        {
            // 现券是否足够
            if(volume > longVol)
            {
                msg.OrderRequest.ErrorID = Message::ERiskRejectedType::ETICKER_LONG_INSUFFICIENT;
                msg.OrderRequest.RiskStatus = Message::ERiskStatusType::ECHECKED_NOPASS;
                fmt::format_to_n(msg.OrderRequest.ErrorMsg, sizeof(msg.OrderRequest.ErrorMsg),
                                    "Direct Repay Stock exceeds long position, Account:{} Ticker:{} LongPos:{} CloseVol:{}",
                                    key.Account, key.Ticker, longVol, volume);
                FMTLOG(fmtlog::WRN, "AccountPositionLimitCheck failed, {}", msg.OrderRequest.ErrorMsg);
                return false;
            }
            // 现券还券平空头
            if(volume > shortVol)
            {
                msg.OrderRequest.ErrorID = Message::ERiskRejectedType::ETICKER_SHORT_INSUFFICIENT;
                msg.OrderRequest.RiskStatus = Message::ERiskStatusType::ECHECKED_NOPASS;
                fmt::format_to_n(msg.OrderRequest.ErrorMsg, sizeof(msg.OrderRequest.ErrorMsg),
                                    "Direct Repay Stock exceeds short position, Account:{} Ticker:{} ShortPos:{} CloseVol:{}",
                                    key.Account, key.Ticker, shortVol, volume);
                FMTLOG(fmtlog::WRN, "AccountPositionLimitCheck failed, {}", msg.OrderRequest.ErrorMsg);
                return false;
            }
            newShort -= volume;
            newLong -= volume;
        }
        // 2 方向持仓上限检查（仅开仓）
        if(offset == Message::EOrderOffset::EOPEN)
        {
            if(direction == Message::EOrderDirection::EBUY && newLong > limit.LongLimit)
            {
                msg.OrderRequest.ErrorID = Message::ERiskRejectedType::ETICKER_LONG_INSUFFICIENT;
                msg.OrderRequest.RiskStatus = Message::ERiskStatusType::ECHECKED_NOPASS;
                fmt::format_to_n(msg.OrderRequest.ErrorMsg, sizeof(msg.OrderRequest.ErrorMsg),
                                "Long position exceeds limit, Account:{} Ticker:{} NewLong:{} Limit:{}",
                                key.Account, key.Ticker, newLong, limit.LongLimit);
                FMTLOG(fmtlog::WRN, "AccountPositionLimitCheck failed, {}", msg.OrderRequest.ErrorMsg);
                return false;
            }
            if(direction == Message::EOrderDirection::ESELL && newShort > limit.ShortLimit)
            {
                msg.OrderRequest.ErrorID = Message::ERiskRejectedType::ETICKER_SHORT_INSUFFICIENT;
                msg.OrderRequest.RiskStatus = Message::ERiskStatusType::ECHECKED_NOPASS;
                fmt::format_to_n(msg.OrderRequest.ErrorMsg, sizeof(msg.OrderRequest.ErrorMsg),
                                "Short position exceeds limit, Account:{} Ticker:{} NewShort:{} Limit:{}",
                                key.Account, key.Ticker, newShort, limit.ShortLimit);
                FMTLOG(fmtlog::WRN, "AccountPositionLimitCheck failed, {}", msg.OrderRequest.ErrorMsg);
                return false;
            }
            if(direction == Message::EOrderDirection::EMARGIN_BUY && newLong > limit.LongLimit)
            {
                msg.OrderRequest.ErrorID = Message::ERiskRejectedType::ETICKER_LONG_INSUFFICIENT;
                msg.OrderRequest.RiskStatus = Message::ERiskStatusType::ECHECKED_NOPASS;
                fmt::format_to_n(msg.OrderRequest.ErrorMsg, sizeof(msg.OrderRequest.ErrorMsg),
                                "Margin Long position exceeds limit, Account:{} Ticker:{} NewLong:{} Limit:{}",
                                key.Account, key.Ticker, newLong, limit.LongLimit);
                FMTLOG(fmtlog::WRN, "AccountPositionLimitCheck failed, {}", msg.OrderRequest.ErrorMsg);
                return false;
            }
            if(direction == Message::EOrderDirection::ESHORT_SELL && newShort > limit.ShortLimit)
            {
                msg.OrderRequest.ErrorID = Message::ERiskRejectedType::ETICKER_SHORT_INSUFFICIENT;
                msg.OrderRequest.RiskStatus = Message::ERiskStatusType::ECHECKED_NOPASS;
                fmt::format_to_n(msg.OrderRequest.ErrorMsg, sizeof(msg.OrderRequest.ErrorMsg),
                                "ShortSell position exceeds limit, Account:{} Ticker:{} NewShort:{} Limit:{}",
                                key.Account, key.Ticker, newShort, limit.ShortLimit);
                FMTLOG(fmtlog::WRN, "AccountPositionLimitCheck failed, {}", msg.OrderRequest.ErrorMsg);
                return false;
            }
        }

        // 3 净持仓暴露限制
        if(limit.ExposureLowerLimit != 0)
        {
            int net = newLong - newShort;
            bool netPass = true;
            if(limit.ExposureLowerLimit > 0)
            {
                // 正值：限制净多头，要求 net >= ExposureLowerLimit
                netPass = (net >= limit.ExposureLowerLimit);
                if(!netPass)
                {
                    msg.OrderRequest.ErrorID = Message::ERiskRejectedType::ETICKER_NET_LONG_LIMIT;
                    msg.OrderRequest.RiskStatus = Message::ERiskStatusType::ECHECKED_NOPASS;
                    fmt::format_to_n(msg.OrderRequest.ErrorMsg, sizeof(msg.OrderRequest.ErrorMsg),
                                    "Net exposure exceeds limit, Account:{} Ticker:{} Net:{} ExposureLowerLimit:{}",
                                    key.Account, key.Ticker, net, limit.ExposureLowerLimit);
                    FMTLOG(fmtlog::WRN, "AccountPositionLimitCheck failed, {}", msg.OrderRequest.ErrorMsg);
                    return false;
                }
            }
            else // limit.ExposureLowerLimit < 0
            {
                // 负值：限制净空头，要求 net <= ExposureLowerLimit -50 表示空头敞口 >= 50）
                netPass = (net <= limit.ExposureLowerLimit);
                if(!netPass)
                {
                    msg.OrderRequest.ErrorID = Message::ERiskRejectedType::ETICKER_NET_SHORT_LIMIT;
                    msg.OrderRequest.RiskStatus = Message::ERiskStatusType::ECHECKED_NOPASS;
                    fmt::format_to_n(msg.OrderRequest.ErrorMsg, sizeof(msg.OrderRequest.ErrorMsg),
                                    "Net exposure exceeds limit, Account:{} Ticker:{} Net:{} ExposureLowerLimit:{}",
                                    key.Account, key.Ticker, net, limit.ExposureLowerLimit);
                    FMTLOG(fmtlog::WRN, "AccountPositionLimitCheck failed, {}", msg.OrderRequest.ErrorMsg);
                    return false;
                }
            }
        }
        if(limit.ExposureUpperLimit != 0)
        {
            int net = newLong - newShort;
            bool netPass = true;
            if(limit.ExposureUpperLimit > 0)
            {
                // 正值：限制净多头，要求 net <= ExposureUpperLimit
                netPass = (net <= limit.ExposureUpperLimit);
                if(!netPass)
                {
                    msg.OrderRequest.ErrorID = Message::ERiskRejectedType::ETICKER_NET_LONG_LIMIT;
                    msg.OrderRequest.RiskStatus = Message::ERiskStatusType::ECHECKED_NOPASS;
                    fmt::format_to_n(msg.OrderRequest.ErrorMsg, sizeof(msg.OrderRequest.ErrorMsg),
                                    "Net exposure exceeds limit, Account:{} Ticker:{} Net:{} ExposureUpperLimit:{}",
                                    key.Account, key.Ticker, net, limit.ExposureUpperLimit);
                    FMTLOG(fmtlog::WRN, "AccountPositionLimitCheck failed, {}", msg.OrderRequest.ErrorMsg);
                    return false;
                }
            }
            else // limit.ExposureUpperLimit < 0
            {
                // 负值：限制净空头，要求 net >= ExposureUpperLimit -50 表示空头敞口 ≤ 50）
                netPass = (net >= limit.ExposureUpperLimit);
                if(!netPass)
                {
                    msg.OrderRequest.ErrorID = Message::ERiskRejectedType::ETICKER_NET_SHORT_LIMIT;
                    msg.OrderRequest.RiskStatus = Message::ERiskStatusType::ECHECKED_NOPASS;
                    fmt::format_to_n(msg.OrderRequest.ErrorMsg, sizeof(msg.OrderRequest.ErrorMsg),
                                    "Net exposure exceeds limit, Account:{} Ticker:{} Net:{} ExposureUpperLimit:{}",
                                    key.Account, key.Ticker, net, limit.ExposureUpperLimit);
                    FMTLOG(fmtlog::WRN, "AccountPositionLimitCheck failed, {}", msg.OrderRequest.ErrorMsg);
                    return false;
                }
            }
        }
    }
    else
    {
        msg.OrderRequest.ErrorID = Message::ERiskRejectedType::ETICKER_NOT_FOUND;
        msg.OrderRequest.RiskStatus = Message::ERiskStatusType::ECHECKED_NOPASS;
        fmt::format_to_n(msg.OrderRequest.ErrorMsg, sizeof(msg.OrderRequest.ErrorMsg),
                        "Account:{} Ticker:{} not found LongLimit/ShortLimit/ExposureLowerLimit/ExposureUpperLimit",
                        key.Account, key.Ticker);
        FMTLOG(fmtlog::WRN, "AccountPositionLimitCheck failed, {}", msg.OrderRequest.ErrorMsg);
        return false;
    }
    return true;
}

void RiskEngine::UpdateTickerRiskLimit(const Message::TOrderStatus& OrderStatus)
{
    bool cancelled = Message::EOrderStatusType::EPARTTRADED_CANCELLED == OrderStatus.OrderStatus ||
                        Message::EOrderStatusType::ECANCELLED == OrderStatus.OrderStatus ||
                        Message::EOrderStatusType::EEXCHANGE_ERROR == OrderStatus.OrderStatus ||
                        Message::EOrderStatusType::EBROKER_ERROR == OrderStatus.OrderStatus;
    if(cancelled)
    {
        int CancelCount = 0;
        RiskLimitKey key;
        key.Account = OrderStatus.Account;
        key.Ticker = OrderStatus.Ticker;
        auto it = m_RiskLimitMap.find(key);
        if(m_RiskLimitMap.end() != it)
        {
            Message::TRiskReport& report = it->second;
            report.CancelCount += 1;
            CancelCount = report.CancelCount;
            fmt::format_to_n(report.UpdateTime, sizeof(report.UpdateTime), "{}", Utils::getCurrentTimeUs());
            // Update SQLite RiskLimitTable
            std::string SQL = fmt::format("UPDATE RiskLimitTable SET RiskID='{}', CancelCount={}, Trader='{}', UpdateTime='{}' WHERE Account='{}' AND Ticker='{}';",
                                        report.RiskID, report.CancelCount, report.Trader, report.UpdateTime, report.Account, report.Ticker);
            std::string errorString;
            bool ok = m_RiskDBManager->UpdateRiskLimitTable(SQL, "UPDATE", &RiskEngine::sqlite3_callback_RiskLimit, errorString);
            fmt::format_to_n(report.Event, sizeof(report.Event), "{}", errorString);
            // Update Risk to Monitor
            {
                Message::PackMessage message;
                memset(&message, 0, sizeof(message));
                message.MessageType = Message::EMessageType::ERiskReport;
                memcpy(&message.RiskReport, &report, sizeof(message.RiskReport));
                while(!m_RiskResponseQueue.Push(message));
            }
            FMTLOG(fmtlog::INF, "RiskEngine::UpdateTickerRiskLimit, Update Cancelled Order Counter, Product:{} Account:{} Ticker:{} "
                                "OrderRef:{} OrderStatus:{} OrderType:{} CancelCount:{}",
                    OrderStatus.Product, OrderStatus.Account, OrderStatus.Ticker, OrderStatus.OrderRef, OrderStatus.OrderStatus, OrderStatus.OrderType, CancelCount);
        }
    }
}

void RiskEngine::UpdateAccountRiskLimit(const Message::TOrderStatus& OrderStatus)
{
    bool cancelled = Message::EOrderStatusType::EPARTTRADED_CANCELLED == OrderStatus.OrderStatus ||
                        Message::EOrderStatusType::ECANCELLED == OrderStatus.OrderStatus ||
                        Message::EOrderStatusType::EEXCHANGE_ERROR == OrderStatus.OrderStatus ||
                        Message::EOrderStatusType::EBROKER_ERROR == OrderStatus.OrderStatus;
    if(cancelled)
    {
        int CancelCount = 0;
        RiskLimitKey key;
        key.Account = OrderStatus.Account;
        key.Ticker = "";
        auto it = m_RiskLimitMap.find(key);
        if(m_RiskLimitMap.end() != it)
        {
            Message::TRiskReport& report = it->second;
            report.CancelCount += 1;
            CancelCount = report.CancelCount;
            fmt::format_to_n(report.UpdateTime, sizeof(report.UpdateTime), "{}", Utils::getCurrentTimeUs());
            // Update SQLite RiskLimitTable
            std::string SQL = fmt::format("UPDATE RiskLimitTable SET RiskID='{}', CancelCount={}, Trader='{}', UpdateTime='{}' WHERE Account='{}' AND Ticker='{}';",
                                        report.RiskID, report.CancelCount, report.Trader, report.UpdateTime, report.Account, report.Ticker);
            std::string errorString;
            bool ok = m_RiskDBManager->UpdateRiskLimitTable(SQL, "UPDATE", &RiskEngine::sqlite3_callback_RiskLimit, errorString);
            fmt::format_to_n(report.Event, sizeof(report.Event), "{}", errorString);
            // Update Risk to Monitor
            {
                Message::PackMessage message;
                memset(&message, 0, sizeof(message));
                message.MessageType = Message::EMessageType::ERiskReport;
                memcpy(&message.RiskReport, &report, sizeof(message.RiskReport));
                while(!m_RiskResponseQueue.Push(message));
            }
            FMTLOG(fmtlog::INF, "RiskEngine::UpdateAccountRiskLimit, Update Order Cancelled Counter, Product:{} Account:{} Ticker:{} "
                                "OrderRef:{} OrderStatus:{} OrderType:{} CancelCount:{}",
                    OrderStatus.Product, OrderStatus.Account, OrderStatus.Ticker, OrderStatus.OrderRef, OrderStatus.OrderStatus, OrderStatus.OrderType, CancelCount);
        }
    }
    // 委托申报
    if(Message::EOrderStatusType::EORDER_SENDED == OrderStatus.OrderStatus || Message::EOrderStatusType::ECANCELLING == OrderStatus.OrderStatus)
    {
        int OrderCount = 0;
        RiskLimitKey key;
        key.Account = OrderStatus.Account;
        key.Ticker = "";
        auto it = m_RiskLimitMap.find(key);
        if(m_RiskLimitMap.end() != it)
        {
            Message::TRiskReport& report = it->second; 
            report.OrderCount += 1;
            OrderCount = report.OrderCount;
            fmt::format_to_n(report.UpdateTime, sizeof(report.UpdateTime), "{}", Utils::getCurrentTimeUs());
            // Update SQLite RiskLimitTable
            std::string SQL = fmt::format("UPDATE RiskLimitTable SET RiskID='{}', OrderCount={}, Trader='{}', UpdateTime='{}' WHERE Account='{}' AND Ticker='{}';",
                                        report.RiskID, report.OrderCount, report.Trader, report.UpdateTime, report.Account, report.Ticker);
            std::string errorString;
            bool ok = m_RiskDBManager->UpdateRiskLimitTable(SQL, "UPDATE", &RiskEngine::sqlite3_callback_RiskLimit, errorString);
            fmt::format_to_n(report.Event, sizeof(report.Event), "{}", errorString);
            // Update Risk to Monitor
            {
                Message::PackMessage message;
                memset(&message, 0, sizeof(message));
                message.MessageType = Message::EMessageType::ERiskReport;
                memcpy(&message.RiskReport, &report, sizeof(message.RiskReport));
                while(!m_RiskResponseQueue.Push(message));
            }
            FMTLOG(fmtlog::INF, "RiskEngine::UpdateAccountRiskLimit, Update Order Counter, Product:{} Account:{} Ticker:{} "
                                "OrderRef:{} OrderStatus:{} OrderType:{} OrderCount:{}",
                                OrderStatus.Product, OrderStatus.Account, OrderStatus.Ticker, OrderStatus.OrderRef, OrderStatus.OrderStatus, 
                                OrderStatus.OrderType, OrderCount);
        }
    }
}


void RiskEngine::UpdateStrategyPosition(const Message::TOrderStatus& status) 
{
    if(status.EngineID == 0)
        return ;
    VirtualPositionKey key;
    key.Account = status.Account;   
    key.EngineID = status.EngineID;
    key.Ticker = status.Ticker;

    auto it = m_StrategyPositionLimitMap.find(key);
    if (it == m_StrategyPositionLimitMap.end())
        return ; // 未配置限制，通过

    Message::TRiskReport& strategyPos = it->second;
    // 现货买入/开多仓
    if(status.OrderSide == Message::EOrderSide::EOPEN_LONG) 
    {
        strategyPos.LongVolume += status.TotalTradedVolume;  
    } 
    // 现货卖出/平多仓
    else if(status.OrderSide == Message::EOrderSide::ECLOSE_TD_LONG ||
        status.OrderSide == Message::EOrderSide::ECLOSE_YD_LONG ||
        status.OrderSide == Message::EOrderSide::ECLOSE_LONG) 
    {
        strategyPos.LongVolume -= status.TotalTradedVolume;
    }
    // 开空仓
    else if(status.OrderSide == Message::EOrderSide::EOPEN_SHORT) 
    {
        strategyPos.ShortVolume += status.TotalTradedVolume;
    }
    // 买入平空仓
    else if(status.OrderSide == Message::EOrderSide::ECLOSE_TD_SHORT ||
        status.OrderSide == Message::EOrderSide::ECLOSE_YD_SHORT ||
        status.OrderSide == Message::EOrderSide::ECLOSE_SHORT) 
    {
        strategyPos.ShortVolume -= status.TotalTradedVolume;
    }
    // 担保品买入/融资买入
    else if(status.OrderSide == Message::EOrderSide::ESIDE_COLLATERAL_BUY ||
        status.OrderSide == Message::EOrderSide::ESIDE_MARGIN_BUY) 
    {
        strategyPos.LongVolume += status.TotalTradedVolume;  
    } 
    // 担保品卖出/卖券还款
    else if(status.OrderSide == Message::EOrderSide::ESIDE_COLLATERAL_SELL ||
        status.OrderSide == Message::EOrderSide::ESIDE_REPAY_MARGIN_BY_SELL) 
    {
        strategyPos.LongVolume -= status.TotalTradedVolume;
    }
    // 融券卖出
    else if(status.OrderSide == Message::EOrderSide::ESIDE_SHORT_SELL) 
    {
        strategyPos.ShortVolume += status.TotalTradedVolume;
    }
    // 买券还券
    else if(status.OrderSide == Message::EOrderSide::ESIDE_REPAY_STOCK_BY_BUY) 
    {
        strategyPos.ShortVolume -= status.TotalTradedVolume;
    }
    // 现券还券
    else if(status.OrderSide == Message::EOrderSide::ESIDE_REPAY_STOCK_DIRECT) 
    {
        strategyPos.LongVolume -= status.TotalTradedVolume;
        strategyPos.ShortVolume -= status.TotalTradedVolume;
    }
    fmt::format_to_n(strategyPos.RiskID, sizeof(strategyPos.RiskID), "{}", m_XRiskJudgeConfig.RiskID);
    fmt::format_to_n(strategyPos.UpdateTime, sizeof(strategyPos.UpdateTime), "{}", Utils::getCurrentTimeUs());
    // Update SQLite PositionLimitTable
    std::string SQL;
    std::string errorString;
    SQL = fmt::format("UPDATE PositionLimitTable SET RiskID='{}', LongVolume='{}', ShortVolume='{}', UpdateTime='{}' WHERE Account='{}' AND Ticker='{}' AND EngineID='{}';",
                                strategyPos.RiskID, strategyPos.LongVolume, strategyPos.ShortVolume, strategyPos.UpdateTime, 
                                strategyPos.Account, strategyPos.Ticker, strategyPos.EngineID);
    m_RiskDBManager->UpdatePositionLimitTable(SQL, "UPDATE", &RiskEngine::sqlite3_callback_PositionLimit, errorString);
    FMTLOG(fmtlog::INF, "UpdateStrategyPosition Account:{} Ticker:{} EngineID:{} Long:{} Short:{} LongLimit:{} ShortLimit:{}",
           key.Account, key.Ticker, key.EngineID, strategyPos.LongVolume, strategyPos.ShortVolume, strategyPos.LongLimit, strategyPos.ShortLimit);

    Message::PackMessage msg;
    msg.MessageType = Message::EMessageType::ERiskReport;
    memcpy(&msg.RiskReport, &strategyPos, sizeof(msg.RiskReport));
    m_RiskResponseQueue.Push(msg);
}

void RiskEngine::UpdateAccountPosition(const Message::TOrderStatus& status)
{
    VirtualPositionKey key;
    key.Account = status.Account;   
    key.Ticker = status.Ticker;
    key.EngineID = 0;
    
    auto it = m_StrategyPositionLimitMap.find(key);
    if (it == m_StrategyPositionLimitMap.end())
        return ; // 未配置限制，通过

    Message::TRiskReport& accountPos = it->second;
    // 现货买入/开多仓
    if(status.OrderSide == Message::EOrderSide::EOPEN_LONG) 
    {
        accountPos.LongVolume += status.TotalTradedVolume;  
    } 
    // 现货卖出/平多仓
    else if(status.OrderSide == Message::EOrderSide::ECLOSE_TD_LONG ||
        status.OrderSide == Message::EOrderSide::ECLOSE_YD_LONG ||
        status.OrderSide == Message::EOrderSide::ECLOSE_LONG) 
    {
        accountPos.LongVolume -= status.TotalTradedVolume;
    }
    // 开空仓
    else if(status.OrderSide == Message::EOrderSide::EOPEN_SHORT) 
    {
        accountPos.ShortVolume += status.TotalTradedVolume;
    }
    // 买入平空仓
    else if(status.OrderSide == Message::EOrderSide::ECLOSE_TD_SHORT ||
        status.OrderSide == Message::EOrderSide::ECLOSE_YD_SHORT ||
        status.OrderSide == Message::EOrderSide::ECLOSE_SHORT) 
    {
        accountPos.ShortVolume -= status.TotalTradedVolume;
    }
    // 担保品买入/融资买入
    else if(status.OrderSide == Message::EOrderSide::ESIDE_COLLATERAL_BUY ||
        status.OrderSide == Message::EOrderSide::ESIDE_MARGIN_BUY) 
    {
        accountPos.LongVolume += status.TotalTradedVolume;
    } 
    // 担保品卖出/卖券还款
    else if(status.OrderSide == Message::EOrderSide::ESIDE_COLLATERAL_SELL ||
        status.OrderSide == Message::EOrderSide::ESIDE_REPAY_MARGIN_BY_SELL) 
    {
        accountPos.LongVolume -= status.TotalTradedVolume;
    }
    // 融券卖出
    else if(status.OrderSide == Message::EOrderSide::ESIDE_SHORT_SELL) 
    {
        accountPos.ShortVolume += status.TotalTradedVolume;
    }
    // 买券还券
    else if(status.OrderSide == Message::EOrderSide::ESIDE_REPAY_STOCK_BY_BUY) 
    {
        accountPos.ShortVolume -= status.TotalTradedVolume;
    }
    // 现券还券
    else if(status.OrderSide == Message::EOrderSide::ESIDE_REPAY_STOCK_DIRECT) 
    {
        accountPos.LongVolume -= status.TotalTradedVolume;
        accountPos.ShortVolume -= status.TotalTradedVolume;
    }
    fmt::format_to_n(accountPos.RiskID, sizeof(accountPos.RiskID), "{}", m_XRiskJudgeConfig.RiskID);
    fmt::format_to_n(accountPos.UpdateTime, sizeof(accountPos.UpdateTime), "{}", Utils::getCurrentTimeUs());
    // Update SQLite PositionLimitTable
    std::string SQL;
    std::string errorString;
    SQL = fmt::format("UPDATE PositionLimitTable SET RiskID='{}', LongVolume='{}', ShortVolume='{}', UpdateTime='{}' WHERE Account='{}' AND Ticker='{}' AND EngineID='{}';",
                                accountPos.RiskID, accountPos.LongVolume, accountPos.ShortVolume, accountPos.UpdateTime,
                                accountPos.Account, accountPos.Ticker, accountPos.EngineID);
    m_RiskDBManager->UpdatePositionLimitTable(SQL, "UPDATE", &RiskEngine::sqlite3_callback_PositionLimit, errorString);

    FMTLOG(fmtlog::INF, "UpdateStrategyPosition Account:{} Ticker:{} EngineID:{} Long:{} Short:{} LongLimit:{} ShortLimit:{}",
           key.Account, key.Ticker, key.EngineID, accountPos.LongVolume, accountPos.ShortVolume, accountPos.LongLimit, accountPos.ShortLimit);

    Message::PackMessage msg;
    msg.MessageType = Message::EMessageType::ERiskReport;
    memcpy(&msg.RiskReport, &accountPos, sizeof(msg.RiskReport));
    m_RiskResponseQueue.Push(msg);
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
        for(auto& riskLimit : m_RiskLimitMap) 
        {
            Message::PackMessage msg;
            msg.MessageType = Message::EMessageType::ERiskReport;
            memcpy(&msg.RiskReport, &riskLimit.second, sizeof(msg.RiskReport));
            m_RiskResponseQueue.Push(msg);
        }
    }
    return ret;
}

bool RiskEngine::QueryPositionLimit() 
{
    std::string errorString;
    bool ret = m_RiskDBManager->QueryPositionLimit(&RiskEngine::sqlite3_callback_PositionLimit, errorString);
    if(!ret) 
    {
        FMTLOG(fmtlog::WRN, "QueryPositionLimit failed, {}", errorString);
    } 
    else 
    {
        for(auto& positionLimit : m_StrategyPositionLimitMap) 
        {
            Message::PackMessage msg;
            msg.MessageType = Message::EMessageType::ERiskReport;
            memcpy(&msg.RiskReport, &positionLimit.second, sizeof(msg.RiskReport));
            m_RiskResponseQueue.Push(msg);
        }
        FMTLOG(fmtlog::INF, "RiskEngine::QueryPositionLimit {}", m_StrategyPositionLimitMap.size());
    }
    return ret;
}

bool RiskEngine::QueryAccountLocked()
{
    std::string errorString;
    bool ret = m_RiskDBManager->QueryAccountLocked(&RiskEngine::sqlite3_callback_AccountLocked, errorString);
    if(!ret) 
    {
        FMTLOG(fmtlog::WRN, "QueryAccountLocked failed, {}", errorString);
    } 
    else 
    {
        for(auto& accountLocked : m_AccountLockedMap) 
        {
            Message::PackMessage msg;
            msg.MessageType = Message::EMessageType::ERiskReport;
            memcpy(&msg.RiskReport, &accountLocked.second, sizeof(msg.RiskReport));
            m_RiskResponseQueue.Push(msg);
        }
        FMTLOG(fmtlog::INF, "RiskEngine::QueryAccountLocked {}", m_AccountLockedMap.size());
    }
    return ret;
}


int RiskEngine::sqlite3_callback_RiskLimit(void *data, int argc, char **argv, char **azColName) 
{
    static std::string riskID;
    static std::string account;
    static std::string ticker;
    static uint32_t businessType = 0;
    static int flowLimit = 0;
    static int cancelCount = 0;
    static int cancelLimit = 0;
    static int orderCount = 0;
    static int orderLimit = 0;
    static int orderCancelLimit = 0;
    static std::string trader;
    for(int i = 0; i < argc; i++) 
    {
        FMTLOG(fmtlog::INF, "RiskEngine::sqlite3_callback_RiskLimit, {} {} = {}", (char*)data, azColName[i], argv[i]);
        std::string col = azColName[i];
        std::string val = argv[i] ? argv[i] : "";
        if(col == "RiskID") 
            riskID = val;
        else if(col == "Account") 
            account = val;
        else if(col == "Ticker") 
            ticker = val;
        else if(col == "BusinessType") 
            businessType = std::stoull(val);
        else if(col == "FlowLimit") 
            flowLimit = std::stoi(val);
        else if(col == "CancelCount") 
            cancelCount = std::stoi(val);
        else if(col == "CancelLimit") 
            cancelLimit = std::stoi(val);
        else if(col == "OrderCount") 
            orderCount = std::stoi(val);
        else if(col == "OrderLimit") 
            orderLimit = std::stoi(val);
        else if(col == "OrderCancelLimit") 
            orderCancelLimit = std::stoi(val);
        else if(col == "Trader") 
            trader = val;
        else if(col == "UpdateTime") 
        {
            RiskLimitKey key;
            key.Account = account;
            key.Ticker = ticker;
            auto& limit = m_RiskLimitMap[key];
            limit.ReportType = Message::ERiskReportType::ERISK_LIMIT;
            fmt::format_to_n(limit.RiskID, sizeof(limit.RiskID), "{}", riskID);
            fmt::format_to_n(limit.Account, sizeof(limit.Account), "{}", account);
            fmt::format_to_n(limit.Ticker, sizeof(limit.Ticker), "{}", ticker);
            limit.BusinessType = businessType;
            limit.FlowLimit = flowLimit;
            limit.CancelCount = cancelCount;
            limit.CancelLimit = cancelLimit;
            limit.OrderCount = orderCount;
            limit.OrderLimit = orderLimit;
            limit.OrderCancelLimit = orderCancelLimit;
            fmt::format_to_n(limit.Trader, sizeof(limit.Trader), "{}", trader);
            fmt::format_to_n(limit.UpdateTime, sizeof(limit.UpdateTime), "{}", Utils::getCurrentTimeUs());
        }
    }
    return 0;
}


int RiskEngine::sqlite3_callback_PositionLimit(void *data, int argc, char **argv, char **azColName) 
{
    static std::string riskID;
    static std::string account;
    static uint32_t engineID = 0;
    static std::string ticker;
    static uint32_t businessType = 0;
    static int longVolume = 0;
    static int shortVolume = 0;
    static int longLimit = 0;
    static int shortLimit = 0;
    static int exposureLowerLimit = 0;
    static int exposureUpperLimit = 0;
    static std::string trader;
    for(int i = 0; i < argc; i++) 
    {
        FMTLOG(fmtlog::INF, "RiskEngine::sqlite3_callback_PositionLimit, {} {} = {}", (char*)data, azColName[i], argv[i]);
        std::string col = azColName[i];
        std::string val = argv[i] ? argv[i] : "";
        if(col == "RiskID") 
            riskID = val;
        else if(col == "Account") 
            account = val;
        else if(col == "Ticker") 
            ticker = val;
        else if(col == "EngineID") 
            engineID = std::stoull(val);
        else if(col == "BusinessType") 
            businessType = std::stoull(val);
        else if(col == "LongVolume") 
            longVolume = std::stoi(val);
        else if(col == "ShortVolume") 
            shortVolume = std::stoi(val);
        else if(col == "LongLimit") 
            longLimit = std::stoi(val);
        else if(col == "ShortLimit") 
            shortLimit = std::stoi(val);
        else if(col == "ExposureLowerLimit") 
            exposureLowerLimit = std::stoi(val);
        else if(col == "ExposureUpperLimit") 
            exposureUpperLimit = std::stoi(val);
        else if(col == "Trader") 
            trader = val;
        else if(col == "UpdateTime") 
        {
            VirtualPositionKey key;
            key.Account = account;
            key.Ticker = ticker;
            key.EngineID = engineID;
            auto& pos = m_StrategyPositionLimitMap[key];
            pos.ReportType = Message::ERiskReportType::ERISK_POSITION_LIMIT;
            fmt::format_to_n(pos.RiskID, sizeof(pos.RiskID), "{}", riskID);
            fmt::format_to_n(pos.Account, sizeof(pos.Account), "{}", account);
            fmt::format_to_n(pos.Ticker, sizeof(pos.Ticker), "{}", ticker);
            pos.EngineID = engineID;
            pos.BusinessType = businessType;
            pos.LongVolume = longVolume;
            pos.ShortVolume = shortVolume;
            pos.LongLimit = longLimit;
            pos.ShortLimit = shortLimit;
            pos.ExposureLowerLimit = exposureLowerLimit;
            pos.ExposureUpperLimit = exposureUpperLimit;
            fmt::format_to_n(pos.Trader, sizeof(pos.Trader), "{}", trader);
            fmt::format_to_n(pos.UpdateTime, sizeof(pos.UpdateTime), "{}", Utils::getCurrentTimeUs());
        }
    }
    return 0;
}

int RiskEngine::sqlite3_callback_AccountLocked(void *data, int argc, char **argv, char **azColName) 
{
    static std::string riskID;
    static std::string account;
    static std::string ticker;
    static uint32_t businessType = 0;
    static int lockSide = 0;
    static std::string trader;
    for(int i = 0; i < argc; i++) 
    {
        FMTLOG(fmtlog::INF, "RiskEngine::sqlite3_callback_AccountLocked, {} {} = {}", (char*)data, azColName[i], argv[i]);
        std::string col = azColName[i];
        std::string val = argv[i] ? argv[i] : "";
        if(col == "RiskID") 
            riskID = val;
        else if(col == "Account") 
            account = val;
        else if(col == "Ticker") 
            ticker = val;
        else if(col == "BusinessType") 
            businessType = std::stoull(val);
        else if(col == "LockSide") 
            lockSide = std::stoi(val);
        else if(col == "Trader") 
            trader = val;
        else if(col == "UpdateTime") 
        {
            RiskLimitKey key;
            key.Account = account;
            key.Ticker = ticker;
            auto& accountLocked = m_AccountLockedMap[key];
            accountLocked.ReportType = Message::ERiskReportType::ERISK_ACCOUNT_LOCKED;
            fmt::format_to_n(accountLocked.RiskID, sizeof(accountLocked.RiskID), "{}", riskID);
            fmt::format_to_n(accountLocked.Account, sizeof(accountLocked.Account), "{}", account);
            fmt::format_to_n(accountLocked.Ticker, sizeof(accountLocked.Ticker), "{}", ticker);
            accountLocked.BusinessType = businessType;
            accountLocked.LockSide = lockSide;
            fmt::format_to_n(accountLocked.Trader, sizeof(accountLocked.Trader), "{}", trader);
            fmt::format_to_n(accountLocked.UpdateTime, sizeof(accountLocked.UpdateTime), "{}", Utils::getCurrentTimeUs());
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
    else if(Message::ECommandType::EUPDATE_RISK_POSITION_LIMIT == command.CmdType)
    {
        std::string sql, op;
        if(ParseUpdatePositionLimitCommand(cmd, sql, op, RiskEvent))
        {
            std::string errorString;
            bool ok = m_RiskDBManager->UpdatePositionLimitTable(sql, op, &RiskEngine::sqlite3_callback_PositionLimit, errorString);
            strncpy(RiskEvent.Event, errorString.c_str(), sizeof(RiskEvent.Event));
            QueryPositionLimit();
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
        if(ParseUpdateAccountLockedCommand(cmd, sql, op, RiskEvent))
        {
            std::string errorString;
            bool ok = m_RiskDBManager->UpdateAccountLockedTable(sql, op, &RiskEngine::sqlite3_callback_AccountLocked, errorString);
            strncpy(RiskEvent.Event, errorString.c_str(), sizeof(RiskEvent.Event));
            QueryAccountLocked();
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


bool RiskEngine::ParseUpdateRiskLimitCommand(const std::string& cmd, std::string& sql, std::string& op, Message::TRiskReport& event)
{
    bool ret = true;
    sql.clear();
    std::vector<std::string> items;
    Utils::Split(cmd, ",", items);
    if(11 == items.size())
    {
        std::vector<std::string> keyValue;
        Utils::Split(items[0], ":", keyValue);
        std::string RiskID = keyValue[1];
        fmt::format_to_n(event.RiskID, sizeof(event.RiskID), "{}", RiskID);

        keyValue.clear();
        Utils::Split(items[1], ":", keyValue);
        std::string Account = keyValue[1];
        fmt::format_to_n(event.Account, sizeof(event.Account), "{}", Account);

        keyValue.clear();
        Utils::Split(items[2], ":", keyValue);
        std::string Ticker = keyValue[1];
        fmt::format_to_n(event.Ticker, sizeof(event.Ticker), "{}", Ticker);

        keyValue.clear();
        Utils::Split(items[3], ":", keyValue);
        int BusinessType = atoi(keyValue[1].c_str());
        event.BusinessType = BusinessType;

        keyValue.clear();
        Utils::Split(items[4], ":", keyValue);
        int FlowLimit = atoi(keyValue[1].c_str());
        event.FlowLimit = FlowLimit;

        keyValue.clear();
        Utils::Split(items[5], ":", keyValue);
        int CancelCount = atoi(keyValue[1].c_str());
        event.CancelCount = CancelCount;

        keyValue.clear();
        Utils::Split(items[6], ":", keyValue);
        int CancelLimit = atoi(keyValue[1].c_str());
        event.CancelLimit = CancelLimit;

        keyValue.clear();
        Utils::Split(items[7], ":", keyValue);
        int OrderCount = atoi(keyValue[1].c_str());
        event.OrderCount = OrderCount;

        keyValue.clear();
        Utils::Split(items[8], ":", keyValue);
        int OrderLimit = atoi(keyValue[1].c_str());
        event.OrderLimit = OrderLimit;

        keyValue.clear();
        Utils::Split(items[9], ":", keyValue);
        int OrderCancelLimit = atoi(keyValue[1].c_str());
        event.OrderCancelLimit = OrderCancelLimit;

        keyValue.clear();
        Utils::Split(items[10], ":", keyValue);
        std::string Trader = keyValue[1];

        fmt::format_to_n(event.Trader, sizeof(event.Trader), "{}", Trader);
        fmt::format_to_n(event.UpdateTime, sizeof(event.UpdateTime), "{}", Utils::getCurrentTimeUs());

        RiskLimitKey key;
        key.Account = Account;
        key.Ticker = Ticker;

        auto it = m_RiskLimitMap.find(key);
        if(m_RiskLimitMap.end() == it)
        {
            // Insert
            sql = fmt::format("INSERT INTO RiskLimitTable(RiskID,Account,Ticker,BusinessType,FlowLimit,CancelCount,CancelLimit,OrderCount,OrderLimit,OrderCancelLimit,Trader,UpdateTime) VALUES('{}','{}','{}',{},{},{},{},{},{},{},'{}','{}');",
                            RiskID, Account, Ticker, BusinessType, FlowLimit, CancelCount, CancelLimit, OrderCount, OrderLimit, OrderCancelLimit, Trader, event.UpdateTime);
            op = "INSERT";
        }
        else
        {
            // Update
            sql = fmt::format("UPDATE RiskLimitTable SET RiskID='{}',BusinessType={},FlowLimit={},CancelCount={},CancelLimit={},OrderCount={},OrderLimit={},OrderCancelLimit={},Trader='{}',UpdateTime='{}' WHERE Account='{}' AND Ticker='{}';",
                            RiskID, BusinessType, FlowLimit, CancelCount, CancelLimit, OrderCount, OrderLimit, OrderCancelLimit, Trader, event.UpdateTime, Account, Ticker);
            op = "UPDATE";
        }
        FMTLOG(fmtlog::INF, "RiskEngine::ParseUpdateRiskLimitCommand, RiskID:{} Account:{} Ticker:{} FlowLimit:{} CancelCount:{} CancelLimit:{} OrderCount:{} OrderLimit:{} OrderCancelLimit:{} Trader:{} MapSize:{}",
                RiskID, Account, Ticker, FlowLimit, CancelCount, CancelLimit, OrderCount, OrderLimit, OrderCancelLimit, Trader, m_RiskLimitMap.size());
    }
    else
    {
        ret = false;
        fmt::format_to_n(event.RiskID, sizeof(event.RiskID), "{}", m_XRiskJudgeConfig.RiskID);
        fmt::format_to_n(event.Event, sizeof(event.Event), "invalid command:{}", cmd);
        FMTLOG(fmtlog::WRN, "RiskEngine::ParseUpdateRiskLimitCommand invalid command, {}", cmd);
    }
    return ret;
}

bool RiskEngine::ParseUpdatePositionLimitCommand(const std::string& cmd, std::string& sql, std::string& op, Message::TRiskReport& event)
{
    bool ret = true;
    sql.clear();
    std::vector<std::string> items;
    Utils::Split(cmd, ",", items);
    if(12 == items.size())
    {
        std::vector<std::string> keyValue;
        Utils::Split(items[0], ":", keyValue);
        std::string RiskID = keyValue[1];
        fmt::format_to_n(event.RiskID, sizeof(event.RiskID), "{}", RiskID);

        keyValue.clear();
        Utils::Split(items[1], ":", keyValue);
        std::string Account = keyValue[1];
        fmt::format_to_n(event.Account, sizeof(event.Account), "{}", Account);

        keyValue.clear();
        Utils::Split(items[2], ":", keyValue);
        std::string Ticker = keyValue[1];
        fmt::format_to_n(event.Ticker, sizeof(event.Ticker), "{}", Ticker);

        keyValue.clear();
        Utils::Split(items[3], ":", keyValue);
        int EngineID = atoi(keyValue[1].c_str());

        keyValue.clear();
        Utils::Split(items[4], ":", keyValue);
        int BusinessType = atoi(keyValue[1].c_str());

        keyValue.clear();
        Utils::Split(items[5], ":", keyValue);
        int LongVolume = atoi(keyValue[1].c_str());

        keyValue.clear();
        Utils::Split(items[6], ":", keyValue);
        int ShortVolume = atoi(keyValue[1].c_str());

        keyValue.clear();
        Utils::Split(items[7], ":", keyValue);
        int LongLimit = atoi(keyValue[1].c_str());

        keyValue.clear();
        Utils::Split(items[8], ":", keyValue);
        int ShortLimit = atoi(keyValue[1].c_str());

        keyValue.clear();
        Utils::Split(items[9], ":", keyValue);
        int ExposureLowerLimit = atoi(keyValue[1].c_str());

        keyValue.clear();
        Utils::Split(items[10], ":", keyValue);
        int ExposureUpperLimit = atoi(keyValue[1].c_str());

        keyValue.clear();
        Utils::Split(items[11], ":", keyValue);
        std::string Trader = keyValue[1];
        fmt::format_to_n(event.Trader, sizeof(event.Trader), "{}", Trader);

        fmt::format_to_n(event.UpdateTime, sizeof(event.UpdateTime), "{}", Utils::getCurrentTimeUs());

        VirtualPositionKey key;
        key.Account = Account;
        key.Ticker = Ticker;
        key.EngineID = EngineID;

        auto it = m_StrategyPositionLimitMap.find(key);
        if(m_StrategyPositionLimitMap.end() == it)
        {
            // Insert
            sql = fmt::format("INSERT INTO PositionLimitTable(RiskID,Account,Ticker,EngineID,BusinessType,LongVolume,ShortVolume,LongLimit,ShortLimit,ExposureLowerLimit,ExposureUpperLimit,Trader,UpdateTime) VALUES('{}','{}','{}',{},{},{},{},{},{},{},{},'{}','{}');",
                            RiskID, Account, Ticker, EngineID, BusinessType, LongVolume, ShortVolume, LongLimit, ShortLimit, ExposureLowerLimit, ExposureUpperLimit, Trader, event.UpdateTime);
            op = "INSERT";
        }
        else
        {
            // Update
            sql = fmt::format("UPDATE PositionLimitTable SET RiskID='{}',BusinessType={},LongVolume={},ShortVolume={},LongLimit={},ShortLimit={},ExposureLowerLimit={},ExposureUpperLimit={},Trader='{}',UpdateTime='{}' WHERE Account='{}' AND Ticker='{}' AND EngineID={};",
                            RiskID, BusinessType, LongVolume, ShortVolume, LongLimit, ShortLimit, ExposureLowerLimit, ExposureUpperLimit, Trader, event.UpdateTime, Account, Ticker, EngineID);
            op = "UPDATE";
        }
        FMTLOG(fmtlog::INF, "RiskEngine::ParseUpdatePositionLimitCommand, RiskID:{} Account:{} Ticker:{} EngineID:{} BusinessType:{} LongVolume:{} ShortVolume:{} LongLimit:{} ShortLimit:{} ExposureLowerLimit:{} ExposureUpperLimit:{} Trader:{} MapSize:{}",
                RiskID, Account, Ticker, EngineID, BusinessType, LongVolume, ShortVolume, LongLimit, ShortLimit, ExposureLowerLimit, ExposureUpperLimit, Trader, m_StrategyPositionLimitMap.size());
    }
    else
    {
        ret = false;
        fmt::format_to_n(event.RiskID, sizeof(event.RiskID), "{}", m_XRiskJudgeConfig.RiskID);
        fmt::format_to_n(event.Event, sizeof(event.Event), "invalid command:{}", cmd);
        FMTLOG(fmtlog::WRN, "RiskEngine::ParseUpdatePositionLimitCommand invalid command, {}", cmd);
    }
    return ret;
}

bool RiskEngine::ParseUpdateAccountLockedCommand(const std::string& cmd, std::string& sql, std::string& op, Message::TRiskReport& event)
{
    bool ret = true;
    sql.clear();
    std::vector<std::string> items;
    Utils::Split(cmd, ",", items);
    if(6 == items.size())
    {
        std::vector<std::string> keyValue;
        Utils::Split(items[0], ":", keyValue);
        std::string RiskID = keyValue[1];
        fmt::format_to_n(event.RiskID, sizeof(event.RiskID), "{}", RiskID);

        keyValue.clear();
        Utils::Split(items[1], ":", keyValue);
        std::string Account = keyValue[1];
        fmt::format_to_n(event.Account, sizeof(event.Account), "{}", Account);

        keyValue.clear();
        Utils::Split(items[2], ":", keyValue);
        std::string Ticker = keyValue[1];
        fmt::format_to_n(event.Ticker, sizeof(event.Ticker), "{}", Ticker);

        keyValue.clear();
        Utils::Split(items[3], ":", keyValue);
        int BusinessType = atoi(keyValue[1].c_str());

        keyValue.clear();
        Utils::Split(items[4], ":", keyValue);
        int LockSide = atoi(keyValue[1].c_str());

        keyValue.clear();
        Utils::Split(items[5], ":", keyValue);
        std::string Trader = keyValue[1];
        fmt::format_to_n(event.Trader, sizeof(event.Trader), "{}", Trader);

        fmt::format_to_n(event.UpdateTime, sizeof(event.UpdateTime), "{}", Utils::getCurrentTimeUs());

        RiskLimitKey key;
        key.Account = Account;
        key.Ticker = Ticker;

        auto it = m_AccountLockedMap.find(key);
        if(m_AccountLockedMap.end() == it)
        {
            // Insert
            sql = fmt::format("INSERT INTO AccountLockedTable(RiskID,Account,Ticker,BusinessType,LockSide,Trader,UpdateTime) VALUES('{}','{}','{}',{},{},'{}','{}');",
                            RiskID, Account, Ticker, BusinessType, LockSide, Trader, event.UpdateTime);
            op = "INSERT";
        }
        else
        {
            // Update
            sql = fmt::format("UPDATE AccountLockedTable SET RiskID='{}',BusinessType={},LockSide={},Trader='{}',UpdateTime='{}' WHERE Account='{}' AND Ticker='{}';",
                            RiskID, BusinessType, LockSide, Trader, event.UpdateTime, Account, Ticker);
            op = "UPDATE";
        }
        FMTLOG(fmtlog::INF, "RiskEngine::ParseUpdateAccountLockedCommand, RiskID:{} Account:{} Ticker:{} BusinessType:{} LockSide:{} Trader:{} MapSize:{}",
                RiskID, Account, Ticker, BusinessType, LockSide, Trader, m_AccountLockedMap.size());
    }
    else
    {
        ret = false;
        fmt::format_to_n(event.RiskID, sizeof(event.RiskID), "{}", m_XRiskJudgeConfig.RiskID);
        fmt::format_to_n(event.Event, sizeof(event.Event), "invalid command:{}", cmd);
        FMTLOG(fmtlog::WRN, "RiskEngine::ParseUpdateAccountLockedCommand invalid command, {}", cmd);
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
    std::string CommitID = std::string(APP_COMMITID) + ":" + SHMSERVER_COMMITID;
    strncpy(AppStatus.CommitID, CommitID.c_str(), sizeof(AppStatus.CommitID));
    strncpy(AppStatus.UtilsCommitID, UTILS_COMMITID, sizeof(AppStatus.UtilsCommitID));
    strncpy(AppStatus.APIVersion, API_VERSION, sizeof(AppStatus.APIVersion));
    strncpy(AppStatus.StartTime, Utils::getCurrentTimeUs(), sizeof(AppStatus.StartTime));
    strncpy(AppStatus.LastStartTime, Utils::getCurrentTimeUs(), sizeof(AppStatus.LastStartTime));
    strncpy(AppStatus.UpdateTime, Utils::getCurrentTimeUs(), sizeof(AppStatus.UpdateTime));
}
