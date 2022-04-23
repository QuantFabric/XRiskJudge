#include "RiskEngine.h"

extern Utils::Logger* gLogger;

Utils::RingBuffer<Message::PackMessage> RiskEngine::m_RiskResponseQueue(1 << 10);
XRiskLimit RiskEngine::m_XRiskLimit;
std::unordered_map<std::string, Message::TRiskReport> RiskEngine::m_TickerCancelledCounterMap;

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
    // TODO: Handle OrderRequest
}

void RiskEngine::HandleActionRequest(Message::PackMessage& msg)
{
    // TODO: Handle ActionRequest
}
