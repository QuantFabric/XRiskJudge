#include "HPPackServer.h"


HPPackServer::ConnectionMapT HPPackServer::m_sConnections;
Utils::LockFreeQueue<Message::PackMessage> HPPackServer::m_RequestMessageQueue(1 << 12);

HPPackServer::HPPackServer(const char *ip, unsigned int port)
{
    m_ServerIP = ip;
    m_ServerPort = port;
    // 创建监听器对象
    m_pListener = ::Create_HP_TcpPackServerListener();
    // 创建 Socket 对象
    m_pServer = ::Create_HP_TcpPackServer(m_pListener);

    // 设置 Socket 监听器回调函数
    ::HP_Set_FN_Server_OnPrepareListen(m_pListener, OnPrepareListen);
    ::HP_Set_FN_Server_OnAccept(m_pListener, OnAccept);
    ::HP_Set_FN_Server_OnSend(m_pListener, OnSend);
    ::HP_Set_FN_Server_OnReceive(m_pListener, OnReceive);
    ::HP_Set_FN_Server_OnClose(m_pListener, OnClose);
    ::HP_Set_FN_Server_OnShutdown(m_pListener, OnShutdown);

    // 设置包头标识与最大包长限制
    ::HP_TcpPackServer_SetMaxPackSize(m_pServer, 0xFFFF);
    ::HP_TcpPackServer_SetPackHeaderFlag(m_pServer, 0x169);
    ::HP_TcpServer_SetKeepAliveTime(m_pServer, 30 * 1000);
}

HPPackServer::~HPPackServer()
{
    // 销毁 Socket 对象
    ::Destroy_HP_TcpPackServer(m_pServer);
    // 销毁监听器对象
    ::Destroy_HP_TcpPackServerListener(m_pListener);
}

void HPPackServer::Start()
{
    // EventLog
    Message::PackMessage message;
    memset(&message, 0, sizeof(message));
    message.MessageType = Message::EMessageType::EEventLog;
    char errorString[256] = {0};
    if (::HP_Server_Start(m_pServer, m_ServerIP.c_str(), m_ServerPort))
    {
        fmt::format_to_n(errorString, sizeof(errorString), "HPPackServer::Start listen to {}:{} successed", m_ServerIP, m_ServerPort);
        FMTLOG(fmtlog::INF, "HPPackServer::Start listen to {}:{} successed", m_ServerIP, m_ServerPort);
        message.EventLog.Level = Message::EEventLogLevel::EINFO;
    }
    else
    {
        fmt::format_to_n(errorString, sizeof(errorString), "HPPackServer::Start listen to {}:{} failed, error code:{} error massage:{}",
                        m_ServerIP, m_ServerPort, ::HP_Client_GetLastError(m_pServer), HP_Client_GetLastErrorDesc(m_pServer));
        FMTLOG(fmtlog::WRN, "HPPackServer::Start listen to {}:{} failed, error code:{} error massage:{}",
                m_ServerIP, m_ServerPort, ::HP_Client_GetLastError(m_pServer), HP_Client_GetLastErrorDesc(m_pServer));
        message.EventLog.Level = Message::EEventLogLevel::EWARNING;
    }
    strncpy(message.EventLog.App, APP_NAME, sizeof(message.EventLog.App));
    strncpy(message.EventLog.Event, errorString, sizeof(message.EventLog.Event));
    strncpy(message.EventLog.UpdateTime, Utils::getCurrentTimeUs(), sizeof(message.EventLog.UpdateTime));
    while(!m_RequestMessageQueue.Push(message));
}

void HPPackServer::Stop()
{
    //停止服务器
    ::HP_Server_Stop(m_pServer);
}

void HPPackServer::SendData(HP_CONNID dwConnID, const unsigned char *pBuffer, int iLength)
{
    bool ret = ::HP_Server_Send(m_pServer, dwConnID, pBuffer, iLength);
    if(!ret)
    {
        FMTLOG(fmtlog::WRN, "HPPackServer::SendData failed, sys error:{}, error code:{}, error message:{}",
                SYS_GetLastErrorStr(), HP_Client_GetLastError(m_pServer), HP_Client_GetLastErrorDesc(m_pServer));
        Message::PackMessage message;
        memset(&message, 0, sizeof(message));
        message.MessageType = Message::EMessageType::EEventLog;
        message.EventLog.Level = Message::EEventLogLevel::EWARNING;
        strncpy(message.EventLog.App, APP_NAME, sizeof(message.EventLog.App));
        fmt::format_to_n(message.EventLog.Event, sizeof(message.EventLog.Event), 
                        "HPPackServer::SendData failed, sys error:{}, error code:{}, error message:{}",
                        SYS_GetLastErrorStr(), HP_Client_GetLastError(m_pServer), HP_Client_GetLastErrorDesc(m_pServer));
        strncpy(message.EventLog.UpdateTime, Utils::getCurrentTimeUs(), sizeof(message.EventLog.UpdateTime));
        while(!m_RequestMessageQueue.Push(message));
    }
}

En_HP_HandleResult __stdcall HPPackServer::OnPrepareListen(HP_Server pSender, UINT_PTR soListen)
{
    TCHAR szAddress[50];
    int iAddressLen = sizeof(szAddress) / sizeof(TCHAR);
    USHORT usPort;
    ::HP_Server_GetListenAddress(pSender, szAddress, &iAddressLen, &usPort);
    int enable = 1;
    int ret1 = SYS_SetSocketOption(soListen, IPPROTO_TCP, TCP_NODELAY, &enable, sizeof(enable));
    int ret2 = SYS_SetSocketOption(soListen, IPPROTO_TCP, TCP_QUICKACK, &enable, sizeof(enable));
    FMTLOG(fmtlog::INF, "HPPackServer::OnPrepareListen Socket:{} NoDelay:{} QuickAck:{}", soListen, ret1 == 0, ret2 == 0);
    return HR_OK;
}

En_HP_HandleResult __stdcall HPPackServer::OnAccept(HP_Server pSender, HP_CONNID dwConnID, UINT_PTR soClient)
{
    TCHAR szAddress[50];
    int iAddressLen = sizeof(szAddress) / sizeof(TCHAR);
    USHORT usPort;
    ::HP_Server_GetRemoteAddress(pSender, dwConnID, szAddress, &iAddressLen, &usPort);
    Connection connection;
    memset(&connection, 0, sizeof(connection));
    connection.dwConnID = dwConnID;
    auto it = m_sConnections.find(dwConnID);
    if (m_sConnections.end() == it)
    {
        m_sConnections.insert(std::pair<HP_CONNID, Connection>(dwConnID, connection));
    }
    FMTLOG(fmtlog::INF, "HPPackServer::OnAccept accept an new connection dwConnID:{} from {}:{}",  dwConnID, szAddress, usPort);
    return HR_OK;
}

En_HP_HandleResult __stdcall HPPackServer::OnSend(HP_Server pSender, HP_CONNID dwConnID, const BYTE *pData, int iLength)
{
    return HR_OK;
}

En_HP_HandleResult __stdcall HPPackServer::OnReceive(HP_Server pSender, HP_CONNID dwConnID, const BYTE *pData, int iLength)
{
    TCHAR szAddress[50];
    int iAddressLen = sizeof(szAddress) / sizeof(TCHAR);
    USHORT usPort;
    ::HP_Server_GetRemoteAddress(pSender, dwConnID, szAddress, &iAddressLen, &usPort);
    Message::PackMessage message;
    memcpy(&message, pData, iLength);
    FMTLOG(fmtlog::INF, "HPPackServer::OnReceive receive PackMessage, MessageType:{:#X}",  message.MessageType);
    // LoginRequest
    if (Message::EMessageType::ELoginRequest == message.MessageType)
    {
        auto it = m_sConnections.find(dwConnID);
        if (it != m_sConnections.end())
        {
            it->second.ClientType = message.LoginRequest.ClientType;
            strncpy(it->second.Account, message.LoginRequest.Account, sizeof(it->second.Account));
            FMTLOG(fmtlog::INF, "HPPackServer::OnReceive accept an new Client login from {}:{}, Account:{}",
                    szAddress, usPort, message.LoginRequest.Account);
            // EventLog
            Message::PackMessage msg;
            memset(&msg, 0, sizeof(msg));
            msg.MessageType = Message::EMessageType::EEventLog;
            msg.EventLog.Level = Message::EEventLogLevel::EINFO;
            strncpy(msg.EventLog.Account, it->second.Account, sizeof(msg.EventLog.Account));
            strncpy(msg.EventLog.App, APP_NAME, sizeof(msg.EventLog.App));
            fmt::format_to_n(msg.EventLog.Event, sizeof(msg.EventLog.Event), 
                            "HPPackServer::OnReceive accept an new Client login from {}:{}, Account:{}",
                            szAddress, usPort, message.LoginRequest.Account);
            strncpy(msg.EventLog.UpdateTime, Utils::getCurrentTimeUs(), sizeof(msg.EventLog.UpdateTime));
            while(!m_RequestMessageQueue.Push(msg));
        }
    }
    while(!m_RequestMessageQueue.Push(message));
    return HR_OK;
}

En_HP_HandleResult __stdcall HPPackServer::OnClose(HP_Server pSender, HP_CONNID dwConnID, En_HP_SocketOperation enOperation, int iErrorCode)
{
    TCHAR szAddress[50];
    int iAddressLen = sizeof(szAddress) / sizeof(TCHAR);
    USHORT usPort;
    ::HP_Server_GetRemoteAddress(pSender, dwConnID, szAddress, &iAddressLen, &usPort);

    auto it = m_sConnections.find(dwConnID);
    if (it != m_sConnections.end())
    {
        FMTLOG(fmtlog::WRN, "HPPackServer::OnClose have an connection dwConnID:{} Account:{} from {}:{} closed",  
                dwConnID, it->second.Account, szAddress, usPort);
        // EventLog
        Message::PackMessage message;
        memset(&message, 0, sizeof(message));
        message.MessageType = Message::EMessageType::EEventLog;
        message.EventLog.Level = Message::EEventLogLevel::EWARNING;
        strncpy(message.EventLog.Account, it->second.Account, sizeof(message.EventLog.Account));
        strncpy(message.EventLog.App, APP_NAME, sizeof(message.EventLog.App));
        fmt::format_to_n(message.EventLog.Event, sizeof(message.EventLog.Event), 
                        "HPPackServer::OnClose have an connection dwConnID:{} Account:{} from {}:{} closed",  
                        dwConnID, it->second.Account, szAddress, usPort);
        strncpy(message.EventLog.UpdateTime, Utils::getCurrentTimeUs(), sizeof(message.EventLog.UpdateTime));
        while(!m_RequestMessageQueue.Push(message));

        m_sConnections.erase(dwConnID);
    }
    
    return HR_OK;
}

En_HP_HandleResult __stdcall HPPackServer::OnShutdown(HP_Server pSender)
{
    return HR_OK;
}