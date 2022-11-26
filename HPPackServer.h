#ifndef HPPACKSERVER_H
#define HPPACKSERVER_H

#include <string>
#include <stdio.h>
#include <string.h>
#include <cstdlib>
#include <mutex>
#include <unordered_map>
#include <unistd.h>
#include <sys/types.h>         
#include <sys/socket.h>
#include <netinet/tcp.h>
#include <netinet/in.h>
#include "HPSocket4C.h"
#include "PackMessage.hpp"
#include "Logger.h"
#include "LockFreeQueue.hpp"
#include "phmap.h"
#include <shared_mutex>

#define APP_NAME "XRiskJudge"

struct Connection
{
    HP_CONNID dwConnID;
    int ClientType;
    char Account[16];
};

class HPPackServer
{
public:
    HPPackServer(const char *ip, unsigned int port);
    virtual ~HPPackServer();
    void Start();
    void Stop();
    void SendData(HP_CONNID dwConnID, const unsigned char *pBuffer, int iLength);
public:
    typedef phmap::parallel_flat_hash_map<HP_CONNID, Connection, phmap::priv::hash_default_hash<HP_CONNID>,
                                     phmap::priv::hash_default_eq<HP_CONNID>,
                                     std::allocator<std::pair<const HP_CONNID, Connection>>, 8, std::shared_mutex>
    ConnectionMapT;
    static ConnectionMapT m_sConnections;
    static Utils::LockFreeQueue<Message::PackMessage> m_RequestMessageQueue;
protected:
    static En_HP_HandleResult __stdcall OnPrepareListen(HP_Server pSender, UINT_PTR soListen);
    static En_HP_HandleResult __stdcall OnAccept(HP_Server pSender, HP_CONNID dwConnID, UINT_PTR soClient);
    static En_HP_HandleResult __stdcall OnSend(HP_Server pSender, HP_CONNID dwConnID, const BYTE *pData, int iLength);
    static En_HP_HandleResult __stdcall OnReceive(HP_Server pSender, HP_CONNID dwConnID, const BYTE *pData, int iLength);
    static En_HP_HandleResult __stdcall OnClose(HP_Server pSender, HP_CONNID dwConnID, En_HP_SocketOperation enOperation, int iErrorCode);
    static En_HP_HandleResult __stdcall OnShutdown(HP_Server pSender);
private:
    std::string m_ServerIP;
    int m_ServerPort;
    HP_TcpServer m_pServer;
    HP_TcpServerListener m_pListener;
};

#endif // HPPACKSERVER_H