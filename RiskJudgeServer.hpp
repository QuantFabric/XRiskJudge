#ifndef RISKJUDGESERVER_HPP
#define RISKJUDGESERVER_HPP

#include <string>
#include <stdio.h>
#include <string.h>
#include <cstdlib>
#include <mutex>
#include <unordered_map>
#include <unistd.h>
#include "PackMessage.hpp"
#include "FMTLogger.hpp"
#include "LockFreeQueue.hpp"
#include <shared_mutex>

#define APP_NAME "XRiskJudge"

#include "SHMServer.hpp"

struct ServerConf : public SHMIPC::CommonConf
{
    static const bool Publish = false;
    static const bool Performance = true;
};

class RiskJudgeServer : public SHMIPC::SHMServer<Message::PackMessage, ServerConf>
{
public:
    RiskJudgeServer():SHMServer<Message::PackMessage, ServerConf>()
    {

    }

    virtual ~RiskJudgeServer()
    {

    }

    void HandleMsg()
    {

    }
};


#endif // RISKJUDGESERVER_HPP