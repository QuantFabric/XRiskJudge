#include "RiskEngine.h"

extern Utils::Logger* gLogger;

Utils::RingBuffer<Message::PackMessage> RiskEngine::m_RiskResponseQueue(1 << 10);

RiskEngine::RiskEngine()
{
    m_HPPackServer = NULL;
    m_HPPackClient = NULL;
    m_RequestThread = NULL;
    m_ResponseThread = NULL;
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
    strncpy(login.Account, "XRiskJudge", sizeof(login.Account));
    m_HPPackClient->Login(login);
}

void RiskEngine::HandleRequestFunc()
{
    Utils::gLogger->Log->info("RiskEngine::HandleRequestFunc Risk Service {} Running", m_XRiskJudgeConfig.RiskID);
    while (true)
    {

    }
}

void RiskEngine::HandleResponseFunc()
{
    Utils::gLogger->Log->info("RiskEngine::HandleResponseFunc Response Message Handling");
    while (true)
    {

    }
}

