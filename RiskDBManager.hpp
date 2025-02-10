#ifndef RISKDBMANAGER_HPP
#define RISKDBMANAGER_HPP

#include <fmt/core.h>
#include "Singleton.hpp"
#include "Util.hpp"
#include "FMTLogger.hpp"
#include "YMLConfig.hpp"
#include "SQLiteManager.hpp"

class RiskDBManager
{
    friend class Utils::Singleton<RiskDBManager>;
public:
    bool LoadDataBase(const std::string& dbPath, std::string& errorString)
    {
        m_DBManager = Utils::Singleton<Utils::SQLiteManager>::GetInstance();
        bool ret = m_DBManager->LoadDataBase(dbPath, errorString);
        if(!ret)
        {
            FMTLOG(fmtlog::WRN, "RiskDBManager::LoadDataBase failed, {}", errorString);
        }
        return ret;
    }

    bool UpdateCancelledCountTable(const std::string& sql, const std::string& op, sqlite3_callback cb, std::string& errorString)
    {
        errorString.clear();
        char errorBuffer[256] = {0};
        bool ret = m_DBManager->Execute(sql, cb, op.c_str(), errorString);
        if(!ret)
        {
            fmt::format_to_n(errorBuffer, sizeof(errorBuffer), "ErrorMsg: {}, SQL: {}", errorString, sql);
            FMTLOG(fmtlog::WRN, "RiskDBManager::UpdateCancelledCountTable failed, CancelledCountTable failed, ErrorMsg:{} sql:{}",
                    errorString, sql);
        }
        else
        {
            fmt::format_to_n(errorBuffer, sizeof(errorBuffer), "SQL: {}", sql);
            FMTLOG(fmtlog::INF, "RiskDBManager::UpdateCancelledCountTable successed, sql:{}", sql);
        }
        errorString = errorBuffer;
        return ret;
    }

    bool UpdateLockedAccountTable(const std::string& sql, const std::string& op, sqlite3_callback cb, std::string& errorString)
    {
        errorString.clear();
        char errorBuffer[256] = {0};
        bool ret = m_DBManager->Execute(sql, cb, op, errorString);
        if(!ret)
        {
            FMTLOG(fmtlog::WRN, "RiskDBManager::UpdateLockedAccountTable failed, ErrorMsg:{} sql:{}", errorString, sql);
            fmt::format_to_n(errorBuffer, sizeof(errorBuffer), "ErrorMsg:{} SQL:{}", errorString, sql);
        }
        else
        {
            FMTLOG(fmtlog::INF, "RiskDBManager::UpdateLockedAccountTable successed, sql:{}", sql);
            fmt::format_to_n(errorBuffer, sizeof(errorBuffer), "SQL:{}", sql);
        }
        errorString = errorBuffer;
        return ret;
    }

    bool UpdateRiskLimitTable(const std::string& sql, const std::string& op, sqlite3_callback cb, std::string& errorString)
    {
        errorString.clear();
        char errorBuffer[256] = {0};
        bool ret = m_DBManager->Execute(sql, cb, op, errorString);
        if(!ret)
        {
            FMTLOG(fmtlog::WRN, "RiskDBManager::UpdateRiskLimitTable failed, ErrorMsg:{} sql:{}", errorString, sql);
            fmt::format_to_n(errorBuffer, sizeof(errorBuffer), "ErrorMsg:{} SQL:{}", errorString, sql);
        }
        else
        {
            FMTLOG(fmtlog::INF, "RiskDBManager::UpdateRiskLimitTable successed, sql:{}", sql);
            fmt::format_to_n(errorBuffer, sizeof(errorBuffer), "SQL:{}", sql);
        }
        errorString = errorBuffer;
        return ret;
    }

    bool QueryRiskLimit(sqlite3_callback cb, std::string& errorString)
    {
        std::string SQL_SELECT_RISK_LIMIT = "SELECT * FROM RiskLimitTable;";
        bool ret = m_DBManager->Execute(SQL_SELECT_RISK_LIMIT, cb, "SELECT", errorString);
        if(!ret)
        {
            FMTLOG(fmtlog::WRN, "RiskDBManager::Select RiskLimitTable failed, {}", errorString);
        }
        return ret;
    }

    bool QueryLockedAccount(sqlite3_callback cb, std::string& errorString)
    {
        std::string SQL_SELECT_LOCKED_ACCOUNT = "SELECT * FROM LockedAccountTable;";
        bool ret = m_DBManager->Execute(SQL_SELECT_LOCKED_ACCOUNT, cb, "SELECT", errorString);
        if(!ret)
        {
            FMTLOG(fmtlog::WRN, "RiskDBManager::Select LockedAccountTable failed, {}", errorString);
        }
        return ret;
    }

    bool QueryCancelledCount(sqlite3_callback cb, std::string& errorString)
    {
        std::string SQL_SELECT_TICKER_LIMIT = "SELECT * FROM CancelledCountTable;";
        bool ret = m_DBManager->Execute(SQL_SELECT_TICKER_LIMIT, cb, "SELECT", errorString);
        if(!ret)
        {
            FMTLOG(fmtlog::WRN, "RiskDBManager::Select CancelledCountTable failed, {}", errorString);
        }
        return ret;
    }

private:
    RiskDBManager() {}
    RiskDBManager &operator=(const RiskDBManager&);
    RiskDBManager(const RiskDBManager&);
private:
    Utils::SQLiteManager* m_DBManager;
};


#endif // RISKDBMANAGER_HPP