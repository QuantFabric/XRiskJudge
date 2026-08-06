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

    bool QueryPositionLimit(sqlite3_callback cb, std::string& errorString)
    {
        std::string SQL_SELECT_POSITION_LIMIT = "SELECT * FROM PositionLimitTable;";
        bool ret = m_DBManager->Execute(SQL_SELECT_POSITION_LIMIT, cb, "SELECT", errorString);
        if(!ret)
        {
            FMTLOG(fmtlog::WRN, "RiskDBManager::Select PositionLimitTable failed, {}", errorString);
        }
        return ret;
    }

    bool UpdatePositionLimitTable(const std::string& sql, const std::string& op, sqlite3_callback cb, std::string& errorString)
    {
        errorString.clear();
        char errorBuffer[256] = {0};
        bool ret = m_DBManager->Execute(sql, cb, op, errorString);
        if(!ret)
        {
            FMTLOG(fmtlog::WRN, "RiskDBManager::UpdatePositionLimitTable failed, ErrorMsg:{} sql:{}", errorString, sql);
            fmt::format_to_n(errorBuffer, sizeof(errorBuffer), "ErrorMsg:{} SQL:{}", errorString, sql);
        }
        else
        {
            FMTLOG(fmtlog::INF, "RiskDBManager::UpdatePositionLimitTable successed, sql:{}", sql);
            fmt::format_to_n(errorBuffer, sizeof(errorBuffer), "SQL:{}", sql);
        }
        errorString = errorBuffer;
        return ret;
    }

    bool QueryAccountLocked(sqlite3_callback cb, std::string& errorString)
    {
        std::string SQL_SELECT = "SELECT * FROM AccountLockedTable;";
        bool ret = m_DBManager->Execute(SQL_SELECT, cb, "SELECT", errorString);
        if(!ret)
        {
            FMTLOG(fmtlog::WRN, "RiskDBManager::Select AccountLockedTable failed, {}", errorString);
        }
        return ret;
    }

    bool UpdateAccountLockedTable(const std::string& sql, const std::string& op, sqlite3_callback cb, std::string& errorString)
    {
        errorString.clear();
        char errorBuffer[256] = {0};
        bool ret = m_DBManager->Execute(sql, cb, op, errorString);
        if(!ret)
        {
            FMTLOG(fmtlog::WRN, "RiskDBManager::UpdateAccountLockedTable failed, ErrorMsg:{} sql:{}", errorString, sql);
            fmt::format_to_n(errorBuffer, sizeof(errorBuffer), "ErrorMsg:{} SQL:{}", errorString, sql);
        }
        else
        {
            FMTLOG(fmtlog::INF, "RiskDBManager::UpdateAccountLockedTable successed, sql:{}", sql);
            fmt::format_to_n(errorBuffer, sizeof(errorBuffer), "SQL:{}", sql);
        }
        errorString = errorBuffer;
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