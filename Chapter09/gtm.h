#ifndef _GTM_H
#define _GTM_H_

#include <ace/Recursive_Thread_Mutex.h>
#include <ace/Singleton.h>
#include <boost/shared_ptr.hpp>
#include <list>

#include "dbentry.h"

#define NEXT_GID_KEY 1
#define INVALID_ENV_ID -1

struct EnvInfo
{
    int envId;
    const char *envDir;
};

struct XidCompare
{
    bool operator() (const u_int8_t* xid1,
            const u_int8_t* xid2) const
    {
        for(int i = 0; i < DB_XIDDATASIZE; i++)
        {
            return (xid1[i] < xid2[i]) ? true : false;
        }
    }
};

struct LocalTxnHndls
{
    int envId;
    Db *db;
    DbTxn *txn;
    DbEnv *env;
};

typedef boost::shared_ptr<LocalTxnHndls> LocalTxnHndls_sp;
typedef std::list<LocalTxnHndls_sp> LocalHandles;
typedef std::list<LocalTxnHndls_sp>::iterator LocalHandlesIter;

typedef std::map< u_int8_t*, DbTxn*, XidCompare > TxnMap;
typedef std::map< u_int8_t*, DbTxn*, XidCompare >::iterator TxnMapIter;

enum TXN_ACTION
{
    PREPARE,
    COMMIT,
    ABORT
};

class GTM;
class LocalTxnMgr
{
    public:
        LocalTxnMgr(const char* envDir, int envId);
        ~LocalTxnMgr();

        int addGTxn(u_int8_t *xid);
        LocalTxnHndls_sp getLocalTxnHandles(u_int8_t *xid);
        int processTxn(u_int8_t *xid, TXN_ACTION action );
    private:
        void close();
        int openDbs();
        int recoverGTxn();

        DbEnv env_;
        int envId_;
        TxnMap txnMap_; 
        Db *personDb_;

        friend class GTM;
        friend class GTxn;
};

typedef boost::shared_ptr<LocalTxnMgr> LocalTxnMgr_sp;

typedef std::map< int, LocalTxnMgr_sp > EnvMap;
typedef std::map< int, LocalTxnMgr_sp >::iterator EnvMapIter;

typedef std::list<int> GTxnEnvList;
typedef std::list<int>::iterator GTxnEnvListIter;
class GTxn
{
    public:
        GTxn();
        int begin();
        int addEnv(int envId);
        int commit();
        int abort();
        int getLocalHandles(LocalHandles& handles);
    private:
        u_int8_t    xid_[128];
        GTxnEnvList envList_;
        GTxnEntry_sp   gTxnEntry_;
        bool        begin_;
        int processTxn(TXN_ACTION action);
};

class GTM
{
    public:
        friend class
            ACE_Singleton<GTM, ACE_Recursive_Thread_Mutex>;
        int openLocalEnvs();
    private:
        GTM();
        ~GTM();

        void close();
        int openGTMDb();
        int openSequence();
        int getNextSeq();
        int getNextGid(u_int8_t *gid);

        EnvMap envMap_;
        DbEnv gtmEnv_;
        Db *gtmDb_;
        DbSequence *seqDb_;
        db_seq_t nextSeq_;
        static u_int32_t gid_;
        friend class GTxn;
        friend class GTxnEntry;
};
typedef ACE_Singleton<GTM, ACE_Recursive_Thread_Mutex>
GTMSingleton;

#endif // _GTM_H_
