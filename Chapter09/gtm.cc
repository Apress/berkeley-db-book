#include "gtm.h"

static const EnvInfo ENV_INFO_ARR[] =
{
    {1, "env_1"},
    {2, "env_2"},
    {3, "env_3"}
};

static const int numEnvs_ = 3;

static int openEnv(const char *envDir, DbEnv& env);

static void errCallback (const DbEnv *env,
        const char *prefix,
        const char *errMsg)
{
    ACE_DEBUG((LM_ERROR,
                "(%P:%t) %s errMsg: %s\n", prefix, errMsg));
}

GTM::GTM():
    gtmEnv_(0)
{
    try
    {
        openEnv("gtm_env", gtmEnv_);
        openGTMDb();
        openSequence();
    }
    catch(DbException &dbex)
    {
        ACE_DEBUG((LM_ERROR,
                    "Error opening GTM %d\n",
                    dbex.get_errno()));
    }
}

GTM::~GTM()
{
    close();
}

void GTM::close()
{
    try
    {
        gtmDb_->close(0);
        gtmEnv_.close(0);
    }
    catch(...)
    {
        ACE_DEBUG((LM_ERROR,
                    "error closing GTM\n"));
    }
}

int openEnv(const char *envDir,
        DbEnv& env)
{
    try
    {
        env.set_errpfx(envDir);
        env.set_errcall(errCallback);
        env.set_lk_detect(DB_LOCK_DEFAULT);
        u_int32_t envFlags =
            DB_INIT_MPOOL | DB_INIT_TXN |
            DB_INIT_LOCK | DB_THREAD |
            DB_INIT_LOG | DB_RECOVER | 
            DB_CREATE;
        env.open(envDir, envFlags, 0);
    }
    catch(DbException &dbex)
    {
        ACE_DEBUG((LM_ERROR,
                    "Error opening env %s : %d\n",
                    envDir, dbex.get_errno()));
        throw;
    }
    return 0;
}

int GTM::openLocalEnvs()
{
    for(int i=0; i < numEnvs_; i++)
    {
        LocalTxnMgr_sp localMgr;
        localMgr.reset(new LocalTxnMgr(ENV_INFO_ARR[i].envDir, 
                    ENV_INFO_ARR[i].envId));
        envMap_[ENV_INFO_ARR[i].envId] = localMgr;
    }
    return 0;
}

int GTM::openGTMDb()
{
    gtmDb_ = new Db(&gtmEnv_, 0);
    DbTxn *txn;
    try
    {
        gtmEnv_.txn_begin(NULL, &txn, 0);
        gtmDb_->open(txn, "GTM_Db", NULL,
                DB_BTREE, DB_CREATE | DB_THREAD, 0644);
        txn->commit(0);
    }
    catch(DbException &dbex)
    {
        txn->abort();
        gtmEnv_.err(dbex.get_errno(), "GTM::openGTMDb");
        throw;
    }
    return 0;
}

int GTM::openSequence()
{
    DbTxn *txn = NULL;
    int err = 0;
    bool retry = true;
    bool create = false;
    u_int32_t flags = DB_EXCL | DB_THREAD;

    seqDb_ = new DbSequence(gtmDb_, 0);
    std::string seqKey = "XID Sequence";

    while(retry == true)
    {
        try
        {
            Dbt key(((void*)seqKey.c_str()), seqKey.size());
            if(create)
            {
                ACE_DEBUG((LM_ERROR,
                            "Sequence not yet initialied\n"));
                seqDb_->initial_value(1000000);
            }
            gtmEnv_.txn_begin(NULL, &txn, 0);
            seqDb_->set_flags(DB_SEQ_INC | DB_SEQ_WRAP);
            seqDb_->open(txn, &key, flags); 
            txn->commit(0);
            retry = false;
        }
        catch(DbException &dbex)
        {
            txn->abort();
            err = dbex.get_errno();
            gtmEnv_.err(dbex.get_errno(), 
                    "GTM::openSequence");
            if(err != DB_NOTFOUND)
                throw;
            flags |= DB_CREATE;
            create = true;
            retry = true;
        }
    }
    return 0;
}

int GTM::getNextSeq()
{
    int ret = -1;
    DbTxn *txn = NULL;
    try
    {
        gtmEnv_.txn_begin(NULL, &txn, 0);
        seqDb_->get(txn, 1, &nextSeq_, 0);
        txn->commit(0);
        std::cout << "next seq is " << nextSeq_ << std::endl;
        ret = 0;
    }
    catch(DbException &dbex)
    {
        txn->abort();
        gtmEnv_.err(dbex.get_errno(), 
                "GTM::getNextSeq");
    }
    return ret;
}

int GTM::getNextGid(u_int8_t *gid)
{
    GTxnEntry entry;
    getNextSeq();
    memset(gid, 0, DB_XIDDATASIZE);
    memcpy(gid, &nextSeq_, sizeof(nextSeq_));
    return 0;
}

LocalTxnMgr::LocalTxnMgr(const char *envDir, int id):
    env_(0),
    envId_(id)
{
    ACE_DEBUG((LM_ERROR, "opening env %d\n", id));
    openEnv(envDir, env_);
    ACE_DEBUG((LM_ERROR, 
                "recovering global txns for env %d\n", id));
    recoverGTxn();
    openDbs();
}

LocalTxnMgr::~LocalTxnMgr()
{
    close();
}

void LocalTxnMgr::close()
{
    try
    {
        personDb_->close(0);
        env_.close(0);
    }
    catch(...)
    {
        ACE_DEBUG((LM_ERROR,
                    "error closing LocalTxnMgr\n"));
    }
}
int LocalTxnMgr::openDbs()
{
    personDb_ = new Db(&env_, 0);
    DbTxn *txn;
    try
    {
        env_.txn_begin(NULL, &txn, 0);
        personDb_->open(txn, "Person_Db", NULL,
                DB_BTREE, DB_CREATE | DB_THREAD, 0644);
        txn->commit(0);
    }
    catch(DbException &dbex)
    {
        txn->abort();
        env_.err(dbex.get_errno(), "LocalTxnMgr::openDbs");
        throw;
    }
    return 0;
}

int LocalTxnMgr::addGTxn(u_int8_t *xid)
{
    DbTxn *txn;
    env_.txn_begin(NULL, &txn, 0);
    txnMap_[xid] = txn;
    return 0;
}

LocalTxnHndls_sp 
LocalTxnMgr::getLocalTxnHandles(u_int8_t *xid)
{
    LocalTxnHndls_sp handles;
    handles.reset(new LocalTxnHndls);
    handles->db = personDb_;
    handles->txn = NULL;
    TxnMapIter it = txnMap_.find(xid);
    if(it != txnMap_.end())
    {
        handles->txn = it->second;
        handles->env = &env_;
        handles->envId = envId_;
    }
    return handles;
}

int LocalTxnMgr::processTxn(u_int8_t *xid, TXN_ACTION action)
{
    int ret = -1;
    TxnMapIter it = txnMap_.find(xid);
    if(it != txnMap_.end())
    {
        DbTxn *txn = it->second;
        try
        {
            switch(action)
            {
                case PREPARE:
                    ret = txn->prepare(xid);
                    break;
                case COMMIT:
                    ret = txn->commit(0);
                    break;
                case ABORT:
                    ret = txn->abort();
                    break;
                default:
                    break;
            }
        }
        catch(DbException& ex)
        {
            txn->abort();
            env_.err(ex.get_errno(), 
                    "LocalTxnMgr::processTxn");
        }
    }
    return ret;
}

int LocalTxnMgr::recoverGTxn()
{
    int res;
    int askedCount = 5;
    long retCount;
    DbPreplist prepList[askedCount];
    u_int32_t flags = DB_FIRST;
    

    while(retCount >= askedCount)
    {
        try
        {
            res = env_.txn_recover(prepList, 
                    askedCount, &retCount, flags); 
            for(int i=0; i < retCount; i++)
            {
                GTxnEntry entry;
                entry.getByXid(prepList[i].gid, NULL);
                ACE_DEBUG((LM_ERROR, "resolving\n"));
                ACE_HEX_DUMP((LM_ERROR, 
                            (const char*)(prepList[i].gid), 
                            DB_XIDDATASIZE));
                TxnStatus status = entry.getStatus();
                ACE_DEBUG((LM_ERROR, "txn status is %d\n",
                            status));
                switch(status)
                {
                    case COMMITTING:
                        (prepList[i].txn)->commit(0);
                        ACE_DEBUG((LM_ERROR, 
                                    "txn commited\n"));
                        break;
                    default:
                        (prepList[i].txn)->abort();
                        ACE_DEBUG((LM_ERROR, 
                                    "txn aborted\n"));
                        break;
                }
            }
            flags = DB_NEXT;
        }
        catch(DbException& ex)
        {
            env_.err(ex.get_errno(), 
                    "LocalTxnMgr::recoverGTxn");
        }
    }
    return res;
}

GTxn::GTxn():
    begin_(false)
{
    std::cout << "creating new distributed Txn " << std::endl;
    GTM *gtm = GTMSingleton::instance();
    gtm->getNextGid(xid_);
    gTxnEntry_.reset(new GTxnEntry(xid_));
}

int GTxn::addEnv(int envId)
{
    if(begin_)
        throw std::exception();

    int ret = -1;
    GTM *gtm = GTMSingleton::instance();
    EnvMapIter it = (gtm->envMap_).find(envId);
    if(it != (gtm->envMap_).end())
    {
        LocalTxnMgr_sp mgr = it->second;
        ACE_DEBUG((LM_ERROR,
                    "GTxn::addEnv: added env ID %d\n",
                    envId));
        mgr->addGTxn(xid_);
        envList_.push_back(envId);
        ret = 0;
    }
    return ret;
}

int GTxn::begin()
{
    begin_ = true;
}

int GTxn::getLocalHandles(LocalHandles& handles)
{
    GTM *gtm = GTMSingleton::instance();
    GTxnEnvListIter beg = envList_.begin();
    GTxnEnvListIter end = envList_.end();
    for(; beg != end; ++beg)
    {
        int envId = (*beg);
        EnvMapIter it = (gtm->envMap_).find(envId);
        if(it != (gtm->envMap_).end())
        {
            handles.push_back(
                    (it->second)->getLocalTxnHandles(xid_));
        }
    }
    return 0;
}

int GTxn::commit()
{
    gTxnEntry_->update(PRE_PREPARE);
    int result = processTxn(PREPARE);
    if(result)
    {
        abort();
        std::cout << "txn aborted" << std::endl;
        return -1;
    }
    gTxnEntry_->update(COMMITTING);
    result = processTxn(COMMIT);
    std::cout << "txn committed" << std::endl;
    if(!result)
    {
        gTxnEntry_->update(DONE);
    }
    return 0;
}

int GTxn::abort()
{
    gTxnEntry_->update(ABORTING);
    int result = processTxn(ABORT);
    if(!result)
    {
        gTxnEntry_->update(DONE);
    }
}

int GTxn::processTxn(TXN_ACTION action)
{
    int result = -1;

    GTM *gtm = GTMSingleton::instance();
    GTxnEnvListIter beg = envList_.begin();
    GTxnEnvListIter end = envList_.end();
    for(; beg != end; ++beg)
    {
        int envId = (*beg);
        if(gtm->nextSeq_ == 1000005 && envId == 2)
        {
            exit(0);
            //return -1;
        }
        EnvMapIter it = (gtm->envMap_).find(envId);
        if(it != (gtm->envMap_).end())
        {
            result = (it->second)->processTxn(xid_, action);
            if(result)
                return result;
        }
    }
    return result;
}
