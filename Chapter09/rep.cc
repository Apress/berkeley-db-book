#include <ace/Connector.h>
#include <ace/SOCK_Connector.h>

#include "rep.h"
#include "worker.h"

static const RepInfo REP_INFO_ARR[] =
{
    {1, "127.0.0.1", 3000, 1},
    {2, "127.0.0.1", 4000, 2},
    {3, "127.0.0.1", 5000, 3},
};
static const int NUM_REPS = 3;


static int nextId = 1;


Replica::Replica(ACE_INET_Addr addr, int id, int priority):
    addr_(addr),
    id_(id),
    sess_(NULL),
    priority_(priority)
{
}

Replica::~Replica()
{
}


RepTask::RepTask()
{
}

RepTask::~RepTask()
{
}

int RepTask::svc()
{
    ACE_Reactor::instance()->run_reactor_event_loop();
}

RepMgr::RepMgr():
    masterId_(DB_EID_INVALID)
{
}

RepMgr::~RepMgr()
{
}

int RepMgr::handle_timeout(const ACE_Time_Value &currTime,
                               const void *act)
{
    ACE_READ_GUARD_RETURN(ACE_RW_Thread_Mutex, 
            g, mapMutex_, -1);
    RepMapIter beg = repMap_.begin();
    RepMapIter end = repMap_.end();
    for(;beg != end; ++beg)
    {
        Replica_SP r = (*beg).second;
        if(r->id_ != myId_ && r->sess_ == NULL)
        {
            connect(r);
        }
    }
}

int RepMgr::beginRep(int myId)
{
    myId_ = myId;
    ACE_Reactor r (&reactor_);
    ACE_Reactor::instance(&r);

    ACE_Reactor::instance()->schedule_timer(
            this, 0, ACE_Time_Value::zero, 2);

    tasks_.activate(THR_NEW_LWP | THR_JOINABLE, 2);

    //configure replication
    DbUtils_SP utils = DbUtils::getInstance();
    DbEnv *env = utils->getEnv();
    //Setup the communication framework
    {
        ACE_WRITE_GUARD_RETURN(ACE_RW_Thread_Mutex, 
                g, mapMutex_, -1);
        for(int i=0; i < NUM_REPS; i++)
        {
            RepInfo ri = REP_INFO_ARR[i];
            Replica_SP replica;
            ACE_INET_Addr addr(ri.port, ri.ip);
            replica.reset(new Replica(addr, 
                        ri.id, ri.priority));
            repMap_[ri.id] = replica;
            if(ri.id == myId_)
                myReplica_ = replica;
        }
    }
    cData_.set_data(&myId_);
    cData_.set_size(sizeof(myId_));
    if(listener_.open(myReplica_->addr_) == -1)
    {
        ACE_DEBUG((LM_ERROR,
                    "(%t)beginRep: listener error on %s %d\n",
                    myReplica_->addr_.get_host_addr(),
                    myReplica_->addr_.get_port_number()));
        return -1;
    }
    try
    {
        env->set_verbose(DB_VERB_REPLICATION, 1);
        env->set_rep_transport(myId_, repSend);
        env->rep_start(&cData_, DB_REP_CLIENT);
        startElec();
    }
    catch(DbException& dbex)
    {
        env->err(dbex.get_errno(), "beginRep");
    }

    while(!utils->initDbDone_)
    {
        utils->loadDbs(isMaster() ? DB_REP_MASTER : DB_REP_CLIENT); 
        ACE_OS::sleep(1);
        ACE_DEBUG((LM_ERROR, "waiting for Dbs to init\n"));
    }
    DbWorker_SP r1(new DbWorker( READER, 2));
    if(r1->activate((THR_NEW_LWP | THR_JOINABLE), 1) != 0)
        ACE_DEBUG((LM_ERROR, "Could not launch r1\n"));

    ACE_Thread_Manager *tm = 
        ACE_Thread_Manager::instance();
    tm->wait();
    return 0;
}

bool RepMgr::isMaster()
{
    ACE_WRITE_GUARD_RETURN(ACE_Recursive_Thread_Mutex, 
            g, masterMutex_, -1);
    return ( (masterId_ == myId_) ? true : false );
}

int RepMgr::setMaster(int envId)
{
    ACE_WRITE_GUARD_RETURN(ACE_Recursive_Thread_Mutex, 
            g, masterMutex_, -1);
    if(envId == myId_ && !isMaster())
        beginRole(DB_REP_MASTER);
    else //if(envId != myId_ && isMaster())
        beginRole(DB_REP_CLIENT);
    masterId_ = envId;
    return 0;
}

int RepMgr::startElec()
{
    DbUtils_SP utils = DbUtils::getInstance();
    DbEnv *env = utils->getEnv();

    int master = DB_EID_INVALID;
    int priority;
    int votes;
    if(myId_ == 1)
    {
        priority = 100;
        votes = 1;
    }
    else
    {
        priority = myReplica_->priority_;
        votes = NUM_REPS/2 + 1;
    }

    while(master == DB_EID_INVALID)
    {
        try
        {
            env->rep_elect(NUM_REPS, votes,
                           priority, 100, &master, 0);
            ACE_DEBUG((LM_ERROR, "new master is %d\n",
                                 master));
            setMaster(master);
            break;
        }
        catch(DbException& dbex)
        {
            env->err(dbex.get_errno(), "startElec");
        }
        ACE_DEBUG((LM_ERROR, 
            "startElec: unable to elect a master\n"));
        ACE_OS::sleep(2);
    }
    return 0;
}

int RepMgr::beginRole(int role)
{
    DbUtils_SP utils = DbUtils::getInstance();
    DbEnv *env = utils->getEnv();
    try
    {
        ACE_WRITE_GUARD_RETURN(
                ACE_Recursive_Thread_Mutex, 
                g, masterMutex_, -1);
        env->rep_start(&cData_, role);
        if((isMaster() && (role != DB_REP_MASTER)) ||
                (!isMaster() && (role == DB_REP_MASTER))) 
        {
            utils->loadDbs(role);
            ACE_DEBUG((LM_ERROR,"(%t) loaded Dbs\n"));
        }
    }
    catch(DbException& dbex)
    {
        env->err(dbex.get_errno(), "beginRole");
    }
    ACE_DEBUG((LM_ERROR,"(%t) done beginRole\n"));
}

int RepMgr::connect(Replica_SP r)
{
    ACE_WRITE_GUARD_RETURN(ACE_Recursive_Thread_Mutex, 
            g, r->mutex_ , -1);
    ACE_DEBUG((LM_ERROR, "connect:: id %d  %s  %d\n",
                r->id_,
                r->addr_.get_host_addr(), 
                r->addr_.get_port_number()));
    ACE_Connector < RepSession, ACE_SOCK_CONNECTOR > 
        connector;
    RepSession *s = NULL;
    int res = connector.connect(s, r->addr_);

    ACE_DEBUG((LM_ERROR, "connect::res is %d\n", res));
    if(res == -1)
    {
        ACE_DEBUG((LM_ERROR, 
                    "(%t) error connecting to %s %d\n",
                    r->addr_.get_host_addr(), 
                    r->addr_.get_port_number()));
    }
    else
    {
        //send the first message
        r->sess_ = s;
        sendFirstMsg(r->id_);
    }
    return res;
}

int RepMgr::sendFirstMsg(int repId)
{
    send(NULL, NULL, NULL, repId, 0, FIRST_MSG);
}

int RepMgr::send(const Dbt *cntl,
                 const Dbt *rec,
                 const DbLsn *lsn,
                 int    eid,
                 u_int32_t flags,
                 int msgId)
{
    //encode the message to be sent
    RepMsg m;
    int cntlLen = 0;
    int recLen = 0;
    memset(&m, 0, sizeof(m));
    m.msgId = msgId;

    if(msgId == FIRST_MSG)
    {
        m.envId = myId_;
        m.cntlLen = 0;
        m.recLen = 0;
    }
    else
    {
        if( flags & DB_REP_PERMANENT && lsn != NULL)
        {
            m.lsn.file = lsn->file;
            m.lsn.offset = lsn->offset;
        }
        m.flags = flags;

        if(cntl != NULL)
            m.cntlLen = cntl->get_size();

        if(rec != NULL)
            m.recLen = rec->get_size();

        cntlLen = m.cntlLen;
        recLen = m.recLen;

    }

    int size = sizeof(m) + cntlLen + recLen;
    ACE_DEBUG((LM_ERROR, 
    "(%t)send: msgId %d eid %d cntlLen %d, recLen %d\n", 
    msgId, eid, cntlLen, recLen)); 
    char sendBuf[size];

    char *offset = sendBuf;
    memcpy(offset, &m, sizeof(m));
    offset += sizeof(m);
    if(cntl != NULL)
    {
        memcpy(offset, cntl->get_data(), m.cntlLen);
        offset += m.cntlLen;
    }
    if(rec != NULL)
    {
        memcpy(offset, rec->get_data(), m.recLen);
    }

    //Create a message block to enqueue
    ACE_Message_Block *mb = 
        new ACE_Message_Block(size);
    std::memcpy(mb->wr_ptr(), sendBuf, size);
    mb->wr_ptr(size);

    ACE_READ_GUARD_RETURN(ACE_RW_Thread_Mutex, 
            g, mapMutex_, -1);
    if(eid == DB_EID_BROADCAST)
    {
        RepMapIter beg = repMap_.begin();
        RepMapIter end = repMap_.end();
        for(; beg != end; ++beg)
        {
            Replica_SP r = (*beg).second;
            if(r->id_ == myId_)
                continue;
            if(r->sess_ != NULL)
            {
                assert(r->sess_->msg_queue());
                r->sess_->putq(mb->duplicate());
                ACE_DEBUG((LM_ERROR,
                     "repSend: added to send queue of %d\n",
                     r->id_));
            }
            else
            {
                ACE_DEBUG((LM_ERROR,
                      "repSend: no conn availbale for %d\n",
                      r->id_));
            }
        }
    }
    else
    {
        RepMapIter it = repMap_.find(eid);
        if(it != repMap_.end())
        {
            Replica_SP r = (*it).second;
            if(r->sess_ != NULL)
            {
                r->sess_->putq(mb);
                ACE_DEBUG((LM_ERROR,
                       "repSend: added to send queue of %d\n",
                       r->id_));
            }
            else
            {
                ACE_DEBUG((LM_ERROR,
                       "repSend: no conn availbale for %d\n",
                       eid));
            }
        }
    }
    return 0;
}

RepSession::RepSession():
    envId_(DB_EID_INVALID),
    notifier_(0,
              this, 
              ACE_Event_Handler::WRITE_MASK)
{
}

int RepSession::open(void *p)
{
    if(super::open(p) == -1)
        return -1;
    this->notifier_.reactor(this->reactor());
    this->msg_queue()->notification_strategy(
            &this->notifier_);

    //store the session in the map
    RepMgr *mgr = RepMgrSingleton::instance();

    ACE_INET_Addr remAddr;
    int res = peer().get_remote_addr(remAddr);
    ACE_DEBUG((LM_ERROR, 
                "(%t) open: connected to %s %d\n",
                remAddr.get_host_addr(), 
                remAddr.get_port_number()));
    ACE_READ_GUARD_RETURN(ACE_RW_Thread_Mutex, 
            g, mgr->mapMutex_, -1);
    RepMapIter beg = mgr->repMap_.begin();
    RepMapIter end = mgr->repMap_.end();
    for(;beg != end; ++beg)
    {
        Replica_SP r = (*beg).second;
        if( res == 0 && r->addr_ == remAddr)
        {
            envId_ = r->id_;
            ACE_DEBUG((LM_ERROR, "(%t)OPEN:: connected to %d\n",
                        r->id_));
            r->sess_ = this;
            break;
        }
    }
    return 0;
}

int RepSession::handle_input(ACE_HANDLE fd)
{
    ssize_t count = -1;
    RepMsg m;
    int res;

    count = this->peer().recv_n(&m, sizeof(m));
    if(count > 0)
    {
        if(m.msgId == FIRST_MSG)
        {
            envId_ = m.envId;
            ACE_DEBUG((LM_ERROR,
            "(%t)handle_input:OPEN::received first msg from %d\n",
            m.envId));
            RepMgr *mgr = RepMgrSingleton::instance();
            ACE_READ_GUARD_RETURN(ACE_RW_Thread_Mutex, 
                    g, mgr->mapMutex_, -1);
            RepMapIter it = mgr->repMap_.find(m.envId);
            if(it != mgr->repMap_.end())
            {
                Replica_SP r = (*it).second;
                r->sess_ = this;
                ACE_DEBUG((LM_ERROR, 
                            "handle_input: added %d to map\n",
                            m.envId));
            }
            res = 1;
        }
        else
        {
            char cntlBuf[m.cntlLen];
            char recBuf[m.recLen];
            ACE_DEBUG((LM_ERROR, 
        "(%t)handle_input: rep id %d:recLen %d cntlLen %d\n", 
            envId_, m.recLen, m.cntlLen)); 

            if(m.cntlLen > 0)
                count = this->peer().recv_n(cntlBuf, 
                        m.cntlLen);
            if(count > 0)
            {
                if(m.recLen > 0)
                    count = this->peer().recv_n(recBuf, 
                            m.recLen);
                if(count > 0)
                {

                    Dbt rec(recBuf, m.recLen);
                    Dbt cntl(cntlBuf, m.cntlLen);
                    DbLsn retLsn;
                    res = processMsg(&cntl, &rec, 
                            &envId_, &retLsn);

                    return 0;
                }
            }
        }
    }
    if(count == 0 || ACE_OS::last_error() != EWOULDBLOCK)
    {
        ACE_DEBUG((LM_ERROR, 
           "(%t)handle_input: rep id %d: conn closed\n",
           envId_));
        return -1;
    }
    return 0;
}

int RepSession::handle_output(ACE_HANDLE fd)
{
    ACE_Message_Block *mb;
    ACE_Time_Value noWait (ACE_OS::gettimeofday());
    while(this->getq(mb, &noWait) != -1)
    {
        ssize_t sendCnt = 
            this->peer().send(mb->rd_ptr(), mb->length());
        if(sendCnt == -1)
            ACE_DEBUG((LM_ERROR, 
                        "(%t)rep id %d: output error\n",
                        envId_));
        else
            mb->rd_ptr(ACE_static_cast(size_t, sendCnt));
        if(mb->length() > 0)
        {
            ungetq(mb);
            break;
        }
        ACE_DEBUG((LM_ERROR, "(%t)rep id %d: %d bytes sent\n",
                    envId_, sendCnt));
        mb->release();
    }
    if(this->msg_queue()->is_empty())
        this->reactor()->cancel_wakeup(this, 
                ACE_Event_Handler::WRITE_MASK);
    else
        this->reactor()->schedule_wakeup(this, 
                ACE_Event_Handler::WRITE_MASK);
    return 0;
}

int RepSession::handle_close(ACE_HANDLE fd,
                         ACE_Reactor_Mask mask)
{
    ACE_DEBUG((LM_ERROR, "(%t)rep id %d:connection closed\n",
                envId_));
    RepMgr *mgr = RepMgrSingleton::instance();
    ACE_READ_GUARD_RETURN(ACE_RW_Thread_Mutex, 
            g, mgr->mapMutex_, -1);
    RepMapIter it = mgr->repMap_.find(envId_);
    if(envId_ != DB_EID_INVALID && it != mgr->repMap_.end())
    {
        Replica_SP r = (*it).second;
        ACE_WRITE_GUARD_RETURN(ACE_Recursive_Thread_Mutex, 
                g, r->mutex_ , -1);
        r->sess_ = NULL;
        ACE_DEBUG((LM_ERROR, 
                    "handle_close: sess for %d removed map\n",
                    envId_));
        if(envId_ == mgr->masterId_)
        {
            ACE_DEBUG((LM_ERROR, 
        "(%t)master disconnected, starting election\n"));
            mgr->startElec();
        }
    }
    return super::handle_close(fd, mask);
}
int RepSession::processMsg(Dbt *control, 
        Dbt *rec, int *envId, 
        DbLsn *retLsn)
{
    DbEnv *env = DbUtils::getEnv();
    RepMgr *mgr = RepMgrSingleton::instance();
    int res;
    try
    {
        ACE_DEBUG((LM_ERROR, "processing msg from %d\n", 
                    *envId));
        res = env->rep_process_message(control, rec,
                envId, retLsn);
    }
    catch(DbException& dbex)
    {
        env->err(dbex.get_errno(), "processMsg");
        return -1;
    }
    switch(res)
    {
        case 0:
            break;
        case DB_REP_NEWSITE:
            {
                int *repId = (int*)(rec->get_data());
                ACE_DEBUG((LM_ERROR,
     "(%t)processMsg:received DB_REP_NEWSITE from %d\n",
                *repId));
            }
            break;
        case DB_REP_HOLDELECTION:
            mgr->startElec();
            break;
        case DB_REP_NEWMASTER:
            mgr->setMaster(*envId);
            ACE_DEBUG((LM_ERROR,
                        "(%t) New master is %d\n",
                        *envId));
            break;
        case DB_REP_ISPERM:
            ACE_DEBUG((LM_ERROR,
             "(%t)ISPERM returned for LSN[%d %d]\n",
             retLsn->file, retLsn->offset));
            break;
        case DB_REP_NOTPERM:
            ACE_DEBUG((LM_ERROR,
            "(%t)NOTPERM returned for LSN[%d %d]\n",
            retLsn->file, retLsn->offset));
            break;
        case DB_REP_DUPMASTER:
            ACE_DEBUG((LM_ERROR,
   "(%t)DUPMASTER received, launching election\n"));
            mgr->beginRole(DB_REP_CLIENT);
            mgr->startElec();
            break;
        case DB_REP_IGNORE:
            ACE_DEBUG((LM_ERROR,
                  "(%t)REP_IGNORE received\n"));
            break;
        case DB_REP_JOIN_FAILURE:
            ACE_DEBUG((LM_ERROR,
                   "(%t)JOIN_FAILURE received\n"));
            break;
        case DB_REP_STARTUPDONE:
            ACE_DEBUG((LM_ERROR,
                   "(%t)STARTUPDONE received\n"));
            break;
        default:
            ACE_DEBUG((LM_ERROR,
             "processMsg: unknown return code %d\n",
                        res));
            break;
    }
    return 0;
}

int repSend(DbEnv *env,
        const Dbt *cntl,
        const Dbt *rec,
        const DbLsn *lsn,
        int    eid,
        u_int32_t flags)
{
    RepMgr *mgr = RepMgrSingleton::instance();
    return mgr->send(cntl, rec, lsn, eid, flags, REP_MSG);
}

