#ifndef _REP_H_
#define _REP_H_

#include "load_db.h"

#include <ace/INET_Addr.h>
#include <ace/TP_Reactor.h>
#include <ace/Acceptor.h>
#include <ace/SOCK_Acceptor.h>
#include <ace/SOCK_Stream.h>
#include <ace/SOCK_Stream.h>
#include <ace/Message_Block.h>
#include <ace/Svc_Handler.h>
#include <ace/Reactor_Notification_Strategy.h>

#define FIRST_MSG 1
#define REP_MSG 2

class RepMgr;
class RepSession;

struct RepMsg
{
    int         msgId;
    DB_LSN      lsn;
    u_int32_t   flags;
    int         envId;
    int         cntlLen;
    int         recLen;
};


#if 0
class RepMsg
{
    public:
        RepMsg(Dbt *cntl, Dbt *rec);
        RepMsg(char *msgBuf);
        const char* getBuf();

    private:
        Dbt *cntl_;
        Dbt *rec_;
};
#endif

struct RepInfo
{
    int             id;
    const char*     ip;
    unsigned short  port;
    int             priority;
};
        
class Replica
{
    public:
        Replica(ACE_INET_Addr addr, int id, int priority);
        ~Replica();

    private:
        int                 id_;
        int                 priority_;
        ACE_INET_Addr       addr_;
        RepSession          *sess_;
        DbLsn               sentLsn_;
        DbLsn               retLsn_;
        ACE_Recursive_Thread_Mutex mutex_;

        friend class RepMgr;
        friend class RepSession;
};

typedef boost::shared_ptr<Replica> Replica_SP; 
typedef std::map< int, Replica_SP > RepMap;
typedef std::map< int, Replica_SP >::iterator RepMapIter;

class RepSession:
    public ACE_Svc_Handler<ACE_SOCK_STREAM, ACE_MT_SYNCH>
{
    typedef ACE_Svc_Handler<ACE_SOCK_STREAM, ACE_MT_SYNCH> 
        super;

public:
    RepSession();

    virtual int open(void * = 0);
    virtual int handle_input(ACE_HANDLE fd);
    virtual int handle_output(ACE_HANDLE fd);
    virtual int handle_close(ACE_HANDLE fd,
                             ACE_Reactor_Mask mask);
    int envId_;
private:
    ACE_Reactor_Notification_Strategy notifier_;

    int processMsg(Dbt *control, 
                   Dbt *rec, int *envId, 
                   DbLsn *retLsn);
};

class RepTask:
    public ACE_Task_Base
{
    public:
        RepTask();
        ~RepTask();
        virtual int svc();
};

class RepMgr:
    public ACE_Event_Handler
{
    typedef ACE_Acceptor< RepSession, ACE_SOCK_ACCEPTOR > 
        RepListener;
public:
    friend class 
        ACE_Singleton<RepMgr, ACE_Recursive_Thread_Mutex>;
    int beginRep(int myId);
    int addReplica(Replica& replica);
    int send(const Dbt *cntl,
             const Dbt *rec,
             const DbLsn    *lsn,
             int       eid,
             u_int32_t flags,
             int msgId);
    int connect(Replica_SP r);
    virtual int 
        handle_timeout(const ACE_Time_Value &currTime,
                               const void *act = 0);
    int sendFirstMsg(int repId);
    int startElec();
    int setMaster(int envId);
    int beginRole(int role);
    bool isMaster();

private:
    RepMgr();
    ~RepMgr();

    RepMap repMap_;
    ACE_TP_Reactor      reactor_;
    Replica_SP          myReplica_;
    int                 myId_;
    RepListener         listener_;
    RepTask             tasks_;
    ACE_RW_Thread_Mutex mapMutex_;
    ACE_Recursive_Thread_Mutex masterMutex_;
    int                 masterId_;
    Dbt                 cData_;
    friend class RepSession;
};

typedef ACE_Singleton<RepMgr, ACE_Recursive_Thread_Mutex> 
RepMgrSingleton;

int repSend (DbEnv *env,
         const Dbt *cnt,
         const Dbt *rec,
         const DbLsn *lsn,
         int    eid,
         u_int32_t flags);
#endif // _REP_H_
