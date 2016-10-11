#ifndef _LOAD_DB_
#define _LOAD_DB_

#include <iostream>
#include <string>
#include <sstream>
#include <fstream>
#include <map>

#include <db_cxx.h>

//#include <ace/Thread_Mutex.h>
#include <ace/Recursive_Thread_Mutex.h>

#include <boost/shared_ptr.hpp>

#define PERSON_DB 1
#define ACCOUNT_DB 2
#define BANK_DB 3
#define PERSON_NAME_DB 4
#define PERSON_DOB_DB 5


class Person;
template <class Entry> class DbIter;

typedef struct
{
    const char*   dbName;
    const char*   fileName;
    int           id;
    int           isSec;
    int           primaryId;
} DbDesc;

class DbUtils;
typedef boost::shared_ptr<DbUtils> DbUtils_SP;

class DbUtils
{
    public:

        ~DbUtils();
        static void getStrVal(std::istream& instrm, 
                              std::string& value);
        void closeDbResources();
        static int getSecKey(Db *secondary, const Dbt *pKey, 
                const Dbt *data, Dbt *secKey);

        Db* getDbHandle(int dbId);
        static DbUtils_SP& getInstance();
        static DbEnv* getEnv();
        void loadDbs(int role = DB_REP_MASTER);
        void initEnv(int role = DB_REP_MASTER);

        static int isAlive(DbEnv *env,
                           pid_t pid,
                           db_threadid_t tid);
        static void threadId(DbEnv *env,
                             pid_t *pid,
                             db_threadid_t *tid);
        void setMainThId(ACE_thread_t id)
        { mainThId_ = id;}
        static int processNumber_;
        static int unrelated_;
        static int runRecovery_;
        static int repId_;
        static bool initDbDone_;

    protected:
        DbUtils();
        DbUtils(const DbUtils&);
        DbUtils& operator= (const DbUtils&);

        void load(const char*file, 
                  const char* dbName, int id);

        typedef std::map< int, Db* > DbMap;
        typedef std::map< int, Db* >::iterator DbMapIter;

        DbMap dbMap_;
        DbEnv dbEnv_;
        static bool isInitialized_;
        static bool isClosed_;
        static ACE_Recursive_Thread_Mutex initMutex_;

        static DbUtils_SP singleton_;
        static DbUtils* ptr_;
        static ACE_thread_t mainThId_;
};

class DbEntry
{
    public:
        DbEntry();
        virtual int deserialize(char *buf)=0;

    protected:
        virtual char* serialize()=0;
        virtual void getPKey(void*& key, int& length)=0;
        virtual std::string toString()=0;

        void reverseEndian(unsigned long *intVal);

        void dbPut(Db *db, DbTxn* txn=NULL);
        int dbGet(Db *db, Dbt *key, Dbt *data, DbTxn* txn=NULL);
        void dbDel(Db *db, Dbt *key, DbTxn* txn=NULL);

        char serializedData_[1000];
        int serializedLen_;

        friend class DbUtils;
};

class Person : public DbEntry
{
    public:
        Person();
        Person(std::string& line);
        Person(unsigned long ssn, std::string name, std::string dob);

        static void associateSec(Db *primary);

        void insert(DbTxn* txn=NULL);

        int getBySSN(unsigned long ssn, DbTxn* txn=NULL);
        void delBySSN(unsigned long ssn, DbTxn* txn=NULL);

        void getByNameAndDob(std::string& name, 
                std::string dob, DbTxn* txn=NULL);
        
        std::string getName()
        { return name_; }

        void setName(std::string name)
        { name_ = name; }

        std::string getDOB()
        { return dob_; }

        void setDOB(std::string dob)
        { dob_ = dob; }

        unsigned long getSSN()
        { return ssn_; }

        void setSSN(unsigned long ssn)
        { ssn_ = ssn; }

        Db* getDbHandle();

        int deserialize(char *buf);

        void getBulk();

    protected:
        char* serialize();
        void getPKey(void*& key, int& length);
        void regSecKeys(Db *db);
        std::string toString();

    private:
        unsigned long       ssn_;
        std::string         name_;
        std::string         dob_;

        friend class DbUtils;
};

class Account
{
    public:
        unsigned long       accNumber;
        unsigned long long  ssn;
        unsigned long       type;
        unsigned long       balance;
        unsigned long       bankId;
        friend class LoadDb;
};

class Bank
{
    public:
        unsigned long   id;
        std::string     name;
        unsigned long   zip;
        friend class LoadDb;
};

template <class Entry>
class DbIter
{
    public:
        DbIter();
        ~DbIter();

        bool next();
        bool prev();
        Entry& getEntry() {return curr_;}
        void close();

    private:
        void init(u_int32_t flag);
        int get(u_int32_t flag);

        Dbc *cursor_;
        int state_;
        Entry curr_;
        Db *db_;
        DbTxn *txn_;
        bool done_;
        bool closed_;
        static const int INACTIVE = 0;
        static const int ACTIVE = 1;
};

template <class Entry>
DbIter<Entry>::DbIter():
    state_(INACTIVE),
    cursor_(NULL),
    done_(false),
    closed_(false)
{
}

template <class Entry>
DbIter<Entry>::~DbIter()
{
    close();
}

template <class Entry>
void DbIter<Entry>::init(u_int32_t flag)
{
    try
    {
        db_ = curr_.getDbHandle();
        if(db_ != NULL)
        {
            DbUtils::getEnv()->txn_begin(NULL, &txn_, 0);
            int ret = db_->cursor(txn_, &cursor_, flag); 
        }
        else
        {
            done_ = true;
        }
        state_ = ACTIVE;
    }
    catch(DbException& ex)
    {
        DbUtils::getEnv()->err(ex.get_errno(), "DbIter::init");
        txn_->abort();
    }
}

template <class Entry>
void  DbIter<Entry>::close()
{
    if(closed_)
        return;
    try
    {
        if(cursor_ != NULL)
            cursor_->close();
        if(txn_ != NULL)
            txn_->commit(0);
    }
    catch(DbException& ex)
    {
        DbUtils::getEnv()->err(ex.get_errno(), "DbIter::close");
        txn_->abort();
    }
    closed_ = true;
}

template <class Entry>
bool DbIter<Entry>::next()
{
    if(state_ != ACTIVE)
        init(0);
    get(DB_NEXT);
    return !done_;
}

template <class Entry>
bool DbIter<Entry>::prev()
{
    if(state_ != ACTIVE)
        init(0);
    get(DB_PREV);
    return !done_;
}

template <class Entry>
int DbIter<Entry>::get(u_int32_t flag)
{
    Dbt key;
    Dbt value;
    int ret = -1;
    if(closed_ || done_)
        return ret;
    if(DbUtils::unrelated_ == 0)
    {
        int state = DbUtils::getEnv()->failchk(0);
        if(state != 0)
        {
            DbUtils::getEnv()->err(state, "DbIter::next");
            ACE_OS::exit(1);
        }
    }
    ret = cursor_->get(&key, &value, flag);
    if(ret == 0)
    {
        curr_.deserialize((char*)(value.get_data()));
    }
    else if(ret == DB_NOTFOUND)
    { 
        done_ = true;
        close();
    }
    else
    {
        DbUtils::getEnv()->err(ret, "DbIter::next");
    }
    return ret;

}
typedef DbIter<Person> PersonIter;
#endif //_LOAD_DB_
