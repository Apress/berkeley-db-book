#ifndef _DB_ENTRY_
#define _DB_ENTRY_

#include <iostream>
#include <string>
#include <sstream>
#include <fstream>
#include <map>

#include <db_cxx.h>

#include <ace/Recursive_Thread_Mutex.h>
#include <boost/shared_ptr.hpp>

#define PERSON_DB 1
#define GTM_DB 2

class GTxn;
class Person;
template <class Entry> class DbIter;


class DbEntry
{
    public:
        DbEntry();
        virtual int deserialize(char *buf)=0;

    protected:
        virtual char* serialize()=0;
        virtual void getPKey(void*& key, int& length)=0;
        virtual std::string toString()=0;

        void setEnv(DbEnv *env)
        {env_ = env;}

        void reverseEndian(unsigned long *intVal);

        void dbPut(Db *db, DbTxn* txn=NULL, bool serialize=true);
        int dbGet(Db *db, Dbt *key, Dbt *data, DbTxn* txn=NULL);
        void dbDel(Db *db, Dbt *key, DbTxn* txn=NULL);

        char serializedData_[2048];
        int serializedLen_;
        DbEnv *env_;
};


enum TxnStatus
{
    PRE_PREPARE,
    PREPARING,
    COMMITTING,
    ABORTING,
    DONE
};


class GTxnEntry : public DbEntry
{
    public:
        GTxnEntry();
        GTxnEntry(u_int8_t *xid);
        void setStatus(TxnStatus status)
        {status_ = status;}

        TxnStatus getStatus()
        {return status_;}
        
        int deserialize(char *buf);
        int getByXid(u_int8_t *xid, DbTxn *txn);
        int update(TxnStatus status);
    protected:
        char* serialize();
        void getPKey(void*& key, int& length);
        std::string toString();
    private:
        u_int8_t    xid_[128];
        TxnStatus   status_;
};
typedef boost::shared_ptr<GTxnEntry> GTxnEntry_sp;

class Person : public DbEntry
{
    public:
        Person();
        Person(unsigned long ssn, 
                std::string name, std::string dob);
        int deserialize(char *buf);
        void insert(GTxn* txn);
    protected:
        char* serialize();
        void getPKey(void*& key, int& length);
        std::string toString();
    private:
        unsigned long       ssn_;
        unsigned long       ssnReversed_;
        std::string         name_;
        std::string         dob_;
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
        DbEnv *env_;
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
            env_->txn_begin(NULL, &txn_, 0);
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
        env_->err(ex.get_errno(), "DbIter::init");
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
        env_->err(ex.get_errno(), "DbIter::close");
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
        env_->err(ret, "DbIter::next");
    }
    return ret;

}
typedef DbIter<Person> PersonIter;
#endif //_DB_ENTRY_
