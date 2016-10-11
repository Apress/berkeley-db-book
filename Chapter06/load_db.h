#ifndef _LOAD_DB_
#define _LOAD_DB_

#include <iostream>
#include <string>
#include <sstream>
#include <fstream>
#include <map>

#include <db_cxx.h>

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

class DbUtils
{
    public:
        static void loadDbs(DbEnv *env);
        static void load(const char*file, const char* dbName, int id);
        static void getStrVal(std::istream& instrm, std::string& value);
        static void closeDbResources();
        static int getSecKey(Db *secondary, const Dbt *pKey, 
                const Dbt *data, Dbt *secKey);

        static Db* getDbHandle(int dbId);
        typedef std::map< int, Db* > DbMap;
        typedef std::map< int, Db* >::iterator DbMapIter;
        static DbMap dbMap_;
        static DbEnv *dbEnv_;
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
        DbUtils::dbEnv_->txn_begin(NULL, &txn_, 0);
        int ret = db_->cursor(txn_, &cursor_, flag); 
        state_ = ACTIVE;
    }
    catch(DbException& ex)
    {
        DbUtils::dbEnv_->err(ex.get_errno(), "DbIter::init");
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
        cursor_->close();
        txn_->commit(0);
    }
    catch(DbException& ex)
    {
        DbUtils::dbEnv_->err(ex.get_errno(), "DbIter::close");
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
        DbUtils::dbEnv_->err(ret, "DbIter::next");
    }
    return ret;

}
typedef DbIter<Person> PersonIter;
#endif //_LOAD_DB_
