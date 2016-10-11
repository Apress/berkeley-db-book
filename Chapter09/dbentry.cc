#include "dbentry.h"
#include "gtm.h"

#include <ace/Log_Msg.h>
#include <ace/Guard_T.h>
#include <ace/Thread_Manager.h>
#include <ace/OS_NS_Thread.h>
#include <netinet/in.h>

DbEntry::DbEntry()
{}

void DbEntry::dbDel(Db *db, Dbt *key, DbTxn* t)
{
    DbTxn *txn = t;
    try
    {
        if(t == NULL)
            env_->txn_begin(NULL, &txn, 0);
        int ret = db->del(txn, key, 0);
        if(ret)
            env_->err(ret, "DbEntry::dbGet" );
        if(t == NULL)
            txn->commit(0);
    }
    catch(DbException &txn_ex)
    {
        env_->err(txn_ex.get_errno(), "DbEntry::dbGet");
        if(t == NULL)
            txn->abort();
        else
            throw;
    }
}

int DbEntry::dbGet(Db *db, Dbt *key, Dbt* data, DbTxn* t)
{
    int ret = -1;
    DbTxn *txn = t;
    try
    {
        if(t == NULL)
            env_->txn_begin(NULL, &txn, 0);
        ret = db->get(txn, key, data, 0);
        if(ret)
            env_->err(ret, "DbEntry::dbGet" );
        if(t == NULL)
            txn->commit(0);
    }
    catch(DbException &txn_ex)
    {
        env_->err(txn_ex.get_errno(), "DbEntry::dbGet");
        if(t == NULL)
            txn->abort();
        else
            throw;
    }
    return ret;
}

void DbEntry::dbPut(Db *db, DbTxn* t, bool serializeData)
{
    if(serializeData)
        serialize();

    void *pkey = NULL;
    int keyLen = 0;

    getPKey(pkey, keyLen);

    Dbt pkeyDbt(pkey, keyLen);
    Dbt valueDbt(serializedData_, serializedLen_);

    DbTxn *txn = t;
    try
    {
        if(t == NULL)
            env_->txn_begin(NULL, &txn, 0);
        int ret = db->put(txn, &pkeyDbt, &valueDbt, 0);
        if(ret)
            env_->err(ret, "DbEntry::dbPut" );
        if(t == NULL)
            txn->commit(0);
    }
    catch(DbException &txn_ex)
    {
        env_->err(txn_ex.get_errno(), "DbEntry::dbPut");
        if(t == NULL)
            txn->abort();
        else
            throw;
    }

}

void DbEntry::reverseEndian(unsigned long *intVal)
{
    ACE_DEBUG((LM_ERROR, "original value %x\n", *intVal));
    unsigned long tmpInt = *intVal;
    char *buf = (char*)intVal;
    char *bufOrig = (char*)(&tmpInt);
    int size = sizeof(unsigned long);
    for(int i=0; i < size; i++)
    {
        buf[size - i - 1] = bufOrig[i];
    }
    ACE_DEBUG((LM_ERROR, "converted value %x\n", *intVal));
}

GTxnEntry::GTxnEntry()
{
    GTM *gtm = GTMSingleton::instance();
    setEnv(&(gtm->gtmEnv_));
    memset(xid_, 0, DB_XIDDATASIZE);
}


GTxnEntry::GTxnEntry(u_int8_t *xid):
    status_(PRE_PREPARE)
{
    GTM *gtm = GTMSingleton::instance();
    setEnv(&(gtm->gtmEnv_));
    memcpy(xid_, xid, DB_XIDDATASIZE);
}

int GTxnEntry::deserialize(char *buf)
{
    char *tmpBuf = buf;
    int totalLen = 0;
    int elemLen = 0;

    elemLen = sizeof(status_);
    status_ = *((TxnStatus *)tmpBuf);

    std::cout << "status is " << status_ << std::endl;
    return 0;
}

char* GTxnEntry::serialize()
{
    memset (serializedData_, 0, 2048);

    char *buf= serializedData_;

    int elemLen = 0;
    int totalLen = 0;

    elemLen = sizeof(u_int32_t);
    memcpy(buf, &status_, elemLen);
    totalLen += elemLen;
    buf += elemLen;

    serializedLen_ = totalLen;
    return serializedData_;
}

void GTxnEntry::getPKey(void*& key, int& length)
{
    key = xid_;
    length = DB_XIDDATASIZE;
}

int GTxnEntry::getByXid(u_int8_t *xid, DbTxn *txn)
{
    int ret = -1;
    Dbt key;
    Dbt data;
    data.set_flags(DB_DBT_MALLOC);
    key.set_data((void*)(xid));
    key.set_size(DB_XIDDATASIZE);
    GTM *gtm = GTMSingleton::instance();
    ret = dbGet(gtm->gtmDb_, &key, &data, txn);
    if(ret == 0)
        deserialize((char*)(data.get_data()));
    return ret;
}

int GTxnEntry::update(TxnStatus status)
{
    status_ = status;
    GTM *gtm = GTMSingleton::instance();
    dbPut(gtm->gtmDb_, NULL, true);
}

std::string GTxnEntry::toString()
{
    std::stringstream ss;
    ss << "XID: ";
    for(int i=0; i < DB_XIDDATASIZE; i++)
    {
        ss << xid_[i];
        ss << ":";
    }
    ss << " Status: ";
    ss << status_;

    return ss.str();
}

Person::Person()
{}

Person::Person(unsigned long ssn, 
        std::string name, std::string dob):
    ssn_(ssn),
    name_(name),
    dob_(dob)
{
}

char* Person::serialize()
{
    memset (serializedData_, 0, 2048);

    char *buf= serializedData_;

    int elemLen = 0;
    int totalLen = 0;

    elemLen = sizeof(unsigned long);
    memcpy(buf, &ssn_, elemLen);
    totalLen += elemLen;
    buf += elemLen;

    elemLen = name_.length() + 1;
    memcpy(buf, name_.c_str(), elemLen);
    totalLen += elemLen;
    buf += elemLen;

    elemLen = dob_.length() + 1;
    memcpy(buf, dob_.c_str(), elemLen);
    totalLen += elemLen;

    serializedLen_ = totalLen;
    return serializedData_;

}

int Person::deserialize(char *buf)
{
    char *tmpBuf = buf;
    int totalLen = 0;
    int elemLen = 0;

    elemLen = sizeof(ssn_);
    ssn_ = *((unsigned long *)tmpBuf);
    totalLen += elemLen;
    tmpBuf += elemLen;

    name_ = tmpBuf;
    elemLen = name_.length() + 1;
    totalLen += elemLen;
    tmpBuf += elemLen;
    
    dob_ = tmpBuf;
    elemLen = dob_.length() + 1;
    totalLen += elemLen;

    return 0;
}

void Person::getPKey(void*& key, int& length)
{
    ssnReversed_ = htonl(ssn_);
    key = &ssn_;
    length = sizeof(ssn_);
}

void Person::insert(GTxn* gTxn)
{
    LocalHandles handles;
    gTxn->getLocalHandles(handles);
    LocalHandlesIter beg = handles.begin();
    for(; beg != handles.end(); ++beg)
    {
        setEnv((*beg)->env);
        ACE_DEBUG((LM_ERROR, "Person::insert: env ID is %d\n",
                    (*beg)->envId));
        dbPut((*beg)->db, (*beg)->txn);
    }
}

std::string Person::toString()
{
    std::stringstream ss;
    ss << "Name: ";
    ss << name_;
    ss << " SSN: ";
    ss << ssn_;
    ss << " DOB: ";
    ss << dob_;

    return ss.str();
}
