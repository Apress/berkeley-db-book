#include "load_db.h"

#include <ace/Log_Msg.h>
#include <ace/Guard_T.h>
#include <ace/Thread_Manager.h>
#include <ace/OS_NS_Thread.h>
#include <netinet/in.h>

static const DbDesc dbDescArr[] = 
{
    { "Person_Db", "person.txt", PERSON_DB, 0, 0 },
    { "Account_Db", "account.txt", 2, 0, 0 },
    { "Bank_Db", "bank.txt", 3, 0, 0 },
    { "Person_Name_Db", "", PERSON_NAME_DB, 1, PERSON_DB },
    { "Person_Dob_Db", "", PERSON_DOB_DB, 1, PERSON_DB }
};

static const size_t numDb = 5;

static int btreeCompare(Db *db, const Dbt *d1, const Dbt *d2);

static void errCallback (const DbEnv *env, 
                         const char *prefix, 
                         const char *errMsg)
{   
        ACE_DEBUG((LM_ERROR, 
                    "(%P:%t) %s errMsg: %s\n", prefix, errMsg));
}

void readAccount(std::string& line)
{
    //ACE_DEBUG((LM_ERROR, "readAccount: reading %s\n", line.c_str()));
    std::istringstream iss (line, std::istringstream::in);
    std::string strVal;

    Account acc;
    char c;

    iss >> acc.accNumber;
    iss >> c;

    iss >> acc.ssn;
    iss >> c;
    
    iss >> acc.type;
    iss >> c;

    iss >> acc.balance;
    iss >> c;

    iss >> acc.bankId;

    ACE_DEBUG((LM_ERROR, 
                "accNumber-%u ssn-%Q type-%u balance-%u bankId-%u\n",
                acc.accNumber, acc.ssn, acc.type, acc.balance, acc.bankId));
}

void readBank(std::string& line)
{
    //ACE_DEBUG((LM_ERROR, "Load::readBank: reading %s\n", line.c_str()));
    std::istringstream iss (line, std::istringstream::in);
    std::string strVal;

    Bank b;
    char c;

    iss >> b.id;
    iss >> c;

    DbUtils::getStrVal(iss, strVal);
    b.name = strVal;

    iss >> b.zip;

    ACE_DEBUG((LM_ERROR,
                "readBank: id-%u name-%s zip-%u \n",
                b.id, b.name.c_str(), b.zip));
}

bool DbUtils::isInitialized_ = false;
bool DbUtils::isClosed_ = false;
ACE_Recursive_Thread_Mutex DbUtils::initMutex_;
int DbUtils::runRecovery_ = 0;
bool DbUtils::initDbDone_ = false;

DbUtils::DbUtils():
    dbEnv_(0)
{
    initEnv();
}
void DbUtils::initEnv()
{
    try
    {
        std::string envhome = "./chap8_env";
        std::stringstream ss;
        ss << envhome;

        dbEnv_.set_errpfx("chap8_ex");
        dbEnv_.set_errcall(errCallback);
        dbEnv_.set_lk_detect(DB_LOCK_DEFAULT);
        dbEnv_.set_thread_count(16);
        u_int32_t envFlags =
            DB_INIT_MPOOL | DB_INIT_TXN |
            DB_INIT_LOCK | DB_THREAD | 
            DB_INIT_REP | DB_INIT_LOG |
            DB_RECOVER | DB_CREATE;
        dbEnv_.open(ss.str().c_str(), envFlags, 0);
    }
    catch(DbException &dbex)
    {
        dbEnv_.err(dbex.get_errno(), "Db exception caught");
    }
    catch(...)
    {
        std::cout << "unknown exception caught" << std::endl;
    }
}

DbUtils::~DbUtils()
{
    if(!runRecovery_ && !isClosed_)
    {
        closeDbResources();
        isClosed_ = true;
    }
}
DbUtils_SP& DbUtils::getInstance()
{
    ACE_Guard<ACE_Recursive_Thread_Mutex> guard(initMutex_);
    if(!isInitialized_)
    {
        singleton_.reset(new DbUtils());
        isInitialized_ = true;
    }
    return singleton_;
}

DbEnv* DbUtils::getEnv()
{
    return &(getInstance()->dbEnv_);
}

void DbUtils::loadDbs()
{

    for(int i=0; i < numDb; i++)
    {
        DbDesc dd = dbDescArr[i];
        ACE_DEBUG((LM_ERROR,
                    "(%t)opening %s\n",
                    dd.dbName));
        Db *db = new Db(&dbEnv_, 0);
        DbTxn *txn;
        try
        {
            dbEnv_.txn_begin(NULL, &txn, 0);
            db->set_pagesize(512);
            if(dd.isSec)
            {
                db->set_flags(DB_DUPSORT);
                db->open(txn, dd.dbName, NULL ,
                        DB_BTREE, DB_THREAD | DB_CREATE, 0644);
                DbMapIter it = dbMap_.find(dd.primaryId);
                if(it != dbMap_.end())
                {
                    Db *primary = it->second;
                    primary->associate(txn, db, 
                            DbUtils::getSecKey, 0);
                }
                else
                {
                    ACE_DEBUG((LM_ERROR, 
                                "unknonw primary db %d\n", 
                                dd.primaryId));
                    throw;
                }
            }
            else
            {
                db->open(txn, dd.dbName, NULL ,
                        DB_BTREE, DB_CREATE | DB_THREAD, 0644);
            }
            txn->commit(0);
            dbMap_[dd.id] = db;
        }
        catch(DbException &txn_ex)
        {
            txn->abort();
            dbEnv_.err(txn_ex.get_errno(), "loadDbs");
        }
    }

    ACE_DEBUG((LM_ERROR, "loadDbs: going to load default data \n"));
    for(int i=0; i < numDb; i++)
    {
        DbDesc dd = dbDescArr[i];
        if(!dd.isSec)
            load(dd.fileName, dd.dbName, dd.id);
    }
    initDbDone_ = true;
}

void DbUtils::load(const char *file, const char *dbName, int id)
{
    std::ifstream fileStrm(file, std::ios::in);
    if ( !fileStrm )
    {
        ACE_DEBUG((LM_ERROR, "LoadDb::load: unable to open %s\n", file));
        throw std::exception();
    }

    Db *db = getDbHandle(id);

    while (!fileStrm.eof())
    {
        std::string line;
        std::getline(fileStrm, line);
        if(line.length() <= 0)
            break;
        switch(id) 
        {
            case PERSON_DB:
                {
                    Person p(line);
                    p.dbPut(db);
                }
                break;
            case 2:
                readAccount(line);
                break;
            case 3:
                readBank(line);
                break;
            default:
                ACE_DEBUG((LM_ERROR, "LoadDb::load: unknown Db id %d\n", id));
                break;
        }
    }
}

void DbUtils::getStrVal(std::istream& instrm, std::string& value)
{
    char c;
    value.clear();
    while(instrm.get(c))
    {
        switch(c)
        {
            case ',':
            case '\n':
                return;
            default:
                value += c;
        }
    }
}

void DbUtils::closeDbResources()
{
//#if 0
    ACE_DEBUG((LM_ERROR, "DbUtils::closeDbResources start\n"));
    DbMapIter it = dbMap_.begin();
    for(; it != dbMap_.end(); ++it)
    {
        Db *db = it->second;
        db->close(0);
    }
    ACE_DEBUG((LM_ERROR, "DbUtils::closeDbResources done close Db\n"));
    dbEnv_.close(0);
    ACE_DEBUG((LM_ERROR, "DbUtils::closeDbResources end\n"));
//#endif
}

Db* DbUtils::getDbHandle(int dbId)
{
    Db *db = NULL;
    DbMapIter it = (dbMap_).find(dbId);
    if(it != (dbMap_).end())
        db = it->second;
    return db;
}


Person::Person(std::string& line)
{
    std::istringstream iss (line, std::istringstream::in);
    std::string strVal;

    iss >> ssn_;

    char c;
    iss >> c;

    DbUtils::getStrVal(iss, strVal);
    name_ = strVal;

    DbUtils::getStrVal(iss, strVal);
    dob_ = strVal;

    ACE_DEBUG((LM_ERROR, "readPerson: ssn-%u name-%s, dob-%s\n",
                ssn_, name_.c_str(), dob_.c_str()));
}


DbEntry::DbEntry()
{}

void DbEntry::dbDel(Db *db, Dbt *key, DbTxn* t)
{
    DbTxn *txn = t;
    try
    {
        if(t == NULL)
            DbUtils::getEnv()->txn_begin(NULL, &txn, 0);
        int ret = db->del(txn, key, 0);
        if(ret)
            DbUtils::getEnv()->err(ret, "DbEntry::dbGet" );
        if(t == NULL)
            txn->commit(0);
    }
    catch(DbException &txn_ex)
    {
        DbUtils::getEnv()->err(txn_ex.get_errno(), "DbEntry::dbGet");
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
            DbUtils::getEnv()->txn_begin(NULL, &txn, 0);
        ret = db->get(txn, key, data, 0);
        if(ret)
            DbUtils::getEnv()->err(ret, "DbEntry::dbGet" );
        if(t == NULL)
            txn->commit(0);
    }
    catch(DbException &txn_ex)
    {
        DbUtils::getEnv()->err(txn_ex.get_errno(), "DbEntry::dbGet");
        if(t == NULL)
            txn->abort();
        else
            throw;
    }
    return ret;
}

void DbEntry::dbPut(Db *db, DbTxn* t)
{
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
            DbUtils::getEnv()->txn_begin(NULL, &txn, 0);
        int ret = db->put(txn, &pkeyDbt, &valueDbt, 0);
        if(ret)
            DbUtils::getEnv()->err(ret, "DbEntry::dbPut" );
        if(t == NULL)
            txn->commit(0);
    }
    catch(DbException &txn_ex)
    {
        DbUtils::getEnv()->err(txn_ex.get_errno(), "DbEntry::dbPut");
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


Person::Person()
{}

char* Person::serialize()
{
    memset (serializedData_, 0, 1000);

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
    ssn_ = htonl(ssn_);
    key = &ssn_;
    length = sizeof(ssn_);
}

void Person::insert(DbTxn* txn)
{
    dbPut(getDbHandle(), txn);
}

int Person::getBySSN(unsigned long ssn, DbTxn* txn)
{
    int ret = -1;
    unsigned long ssnVal = htonl(ssn);
    Dbt key;
    Dbt data;
    data.set_flags(DB_DBT_MALLOC);
    key.set_data((void*)(&ssnVal));
    key.set_size(sizeof(ssn));
    ret = dbGet(DbUtils::getInstance()->getDbHandle(PERSON_DB), 
            &key, &data, txn);
    if(ret == 0)
        deserialize((char*)(data.get_data()));
    return ret;

}

void Person::delBySSN(unsigned long ssn, DbTxn* txn)
{
    int ret = -1;
    unsigned long ssnVal = htonl(ssn);
    Dbt key;
    key.set_data((void*)(&ssnVal));
    key.set_size(sizeof(ssn));
    dbDel(DbUtils::getInstance()->getDbHandle(PERSON_DB), &key, txn);
}

Db* Person::getDbHandle()
{ 
    return DbUtils::getInstance()->getDbHandle(PERSON_DB); 
}
//void Person::getByName(std::string name)
//{
//}

void Person::getByNameAndDob(std::string& name, 
        std::string dob, DbTxn* txn)
{
    try
    {

        Dbt key;
        Dbt value;

        Dbc *nameCur = NULL;
        Db *personNameDb = DbUtils::getInstance()->getDbHandle(PERSON_NAME_DB);
        int ret = personNameDb->cursor(NULL, &nameCur, 0); 
        key.set_data((void*)(name.c_str()));
        key.set_size(name.length() + 1);
        ret = nameCur->get(&key, &value, DB_SET);
        if(ret)
        {
            DbUtils::getEnv()->err(ret, "Person::getByNameAndDob");
            nameCur->close();
            return;
        }

        Dbc *dobCur = NULL;
        Db *personDobDb = DbUtils::getInstance()->getDbHandle(PERSON_DOB_DB);
        ret = personDobDb->cursor(NULL, &dobCur, 0); 
        key.set_data((void*)dob.c_str());
        key.set_size(dob.length() + 1);
        ret = dobCur->get(&key, &value, DB_SET);
        if(ret)
        {
            DbUtils::getEnv()->err(ret, "Person::getByNameAndDob");
            nameCur->close();
            dobCur->close();
            return;
        }

        Dbc *curArr[3];
        curArr[0] = nameCur;
        curArr[1] = dobCur;
        curArr[2] = NULL;

        Db *personDb = DbUtils::getInstance()->getDbHandle(PERSON_DB);
        Dbc *joinCur;
        ret = personDb->join(curArr, &joinCur, 0);
        if(ret)
        {
            DbUtils::getEnv()->err(ret, "Person::getByNameAndDob");
            nameCur->close();
            dobCur->close();
            return;
        }

        while( (ret = joinCur->get(&key, &value, 0)) == 0)
        {
            Person p;
            p.deserialize((char*)(value.get_data()));
            ACE_DEBUG((LM_ERROR, "Found %s\n", p.toString().c_str()));
        }
        nameCur->close();
        dobCur->close();
        joinCur->close();
    }
    catch(DbException& ex)
    {
        DbUtils::getEnv()->err(ex.get_errno(), "Person::getByNameAndDob");
    }
}

void Person::getBulk()
{
    try
    {
        //the 5K buffer that will be used for bulk retreival
        int BULK_LEN = 5 * 1024;
        char buff[BULK_LEN];
        Dbt bulk;
        bulk.set_data(buff);
        bulk.set_ulen(BULK_LEN);
        bulk.set_flags(DB_DBT_USERMEM);

        //create a cursor on the database
        Dbc *bulkCur;
        Db *personDb = DbUtils::getInstance()->getDbHandle(PERSON_DB);
        int ret = personDb->cursor(NULL, &bulkCur, 0); 

        //in a loop retreive 5K worth of records at a time
        while(true)
        {
            Dbt key;
            ret = bulkCur->get(&key, &bulk, 
                    DB_MULTIPLE_KEY | DB_NEXT);
            if(ret)
            {
                DbUtils::getEnv()->err(ret, "getBulk:bulkCur:get");
                break;
            }
            //DbMultipleKeyDataIterator mIter(bulk);
            DbMultipleDataIterator mIter(bulk);
            Dbt d;
            Dbt k;
            Person p;
            //while( (mIter.next(k, d)) )
            while( (mIter.next(d)) )
            {
                p.deserialize((char*)(d.get_data()));
                ACE_DEBUG((LM_ERROR, "getBulk: %s\n", 
                            p.toString().c_str()));
            }
        }
        bulkCur->close();
    }
    catch(DbException& ex)
    {
        DbUtils::getEnv()->err(ex.get_errno(), "Person::getBulk");
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

int btreeCompare(Db *db, const Dbt *d1, const Dbt *d2)
{
    unsigned int d1_int;
    unsigned int d2_int;
    memcpy(&d1_int, d1->get_data(), sizeof(unsigned int));
    memcpy(&d2_int, d2->get_data(), sizeof(unsigned int));
    int ret = d1_int - d2_int;
    ACE_DEBUG((LM_ERROR, "d1 %d d2 %d returning %d\n", d1_int, d2_int, ret));
    return ret;
}
int DbUtils::getSecKey(Db *secondary, const Dbt *pKey, 
        const Dbt *data, Dbt *secKey)
{
    const char *dbName;
    const char *fileName;
    int ret = secondary->get_dbname(&fileName, &dbName);
    std::string fileStr = fileName;

    if(!fileStr.compare("Person_Name_Db"))
    {
        Person p;
        p.deserialize((char*)(data->get_data()));
        size_t len = p.name_.length() + 1;
        char *name = (char*)(malloc(len));
        memcpy(name, p.name_.c_str(), len);

        memset(secKey, 0, sizeof(Dbt));
        secKey->set_data(name);
        secKey->set_size(len);
        secKey->set_flags(DB_DBT_APPMALLOC);
        //ACE_DEBUG((LM_ERROR, "getSecKey called for %s, sec key returned %s\n",
        //            fileName, name));
    }
    else if(!fileStr.compare("Person_Dob_Db"))
    {
        Person p;
        p.deserialize((char*)(data->get_data()));
        size_t len = p.dob_.length() + 1;
        char *dob = (char*)(malloc(len));
        memcpy(dob, p.dob_.c_str(), len);

        memset(secKey, 0, sizeof(Dbt));
        secKey->set_data(dob);
        secKey->set_size(len);
        secKey->set_flags(DB_DBT_APPMALLOC);
        //ACE_DEBUG((LM_ERROR, "getSecKey called for %s, sec key returned %s\n",
        //            fileName, dob));
    }
    return 0;
}

