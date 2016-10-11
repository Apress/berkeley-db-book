#include <db_cxx.h>
#include <ace/Task.h>
#include <string>
#include <sstream>

void errCallback (const DbEnv *env, const char *prefix, const char *errMsg)
{
    ACE_DEBUG((LM_ERROR, "(%t) %s errMsg: %s\n", prefix, errMsg));
}

struct DataAlign
{
    int a;
    char b;
    int c;
};

std::string getStr(char *data, int len)
{
    std::stringstream ss;
    for(int i=0; i < len; i++)
    {
        ss << data[i];
        ACE_DEBUG((LM_ERROR, "inserting %X \n", data[i]));
    }
    return ss.str();
}

int main(int argc, char **argv)
{
    DbEnv dbenv(0);
    try
    {
        const char *envhome = "./misc_env";

        dbenv.set_errpfx("misc");
        dbenv.set_errcall(errCallback);
        dbenv.set_lk_detect(DB_LOCK_DEFAULT);

        u_int32_t envFlags = 
            DB_CREATE | DB_INIT_MPOOL | DB_INIT_TXN | 
            DB_INIT_LOG | DB_INIT_LOCK | DB_RECOVER | DB_THREAD;

        dbenv.open(envhome, envFlags, 0);

        Db miscDb(&dbenv, 0);
        miscDb.open(NULL, "Misc_Db", NULL, DB_BTREE, 
                DB_CREATE | DB_THREAD, 0644);

        DataAlign d;
        memset(&d, 0, sizeof(d));
        d.a = 1;
        d.b = (char)2;
        d.c = 3;

        char *dataStr = "Data alignment test";

        Dbt key((void*)&d, sizeof(d));
        Dbt data((void*)dataStr, strlen(dataStr) + 1);
        
        miscDb.put(NULL, &key, &data, 0);

        DataAlign d1;
        memset(&d1, 0, sizeof(d1));
        d1.a = 1;
        d1.b = (char)2;
        d1.c = 3;

        Dbt newKey((void*)&d1, sizeof(d1));
        ACE_HEX_DUMP((LM_ERROR, (char*)(newKey.get_data()), 
                    newKey.get_size()));
        Dbt value1;
        value1.set_flags(DB_DBT_MALLOC);
        int ret = miscDb.get(NULL, &newKey, &value1, 0);

        if(ret == 0)
        {
            std::string str = (char*)(value1.get_data());
            ACE_DEBUG((LM_ERROR, "get: %s\n", str.c_str()));
        }
        else
        {
            ACE_DEBUG((LM_ERROR, "data not found\n"));
        }
    }
    catch(DbException &dbex)
    {
        dbenv.err(dbex.get_errno(), "Db exception caught");
    }
    catch(...)
    {
        dbenv.err(0, "unknown exception caught");
    }
}
