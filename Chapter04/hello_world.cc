
#include <iostream>

#include <db_cxx.h>

using std::cout;
using std::endl;
using std::cerr;

void errCallback (const DbEnv *env, const char *prefix, const char *errMsg)
{
    cout << prefix << " " << errMsg << endl;
}

int main(int argc, char **argv)
{
    Db db(0, 0);
    
    try
    {
        db.set_errpfx("hello_world");
        db.set_errcall(errCallback);

        db.open(NULL, 
                "./chap4_db", 
                NULL, 
                DB_BTREE, 
                DB_CREATE, 
                0644);

        char *first_key = "first_record";
        u_int32_t key_len = (u_int32_t)strlen(first_key);
        
        char *first_value = "Hello World - Berkeley DB style!!";
        u_int32_t value_len = (u_int32_t)strlen(first_value);

        Dbt key(first_key, key_len + 1 );
        Dbt value(first_value, value_len + 1);

        int ret;
        ret = db.put(0,
                     &key,
                     &value,
                     DB_NOOVERWRITE);

        if (ret == DB_KEYEXIST)
        {
            db.err(ret, "");
        }

        Dbt stored_value;
        ret = db.get(0,
                     &key,
                     &stored_value,
                     0);
        cout << (char *)stored_value.get_data()
            << endl;

        db.close(0);

    }
    catch(DbException &dbex)
    {
        db.err(dbex.get_errno(), "Db exception caught");
    }
}
