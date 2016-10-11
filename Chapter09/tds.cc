#include "gtm.h"

int main(int argc, char **argv)
{
    try
    {
        GTM *gtm = GTMSingleton::instance();
        gtm->openLocalEnvs();

        GTxn gTxn;
        gTxn.addEnv(1);
        gTxn.addEnv(2);
        gTxn.begin();
        Person p(123456789, "First Global Txn", 
                "May 21 2006");
        p.insert(&gTxn);
        gTxn.commit();

        GTxn gTxn1;
        gTxn1.addEnv(1);
        gTxn1.addEnv(2);
        gTxn1.begin();
        Person p1(223456789, "Second Global Txn", 
                "May 22 2006");
        p1.insert(&gTxn1);
        gTxn1.commit();

    }
    catch(DbException& dbex)
    {
        ACE_DEBUG((LM_ERROR, 
                    "caught DbException in main %d\n",
                    dbex.get_errno()));
    }
    catch(...)
    {
        ACE_DEBUG((LM_ERROR, "unknown exception caught\n"));
    }
}
