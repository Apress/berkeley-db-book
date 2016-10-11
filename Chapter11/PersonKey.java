package chap_11;

import java.io.Serializable;

public class PersonKey implements Serializable
{
    private Long ssn_;

    public PersonKey(Long ssn)
    {
        ssn_ = ssn;
    }

    public final Long getSSN()
    {
        return ssn_;
    }

    public String toString()
    {
        return "(PersonKey: ssn = " + ssn_ + ")";
    }
}
