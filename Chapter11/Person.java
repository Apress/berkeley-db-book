package chap_11;

import java.io.Serializable;
public class Person implements Serializable
{
    private Long ssn_;
    private String name_;
    private String dob_;

    public Person()
    {}

    public Person(Long ssn, 
            String name, 
            String dob)
    {
        ssn_ = ssn;
        name_ = name;
        dob_ = dob;
    }

    public void setSSN(Long ssn)
    {
        ssn_ = ssn;
    }

    public void setName(String name)
    {
        name_ = name;
    }

    public void setDOB(String dob)
    {
        dob_ = dob;
    }

    public Long getSSN()
    {
        return ssn_;
    }

    public String getName()
    {
        return name_;
    }

    public String getDOB()
    {
        return dob_;
    }

    public String toString()
    {
        return "Person: ssn = " + ssn_ + " name = " + name_ 
            + " dob = " + dob_;
    }
};
