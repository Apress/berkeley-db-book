package chap_11;

import java.io.Serializable;

public class PersonData implements Serializable
{
    private String name_;
    private String dob_;

    public PersonData(String name, String dob)
    {
        name_ = name;
        dob_ = dob;
    }

    public final String getName()
    {
        return name_;
    }

    public final String getDOB()
    {
        return dob_;
    }

    public String toString()
    {
        return "(PersonData: name = " + name_ + " dob = " + dob_ + ")";
    }
}
