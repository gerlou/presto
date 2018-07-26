package com.facebook.presto.spi;

import java.util.List;

public class InvocationConvention
{
    private final List<InvocationArgumentConvention> argumentConventionList;
    private final InvocationReturnConvention returnConvention;
    private final boolean hasSession;

    public InvocationConvention(List<InvocationArgumentConvention> argumentConventionList, InvocationReturnConvention returnConvention, boolean hasSession)
    {
        this.argumentConventionList = argumentConventionList;
        this.returnConvention = returnConvention;
        this.hasSession = hasSession;
    }

    public List<InvocationArgumentConvention> getArgumentConventionList()
    {
        return argumentConventionList;
    }

    public InvocationReturnConvention getReturnConvention()
    {
        return returnConvention;
    }

    public InvocationArgumentConvention getArgumentConvention(int index)
    {
        return argumentConventionList.get(index);
    }

    public boolean hasSession()
    {
        return hasSession;
    }

    public enum InvocationArgumentConvention
    {
        NEVER_NULL,
        BOXED_NULLABLE,
        NULL_FLAG,
        BLOCK_POSITION,
        FUNCTION
    }

    public enum InvocationReturnConvention
    {
        FAIL_ON_NULL,
        BOXED_NULLABLE
    }
}
