package com.facebook.presto.metadata;

import java.lang.invoke.MethodHandle;

import static java.util.Objects.requireNonNull;

public class FunctionInvoker
{
    private final MethodHandle methodHandle;

    public FunctionInvoker(MethodHandle methodHandle)
    {
        this.methodHandle = requireNonNull(methodHandle, "Method handle is null");
    }

    public MethodHandle methodHandle()
    {
        return methodHandle;
    }
}
