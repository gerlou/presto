package com.facebook.presto.metadata;

import com.facebook.presto.operator.scalar.ScalarFunctionImplementation;
import com.facebook.presto.operator.scalar.ScalarFunctionImplementation.NullConvention;
import com.facebook.presto.operator.scalar.ScalarFunctionImplementation.ScalarImplementationChoice;
import com.facebook.presto.spi.InvocationConvention;
import com.facebook.presto.spi.InvocationConvention.InvocationArgumentConvention;
import com.google.common.annotations.VisibleForTesting;

import java.util.List;

import static com.facebook.presto.operator.scalar.ScalarFunctionImplementation.NullConvention.BLOCK_AND_POSITION;
import static com.facebook.presto.operator.scalar.ScalarFunctionImplementation.NullConvention.RETURN_NULL_ON_NULL;
import static com.facebook.presto.operator.scalar.ScalarFunctionImplementation.NullConvention.USE_BOXED_TYPE;
import static com.facebook.presto.operator.scalar.ScalarFunctionImplementation.NullConvention.USE_NULL_FLAG;

public class FunctionInvokerProvider
{
    private final FunctionRegistry functionRegistry;

    public FunctionInvokerProvider(FunctionRegistry functionRegistry)
    {
        this.functionRegistry = functionRegistry;
    }

    public FunctionInvoker createFunctionInvoker(Signature signature, InvocationConvention invocationConvention)
    {
        ScalarFunctionImplementation scalarFunctionImplementation = functionRegistry.getScalarFunctionImplementation(signature);
        for (ScalarImplementationChoice choice : scalarFunctionImplementation.getAllChoices()) {
            if (checkChoice(choice.getNullConventionList(), choice.isNullable(), choice.hasSession(), invocationConvention)) {
                return new FunctionInvoker(choice.getMethodHandle());
            }
        }
        return new FunctionInvoker(null);
    }

    @VisibleForTesting
    static boolean checkChoice(List<NullConvention> nullConventionList, boolean nullable, boolean hasSession, InvocationConvention invocationConvention)
    {
        for (int i = 0; i < nullConventionList.size(); i++) {
            InvocationArgumentConvention invocationArgumentConvention = invocationConvention.getArgumentConvention(i);
            NullConvention nullConvention = nullConventionList.get(i);

            if (invocationArgumentConvention == InvocationArgumentConvention.FUNCTION) {
                return false;
            }
            if (nullConvention == RETURN_NULL_ON_NULL && invocationArgumentConvention != InvocationArgumentConvention.NEVER_NULL) {
                return false;
            }
            else if (nullConvention == USE_BOXED_TYPE && invocationArgumentConvention != InvocationArgumentConvention.BOXED_NULLABLE) {
                return false;
            }
            else if (nullConvention == USE_NULL_FLAG && invocationArgumentConvention != InvocationArgumentConvention.NULL_FLAG) {
                return false;
            }
            else if (nullConvention == BLOCK_AND_POSITION && invocationArgumentConvention != InvocationArgumentConvention.BLOCK_POSITION) {
                return false;
            }
        }

        if (nullable && invocationConvention.getReturnConvention() != InvocationConvention.InvocationReturnConvention.BOXED_NULLABLE) {
            return false;
        }
        else if (!nullable && invocationConvention.getReturnConvention() != InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL) {
            return false;
        }
        if (hasSession != invocationConvention.hasSession()) {
            return false;
        }
        return true;
    }
}
