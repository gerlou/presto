package com.facebook.presto.metadata;

import com.facebook.presto.operator.scalar.AbstractTestFunctions;
import com.facebook.presto.spi.InvocationConvention;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import static com.facebook.presto.metadata.FunctionInvokerProvider.checkChoice;
import static com.facebook.presto.operator.scalar.ScalarFunctionImplementation.NullConvention.BLOCK_AND_POSITION;
import static com.facebook.presto.operator.scalar.ScalarFunctionImplementation.NullConvention.RETURN_NULL_ON_NULL;
import static com.facebook.presto.operator.scalar.ScalarFunctionImplementation.NullConvention.USE_BOXED_TYPE;
import static com.facebook.presto.operator.scalar.ScalarFunctionImplementation.NullConvention.USE_NULL_FLAG;
import static com.facebook.presto.spi.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION;
import static com.facebook.presto.spi.InvocationConvention.InvocationArgumentConvention.BOXED_NULLABLE;
import static com.facebook.presto.spi.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static com.facebook.presto.spi.InvocationConvention.InvocationArgumentConvention.NULL_FLAG;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestFunctionInvokerProvider
        extends AbstractTestFunctions
{
    @Test
    public void testFunctionInvokerProvider()
    {
        assertTrue(checkChoice(ImmutableList.of(USE_BOXED_TYPE, USE_BOXED_TYPE),
                true,
                false,
                new InvocationConvention(ImmutableList.of(BOXED_NULLABLE, BOXED_NULLABLE),
                        InvocationConvention.InvocationReturnConvention.BOXED_NULLABLE, false)));

        assertTrue(checkChoice(ImmutableList.of(RETURN_NULL_ON_NULL, BLOCK_AND_POSITION, BLOCK_AND_POSITION),
                false,
                false,
                new InvocationConvention(ImmutableList.of(NEVER_NULL, BLOCK_POSITION, BLOCK_POSITION),
                        InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL, false)));

        assertTrue(checkChoice(ImmutableList.of(BLOCK_AND_POSITION, USE_NULL_FLAG, BLOCK_AND_POSITION),
                false,
                false,
                new InvocationConvention(ImmutableList.of(BLOCK_POSITION, NULL_FLAG, BLOCK_POSITION),
                        InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL, false)));

        assertFalse(checkChoice(ImmutableList.of(BLOCK_AND_POSITION, USE_BOXED_TYPE),
                false,
                false,
                new InvocationConvention(ImmutableList.of(BLOCK_POSITION, BOXED_NULLABLE),
                        InvocationConvention.InvocationReturnConvention.BOXED_NULLABLE, false)));

        assertFalse(checkChoice(ImmutableList.of(BLOCK_AND_POSITION, BLOCK_AND_POSITION),
                false,
                false,
                new InvocationConvention(ImmutableList.of(BLOCK_POSITION, NULL_FLAG),
                        InvocationConvention.InvocationReturnConvention.BOXED_NULLABLE, false)));

        assertFalse(checkChoice(ImmutableList.of(USE_NULL_FLAG, USE_BOXED_TYPE),
                true,
                false,
                new InvocationConvention(ImmutableList.of(BLOCK_POSITION, BOXED_NULLABLE),
                        InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL, false)));
    }
}
