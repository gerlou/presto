package com.facebook.presto.type;

import com.facebook.presto.operator.scalar.AbstractTestFunctions;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.function.BlockIndex;
import com.facebook.presto.spi.function.BlockPosition;
import com.facebook.presto.spi.function.FunctionDependency;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.StandardTypes;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.lang.invoke.MethodHandle;

import static com.facebook.presto.spi.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION;
import static com.facebook.presto.spi.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static com.facebook.presto.spi.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.google.common.base.Throwables.throwIfInstanceOf;

public class TestConventionDependencies
        extends AbstractTestFunctions
{
    @BeforeClass
    public void setUp()
    {
        registerParametricScalar(FunctionWithConvention.class);
        registerParametricScalar(Add.class);
        registerParametricScalar(BlockPositionTest.class);
    }

    @Test
    public void testConventionDependencies()
    {
        assertFunction("test_convention(1, 1)", INTEGER, 2);
        assertFunction("test_convention(50, 10)", INTEGER, 60);
        assertFunction("test_convention(1, 0)", INTEGER, 1);
        assertFunction("test_block_position(ARRAY [1, 2, 3])", INTEGER, 6);
        assertFunction("test_block_position(ARRAY [25, 0, 5])", INTEGER, 30);
        assertFunction("test_block_position(ARRAY [56, 275, 36])", INTEGER, 367);
    }

    @ScalarFunction("test_convention")
    public static class FunctionWithConvention
    {
        @SqlType(StandardTypes.INTEGER)
        public static long testAdd(
                @FunctionDependency(name = "add",
                returnType = StandardTypes.INTEGER,
                argumentTypes = {StandardTypes.INTEGER, StandardTypes.INTEGER},
                convention = @FunctionDependency.Convention(arguments = {NEVER_NULL, NEVER_NULL}, result = FAIL_ON_NULL)) MethodHandle function,
                @SqlType(StandardTypes.INTEGER) long left,
                @SqlType(StandardTypes.INTEGER) long right)
        {
            try {
                return (long) function.invokeExact(left, right);
            }
            catch (Throwable t) {
                throwIfInstanceOf(t, Error.class);
                throwIfInstanceOf(t, PrestoException.class);
                throw new PrestoException(GENERIC_INTERNAL_ERROR, t);
            }
        }
    }

    @ScalarFunction("add")
    public static class Add
    {
        @SqlType(StandardTypes.INTEGER)
        public static long add(
                @SqlType(StandardTypes.INTEGER) long left,
                @SqlType(StandardTypes.INTEGER) long right)
        {
            return Math.addExact((int) left, (int) right);
        }

        @SqlType(StandardTypes.INTEGER)
        public static long addBlockPosition(
                @SqlType(StandardTypes.INTEGER) long first,
                @BlockPosition @SqlType(value = StandardTypes.INTEGER, nativeContainerType = long.class) Block block,
                @BlockIndex int position)
        {
            return Math.addExact((int) first, (int) INTEGER.getLong(block, position));
        }
    }

    @ScalarFunction("test_block_position")
    public static class BlockPositionTest
    {
        @SqlType(StandardTypes.INTEGER)
        public static long testAddBlockPosition(
                @FunctionDependency(
                    name = "add",
                    returnType = StandardTypes.INTEGER,
                    argumentTypes = {StandardTypes.INTEGER, StandardTypes.INTEGER},
                    convention = @FunctionDependency.Convention(arguments = {NEVER_NULL, BLOCK_POSITION}, result = FAIL_ON_NULL)) MethodHandle function,
                @SqlType("array(int)") Block array)
        {
            long sum = 0;
            for (int i = 0; i < array.getPositionCount(); i++) {
                try {
                    sum = (long) function.invokeExact(sum, array, i);
                }
                catch (Throwable t) {
                    throwIfInstanceOf(t, Error.class);
                    throwIfInstanceOf(t, PrestoException.class);
                    throw new PrestoException(GENERIC_INTERNAL_ERROR, t);
                }
            }
            return sum;
        }
    }
}
