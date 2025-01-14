/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.stats;

import org.apache.paimon.casting.CastExecutor;
import org.apache.paimon.casting.CastFieldGetter;
import org.apache.paimon.casting.CastedRow;
import org.apache.paimon.data.BinaryArray;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.format.FieldStats;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.InternalRowUtils;
import org.apache.paimon.utils.ProjectedRow;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import static org.apache.paimon.utils.SerializationUtils.newBytesType;

/** Serializer for array of {@link FieldStats}. */
public class FieldStatsArraySerializer {

    private final InternalRowSerializer serializer;

    private final InternalRow.FieldGetter[] fieldGetters;

    @Nullable private final int[] indexMapping;
    @Nullable private final CastExecutor<Object, Object>[] converterMapping;
    @Nullable private final CastFieldGetter[] castFieldGetters;

    public FieldStatsArraySerializer(RowType type) {
        this(type, null, null, null);
    }

    public FieldStatsArraySerializer(
            RowType type,
            @Nullable int[] indexMapping,
            @Nullable CastExecutor<Object, Object>[] converterMapping,
            @Nullable CastFieldGetter[] castFieldGetters) {
        RowType safeType = toAllFieldsNullableRowType(type);
        this.serializer = new InternalRowSerializer(safeType);
        this.fieldGetters =
                IntStream.range(0, safeType.getFieldCount())
                        .mapToObj(
                                i ->
                                        InternalRowUtils.createNullCheckingFieldGetter(
                                                safeType.getTypeAt(i), i))
                        .toArray(InternalRow.FieldGetter[]::new);
        this.indexMapping = indexMapping;
        this.converterMapping = converterMapping;
        this.castFieldGetters = castFieldGetters;
    }

    public BinaryTableStats toBinary(FieldStats[] stats) {
        int rowFieldCount = stats.length;
        GenericRow minValues = new GenericRow(rowFieldCount);
        GenericRow maxValues = new GenericRow(rowFieldCount);
        Long[] nullCounts = new Long[rowFieldCount];
        for (int i = 0; i < rowFieldCount; i++) {
            minValues.setField(i, stats[i].minValue());
            maxValues.setField(i, stats[i].maxValue());
            nullCounts[i] = stats[i].nullCount();
        }
        return new BinaryTableStats(
                serializer.toBinaryRow(minValues).copy(),
                serializer.toBinaryRow(maxValues).copy(),
                BinaryArray.fromLongArray(nullCounts));
    }

    public FieldStats[] fromBinary(BinaryTableStats array) {
        return fromBinary(array, null);
    }

    public FieldStats[] fromBinary(BinaryTableStats array, @Nullable Long rowCount) {
        int fieldCount = indexMapping == null ? fieldGetters.length : indexMapping.length;
        FieldStats[] stats = new FieldStats[fieldCount];
        BinaryArray nullCounts = array.nullCounts();
        for (int i = 0; i < fieldCount; i++) {
            int fieldIndex = indexMapping == null ? i : indexMapping[i];
            if (fieldIndex < 0 || fieldIndex >= array.minValues().getFieldCount()) {
                // simple evolution for add column
                if (rowCount == null) {
                    throw new RuntimeException("Schema Evolution for stats needs row count.");
                }
                stats[i] = new FieldStats(null, null, rowCount);
            } else {
                CastExecutor<Object, Object> converter =
                        converterMapping == null ? null : converterMapping[i];
                Object min = fieldGetters[fieldIndex].getFieldOrNull(array.minValues());
                min = converter == null || min == null ? min : converter.cast(min);

                Object max = fieldGetters[fieldIndex].getFieldOrNull(array.maxValues());
                max = converter == null || max == null ? max : converter.cast(max);

                stats[i] =
                        new FieldStats(
                                min,
                                max,
                                nullCounts.isNullAt(fieldIndex)
                                        ? null
                                        : nullCounts.getLong(fieldIndex));
            }
        }
        return stats;
    }

    public InternalRow evolution(BinaryRow values) {
        InternalRow row = values;
        if (indexMapping != null) {
            row = ProjectedRow.from(indexMapping).replaceRow(row);
        }

        if (castFieldGetters != null) {
            row = CastedRow.from(castFieldGetters).replaceRow(values);
        }

        return row;
    }

    public InternalArray evolution(BinaryArray nullCounts, @Nullable Long rowCount) {
        if (indexMapping == null) {
            return nullCounts;
        }

        if (rowCount == null) {
            throw new RuntimeException("Schema Evolution for stats needs row count.");
        }

        return new NullCountsEvoArray(indexMapping, nullCounts, rowCount);
    }

    public static RowType schema() {
        List<DataField> fields = new ArrayList<>();
        fields.add(new DataField(0, "_MIN_VALUES", newBytesType(false)));
        fields.add(new DataField(1, "_MAX_VALUES", newBytesType(false)));
        fields.add(new DataField(2, "_NULL_COUNTS", new ArrayType(new BigIntType(true))));
        return new RowType(fields);
    }

    private static RowType toAllFieldsNullableRowType(RowType rowType) {
        // as stated in RollingFile.Writer#finish, field stats are not collected currently so
        // min/max values are all nulls
        return RowType.builder()
                .fields(
                        rowType.getFields().stream()
                                .map(f -> f.type().copy(true))
                                .toArray(DataType[]::new),
                        rowType.getFieldNames().toArray(new String[0]))
                .build();
    }

    private static class NullCountsEvoArray implements InternalArray {

        private final int[] indexMapping;
        private final InternalArray array;
        private final long notFoundValue;

        protected NullCountsEvoArray(int[] indexMapping, InternalArray array, long notFoundValue) {
            this.indexMapping = indexMapping;
            this.array = array;
            this.notFoundValue = notFoundValue;
        }

        @Override
        public int size() {
            return indexMapping.length;
        }

        @Override
        public boolean isNullAt(int pos) {
            if (indexMapping[pos] < 0) {
                return false;
            }
            return array.isNullAt(indexMapping[pos]);
        }

        @Override
        public long getLong(int pos) {
            if (indexMapping[pos] < 0) {
                return notFoundValue;
            }
            return array.getLong(indexMapping[pos]);
        }

        // ============================= Unsupported Methods ================================

        @Override
        public boolean getBoolean(int pos) {
            throw new UnsupportedOperationException();
        }

        @Override
        public byte getByte(int pos) {
            throw new UnsupportedOperationException();
        }

        @Override
        public short getShort(int pos) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getInt(int pos) {
            throw new UnsupportedOperationException();
        }

        @Override
        public float getFloat(int pos) {
            throw new UnsupportedOperationException();
        }

        @Override
        public double getDouble(int pos) {
            throw new UnsupportedOperationException();
        }

        @Override
        public BinaryString getString(int pos) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Decimal getDecimal(int pos, int precision, int scale) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Timestamp getTimestamp(int pos, int precision) {
            throw new UnsupportedOperationException();
        }

        @Override
        public byte[] getBinary(int pos) {
            throw new UnsupportedOperationException();
        }

        @Override
        public InternalArray getArray(int pos) {
            throw new UnsupportedOperationException();
        }

        @Override
        public InternalMap getMap(int pos) {
            throw new UnsupportedOperationException();
        }

        @Override
        public InternalRow getRow(int pos, int numFields) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean equals(Object o) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int hashCode() {
            throw new UnsupportedOperationException();
        }

        @Override
        public String toString() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean[] toBooleanArray() {
            throw new UnsupportedOperationException();
        }

        @Override
        public byte[] toByteArray() {
            throw new UnsupportedOperationException();
        }

        @Override
        public short[] toShortArray() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int[] toIntArray() {
            throw new UnsupportedOperationException();
        }

        @Override
        public long[] toLongArray() {
            throw new UnsupportedOperationException();
        }

        @Override
        public float[] toFloatArray() {
            throw new UnsupportedOperationException();
        }

        @Override
        public double[] toDoubleArray() {
            throw new UnsupportedOperationException();
        }
    }
}
