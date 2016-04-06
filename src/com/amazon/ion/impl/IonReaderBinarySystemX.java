// Copyright (c) 2009-2013 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import static com.amazon.ion.IonType.SYMBOL;
import static com.amazon.ion.SymbolTable.UNKNOWN_SYMBOL_ID;

import com.amazon.ion.Decimal;
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonType;
import com.amazon.ion.NullValueException;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.SymbolToken;
import com.amazon.ion.Timestamp;
import com.amazon.ion.UnknownSymbolException;
import com.amazon.ion.impl._Private_ScalarConversions.AS_TYPE;
import com.amazon.ion.impl._Private_ScalarConversions.ValueVariant;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;
import java.util.Iterator;

/**
 *
 */
class IonReaderBinarySystemX
    extends IonReaderBinaryRawX
    implements _Private_ReaderWriter
{
    IonSystem _system;
    SymbolTable _symbols;
    // ValueVariant _v; actually owned by the raw reader so it can be cleared at appropriate times

    @Deprecated
    IonReaderBinarySystemX(IonSystem system, byte[] bytes, int offset, int length) {
        super();
        UnifiedInputStreamX uis = UnifiedInputStreamX.makeStream(bytes, offset, length);
        init_raw(uis);
        _system = system;
    }

    IonReaderBinarySystemX(IonSystem system, UnifiedInputStreamX in)
    {
        super();
        init_raw(in);
        _system = system;
        _symbols = system.getSystemSymbolTable();
    }


    //
    // public methods that typically user level methods
    // these are filled in by either the system reader
    // or the user reader.  Here they just fail.
    //

    public final int getFieldId()
    {
        return _value_field_id;
    }

    public SymbolToken[] getTypeAnnotationSymbols()
    {
        load_annotations();

        int count = _annotation_count;
        if (count == 0) return SymbolToken.EMPTY_ARRAY;

        SymbolTable symtab = getSymbolTable();

        SymbolToken[] result = new SymbolToken[count];
        for (int i = 0; i < count; i++)
        {
            int sid = _annotation_ids[i];
            String text = symtab.findKnownSymbol(sid);
            result[i] = new SymbolTokenImpl(text, sid);
        }

        return result;
    }

    //
    //  basic scalar value getters (for actual content)
    //
    protected final void prepare_value(int as_type) {
        if (_v.isEmpty()) {
            try {
                load_cached_value(as_type);
            }
            catch (IOException e) {
                error(e);
            }
        }
        if (as_type != 0 && !_v.hasValueOfType(as_type)) {
            // we should never get here with a symbol asking for anything other
            // than a numeric cast (from some other numeric already loaded)
            if (IonType.SYMBOL.equals(_value_type) && !ValueVariant.isNumericType(as_type)) {
                assert(IonType.SYMBOL.equals(_value_type) && !ValueVariant.isNumericType(as_type));
            }

            if (!_v.can_convert(as_type)) {
                String message = "can't cast from "
                    +_Private_ScalarConversions.getValueTypeName(_v.getAuthoritativeType())
                    +" to "
                    +_Private_ScalarConversions.getValueTypeName(as_type);
                throw new IllegalStateException(message);
            }
            int fnid = _v.get_conversion_fnid(as_type);
            _v.cast(fnid);
        }
    }

    /**
     * this checks the state of the raw reader to make sure
     * this is valid.  It also checks for an existing cached
     * value of the correct type.  It will either cast the
     * current value from an existing type to the type desired
     * or it will construct the desired type from the raw
     * input in the raw reader
     *
     * @param value_type desired value type (in local type terms)
     * @throws IOException
     */
    protected final void load_cached_value(int value_type) throws IOException
    {
        if (_v.isEmpty()) {
            load_scalar_value();
        }
    }

    /** Utility method to convert an unsigned magnitude stored as a long to a {@link BigInteger}. */
    private static BigInteger unsignedLongToBigInteger(int signum, long val)
    {
        byte[] magnitude = {
            (byte) ((val >> 56) & 0xFF),
            (byte) ((val >> 48) & 0xFF),
            (byte) ((val >> 40) & 0xFF),
            (byte) ((val >> 32) & 0xFF),
            (byte) ((val >> 24) & 0xFF),
            (byte) ((val >> 16) & 0xFF),
            (byte) ((val >>  8) & 0xFF),
            (byte) (val & 0xFF),
        };
        return new BigInteger(signum, magnitude);
    }

    static final int MAX_BINARY_LENGTH_INT = 4;
    static final int MAX_BINARY_LENGTH_LONG = 8;

    private final void load_scalar_value() throws IOException
    {
        // make sure we're trying to load a scalar value here
        switch(_value_type) {
        case NULL:
        case BOOL:
        case INT:
        case FLOAT:
        case DECIMAL:
        case TIMESTAMP:
        case SYMBOL:
        case STRING:
            break;
        default:
            return;
        }

        // this will be true when the value_type is null as
        // well as when we encounter a null of any other type
        if (_value_is_null) {
            _v.setValueToNull(_value_type);
            _v.setAuthoritativeType(AS_TYPE.null_value);
            return;
        }

        switch (_value_type) {
        default:
            return;
        case BOOL:
            _v.setValue(_value_is_true);
            _v.setAuthoritativeType(AS_TYPE.boolean_value);
            break;
        case INT:
            if (_value_len == 0) {
                int v = 0;
                _v.setValue(v);
                _v.setAuthoritativeType(AS_TYPE.int_value);
            }
            else if (_value_len <= MAX_BINARY_LENGTH_LONG) {
                long v = readULong(_value_len);

                if (v < 0) {
                    // we really can't fit this magnitude properly into a Java long
                    int signum = _value_tid == _Private_IonConstants.tidPosInt ? 1 : -1;
                    BigInteger big = unsignedLongToBigInteger(signum, v);
                    _v.setValue(big);
                    _v.setAuthoritativeType(AS_TYPE.bigInteger_value);
                } else {
                    if (_value_tid == _Private_IonConstants.tidNegInt) {
                        v = -v;
                    }
                    _v.setValue(v);
                    _v.setAuthoritativeType(AS_TYPE.long_value);
                }
            }
            else {
                boolean is_negative = (_value_tid == _Private_IonConstants.tidNegInt);
                BigInteger v = readBigInteger(_value_len, is_negative);
                _v.setValue(v);
                _v.setAuthoritativeType(AS_TYPE.bigInteger_value);
            }
            break;
        case FLOAT:
            double d;
            if (_value_len == 0) {
                d = 0.0;
            }
            else {
                d = readFloat(_value_len);
            }
            _v.setValue(d);
            _v.setAuthoritativeType(AS_TYPE.double_value);
            break;
        case DECIMAL:
            Decimal dec = readDecimal(_value_len);
            _v.setValue(dec);
            _v.setAuthoritativeType(AS_TYPE.decimal_value);
            break;
        case TIMESTAMP:
            // TODO: it looks like a 0 length return a null timestamp - is that right?
            Timestamp t = readTimestamp(_value_len);
            _v.setValue(t);
            _v.setAuthoritativeType(AS_TYPE.timestamp_value);
            break;
        case SYMBOL:
            long sid = readULong(_value_len);
            if (sid < 1 || sid > Integer.MAX_VALUE) {
                String message = "symbol id ["
                               + sid
                               + "] out of range "
                               + "(1-"
                               + Integer.MAX_VALUE
                               + ")";
                throwErrorAt(message);
            }
            // TODO: is treating this as an int too misleading?
            _v.setValue((int)sid);
            _v.setAuthoritativeType(AS_TYPE.int_value);
            break;
        case STRING:
            String s = readString(_value_len);
            _v.setValue(s);
            _v.setAuthoritativeType(AS_TYPE.string_value);
            break;
        }
        _state = State.S_AFTER_VALUE;
    }

    //
    // public value routines
    //

    @Override
    public boolean isNullValue()
    {
        return _value_is_null;
    }

    public boolean booleanValue()
    {
        prepare_value(AS_TYPE.boolean_value);
        return _v.getBoolean();
    }

    public double doubleValue()
    {
        prepare_value(AS_TYPE.double_value);
        return _v.getDouble();
    }

    public int intValue()
    {
        if (_value_type != IonType.INT &&
            _value_type != IonType.DECIMAL &&
            _value_type != IonType.FLOAT)
        {
            throw new IllegalStateException();
        }

        prepare_value(AS_TYPE.int_value);
        return _v.getInt();
    }

    public long longValue()
    {
        if (_value_type != IonType.INT &&
            _value_type != IonType.DECIMAL &&
            _value_type != IonType.FLOAT)
        {
            throw new IllegalStateException();
        }

        prepare_value(AS_TYPE.long_value);
        return _v.getLong();
    }

    public BigInteger bigIntegerValue()
    {
        if (_value_type != IonType.INT &&
            _value_type != IonType.DECIMAL &&
            _value_type != IonType.FLOAT)
        {
            throw new IllegalStateException();
        }

        if (_value_is_null) {
            return null;
        }
        prepare_value(AS_TYPE.bigInteger_value);
        return _v.getBigInteger();
    }

    public BigDecimal bigDecimalValue()
    {
        if (_value_is_null) {
            return null;
        }
        prepare_value(AS_TYPE.decimal_value);
        return _v.getBigDecimal();
    }

    public Decimal decimalValue()
    {
        if (_value_is_null) {
            return null;
        }
        prepare_value(AS_TYPE.decimal_value);
        return _v.getDecimal();
    }

    public Date dateValue()
    {
        if (_value_is_null) {
            return null;
        }
        prepare_value(AS_TYPE.date_value);
        return _v.getDate();
    }

    public Timestamp timestampValue()
    {
        if (_value_is_null) {
            return null;
        }
        prepare_value(AS_TYPE.timestamp_value);
        return _v.getTimestamp();
    }

    // TODO IONJAVA-97 this needs to use the appropriate symbol table for user values

    public final String stringValue()
    {
        if (! IonType.isText(_value_type)) throw new IllegalStateException();
        if (_value_is_null) return null;

        if (_value_type == SYMBOL) {
            if (!_v.hasValueOfType(AS_TYPE.string_value)) {
                int sid = getSymbolId();
                String name = _symbols.findKnownSymbol(sid);
                if (name == null) {
                    throw new UnknownSymbolException(sid);
                }
                _v.addValue(name);
            }
        }
        else {
            prepare_value(AS_TYPE.string_value);
        }
        return _v.getString();
    }

    public final SymbolToken symbolValue()
    {
        if (_value_type != SYMBOL) throw new IllegalStateException();
        if (_value_is_null) return null;

        int sid = getSymbolId();
        assert sid != UNKNOWN_SYMBOL_ID;
        String text = _symbols.findKnownSymbol(sid);

        return new SymbolTokenImpl(text, sid);
    }

    int getSymbolId()
    {
        if (_value_type != SYMBOL) throw new IllegalStateException();
        if (_value_is_null) throw new NullValueException();

        prepare_value(AS_TYPE.int_value);
        return _v.getInt();
    }

    public final String getFieldName()
    {
        String name;
        if (_value_field_id == SymbolTable.UNKNOWN_SYMBOL_ID) {
            name = null;
        }
        else {
            name = _symbols.findKnownSymbol(_value_field_id);
            if (name == null) {
                throw new UnknownSymbolException(_value_field_id);
            }
        }
        return name;
    }

    public final SymbolToken getFieldNameSymbol()
    {
        if (_value_field_id == SymbolTable.UNKNOWN_SYMBOL_ID) return null;
        int sid = _value_field_id;
        String text = _symbols.findKnownSymbol(sid);
        return new SymbolTokenImpl(text, sid);
    }

    public final Iterator<String> iterateTypeAnnotations()
    {
        String[] annotations = getTypeAnnotations();
        return _Private_Utils.stringIterator(annotations);
    }

    public final String[] getTypeAnnotations()
    {
        load_annotations();
        String[] anns;
        if (_annotation_count < 1) {
            anns = _Private_Utils.EMPTY_STRING_ARRAY;
        }
        else {
            anns = new String[_annotation_count];
            for (int ii=0; ii<_annotation_count; ii++) {
                anns[ii] = _symbols.findKnownSymbol(_annotation_ids[ii]);
                if (anns[ii] == null) {
                    throw new UnknownSymbolException(_annotation_ids[ii]);
                }
            }
        }
        return anns;
    }

    public final SymbolTable getSymbolTable()
    {
        return _symbols;
    }

    // system readers don't skip any symbol tables
    public SymbolTable pop_passed_symbol_table()
    {
        return null;
    }
}
