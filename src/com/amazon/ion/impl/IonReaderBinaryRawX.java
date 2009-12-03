// Copyright (c) 2009 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import com.amazon.ion.Decimal;
import com.amazon.ion.IonException;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonType;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.Timestamp;
import com.amazon.ion.Timestamp.Precision;
import com.amazon.ion.impl.IonScalarConversionsX.AS_TYPE;
import com.amazon.ion.impl.IonScalarConversionsX.ValueVariant;
import com.amazon.ion.impl.UnifiedSavePointManagerX.SavePoint;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.util.Date;
import java.util.Iterator;


/**
 *  low level reader, base class, for reading Ion binary
 *  input sources.  This using the UnifiedInputStream just
 *  as the updated (july 2009) text reader does.  The
 *  routines in this impl only include those needed to handle
 *  field id's, annotation ids', and access to the value
 *  headers.  In particular hasNext, next, stepIn and stepOut
 *  are handled here.
 *
 *  scalar values are handled by IonReaderBinarySystem and
 *  symbol tables (as well as field names and annotations as
 *  strings) are handled by IonBinaryReaderUser.
 *
 *  csuver
 *  16 July 2009
 */
abstract public class IonReaderBinaryRawX implements IonReader
{
    static final int DEFAULT_CONTAINER_STACK_SIZE = 12; // a multiple of 3
    static final int DEFAULT_ANNOTATION_SIZE = 10;
    static final int NO_LIMIT = Integer.MIN_VALUE;

    protected enum State {
        S_INVALID,
        S_BEFORE_FIELD, // only true in structs
        S_BEFORE_TID,
        S_BEFORE_VALUE,
        S_AFTER_VALUE,
        S_EOF
    }

    State               _state;

    UnifiedInputStreamX _input;
    int                 _local_remaining;
    boolean             _eof;
    boolean             _has_next_needed;
    ValueVariant        _v;
    IonType             _value_type;
    boolean             _value_is_null;
    boolean             _value_is_true;   // cached boolean value (since we step on the length)
    int                 _value_field_id;
    int                 _value_tid;
    int                 _value_len;
    int                 _value_lob_remaining;
    boolean             _value_lob_is_ready;

    UnifiedSavePointManagerX _save_points;
    SavePoint           _annotations;
    int[]               _annotation_ids;
    int                 _annotation_count;

    // local stack for stepInto() and stepOut()
    boolean             _is_in_struct;
    boolean             _struct_is_ordered;
    int                 _parent_tid;
    int                 _container_top;
    long[]              _container_stack; // triples of: position, type, local_end


    protected final void init(UnifiedInputStreamX uis) {
        _input = uis;
        _local_remaining = NO_LIMIT;
        _parent_tid = IonConstants.tidDATAGRAM;
        _value_field_id = UnifiedSymbolTable.UNKNOWN_SID;
        _state = State.S_BEFORE_TID; // this is where we always start
        _save_points = new UnifiedSavePointManagerX(uis);
        _container_stack = new long[DEFAULT_CONTAINER_STACK_SIZE];
        _annotations = _save_points.savePointAllocate();
        _v = new ValueVariant();
        _annotation_ids = new int[DEFAULT_ANNOTATION_SIZE];
        _has_next_needed = true;
    }

    static private final int  POS_OFFSET        = 0;
    static private final int  TYPE_LIMIT_OFFSET = 1;
    static private final long TYPE_MASK         = 0xffffffff;
    static private final int  LIMIT_SHIFT       = 32;
    static private final int  POS_STACK_STEP    = 2;

    private final void push(int type, long position, int local_remaining)
    {
        int oldlen = _container_stack.length;
        if ((_container_top + POS_STACK_STEP) >= oldlen) {
            int newlen = oldlen * 2;
            long[] temp = new long[newlen];
            System.arraycopy(_container_stack, 0, temp, 0, oldlen);
            _container_stack = temp;
        }
        _container_stack[_container_top + POS_OFFSET]  = position;

        long type_limit = local_remaining;
        type_limit <<= LIMIT_SHIFT;
        type_limit  |= (type & TYPE_MASK);
        _container_stack[_container_top + TYPE_LIMIT_OFFSET] = type_limit;

        _container_top += POS_STACK_STEP;
    }
    private final long get_top_position() {
        assert(_container_top > 0);
        long pos = _container_stack[(_container_top - POS_STACK_STEP) + POS_OFFSET];
        return pos;
    }
    private final int get_top_type() {
        assert(_container_top > 0);
        long type_limit = _container_stack[(_container_top - POS_STACK_STEP) + TYPE_LIMIT_OFFSET];
        int type = (int)(type_limit & TYPE_MASK);
        if (type < 0 || type > IonConstants.tidDATAGRAM) {
            error_at("invalid type id in parent stack");
        }
        return type;
    }
    private final int get_top_local_remaining() {
        assert(_container_top > 0);
        long type_limit = _container_stack[_container_top - POS_STACK_STEP + TYPE_LIMIT_OFFSET];
        int  local_remaining = (int)((type_limit >> LIMIT_SHIFT) & TYPE_MASK);
        return local_remaining;
    }
    private final void pop() {
        assert(_container_top > 0);
        _container_top -= POS_STACK_STEP;
    }

    public boolean hasNext()
    {
        if (!_eof && _has_next_needed) {
            try {
                has_next_helper_raw();
            }
            catch (IOException e) {
                error(e);
            }
        }
        return !_eof;
    }
    public IonType next()
    {
        if (_eof) {
            return null;
        }

        if (_has_next_needed) {
            try {
                has_next_helper_raw();
            }
            catch (IOException e) {
                error(e);
            }
        }

        _has_next_needed = true;
        assert( _value_type != null ); // this should only be null here if we're at eof (and then we should have failed already)
        return _value_type;
    }

    //from IonConstants
    //public static final byte[] BINARY_VERSION_MARKER_1_0 =
    //    { (byte) 0xE0,
    //      (byte) 0x01,
    //      (byte) 0x00,
    //      (byte) 0xEA };
    private static final int BINARY_VERSION_MARKER_TID = IonConstants.getTypeCode(IonConstants.BINARY_VERSION_MARKER_1_0[0]);
    private static final int BINARY_VERSION_MARKER_LEN = IonConstants.getLowNibble(IonConstants.BINARY_VERSION_MARKER_1_0[0]);


    private final void has_next_helper_raw() throws IOException
    {
        clear_value();

        while (_value_tid == -1 && !_eof) {
            switch (_state) {
            case S_BEFORE_FIELD:
                _value_field_id = read_field_id();
                if (_value_field_id == UnifiedInputStreamX.EOF) {
                    _eof = true;
                    break;
                }
                // fall through to try to read the type id right now
            case S_BEFORE_TID:
                _state = State.S_BEFORE_VALUE; // read_type_id may change this for null and bool values
                _value_tid = read_type_id();
                if (_value_tid == UnifiedInputStreamX.EOF) {
                    _state = State.S_EOF;
                    _eof = true;
                    break;
                }
                if (_value_tid == IonConstants.tidTypedecl) {
                    assert (_value_tid == (BINARY_VERSION_MARKER_TID & 0xff)); // the bvm tid happens to be type decl
                    if (_value_len == BINARY_VERSION_MARKER_LEN ) {
                        // this isn't valid for any type descriptor except the first byte
                        // of a 4 byte version marker - so lets read the rest
                        for (int ii=1; ii<IonConstants.BINARY_VERSION_MARKER_1_0.length; ii++) {
                            int b = read();
                            if (b != (IonConstants.BINARY_VERSION_MARKER_1_0[ii] & 0xff)) {
                                error_at("invalid binary image");
                            }
                        }
                        // so it's a 4 byte version marker - make it look like
                        // the symbol $ion_1_0 ...
                        _value_tid = IonConstants.tidSymbol;
                        _value_len = 0; // so skip will go the right place - here
                        _v.setValue(UnifiedSymbolTable.ION_1_0_SID);
                        _v.setAuthoritativeType(AS_TYPE.int_value);
                        _value_type = IonType.SYMBOL;
                        _value_is_null = false;
                        _value_lob_is_ready = false;
                        _annotations.clear();
                        _value_field_id = UnifiedSymbolTable.UNKNOWN_SID;
                        _state = State.S_AFTER_VALUE;
                    }
                    else {
                        // if it's not a bvm then it's an ordinary annotated value

                        // we need to skip over the annotations to read
                        // the actual type id byte for the value.  We'll
                        // save the annotations using a save point, which
                        // will pin the input buffers until we free this,
                        // not later than the next call to hasNext().

                        int alen = readVarUInt();
                        _annotations.start(getPosition(), 0);
                        skip(alen);
                        _annotations.markEnd();

                        // this will both get the type id and it will reset the
                        // length as well (over-writing the len + annotations value
                        // that is there now, before the call)
                        _value_tid = read_type_id();
                        if (_value_tid == UnifiedInputStreamX.EOF) {
                            error_at("unexpected EOF encountered where a type descriptor byte was expected");
                        }

                        _value_type = get_iontype_from_tid(_value_tid);
                        assert( _value_type != null );
                    }
                }
                else {
                    // if it's not a typedesc then we just get the IonType and we're done
                    _value_type = get_iontype_from_tid(_value_tid);
                }
                break;
            case S_BEFORE_VALUE:
                skip(_value_len);
                // fall through to "after value"
            case S_AFTER_VALUE:
                if (isInStruct()) {
                    _state = State.S_BEFORE_FIELD;
                }
                else {
                    _state = State.S_BEFORE_TID;
                }
                break;
            case S_EOF:
                break;
            default:
                error("internal error: raw binary reader in invalid state!");
            }
        }

        // we always want to exit here
        _has_next_needed = false;
        return;
    }
    protected final int load_annotations() throws IOException {
        switch (_state) {
        case S_BEFORE_VALUE:
        case S_AFTER_VALUE:
            if (_annotations.isDefined()) {
                int local_remaining_save = _local_remaining;
                _save_points.savePointPushActive(_annotations, getPosition(), 0);
                _local_remaining =  NO_LIMIT; // limit will be handled by the save point
                _annotation_count = 0;
                do {
                    int a = readVarUIntOrEOF();
                    if (a == UnifiedInputStreamX.EOF) {
                        break;
                    }
                    load_annotation_append(a);
                } while (!isEOF());
                _save_points.savePointPopActive(_annotations);
                _local_remaining = local_remaining_save;
                _annotations.clear();
            }
            // else the count stays zero (or it was previously set)
            break;
        default:
            throw new IllegalStateException("annotations require the value to be ready");
        }
        return _annotation_count;
    }
    private final void load_annotation_append(int a)
    {
        int oldlen = _annotation_ids.length;
        if (_annotation_count >= oldlen) {
            int newlen = oldlen * 2;
            int[] temp = new int[newlen];
            System.arraycopy(_annotation_ids, 0, temp, 0, oldlen);
            _annotation_ids = temp;
        }
        _annotation_ids[_annotation_count++] =  a;
    }

    private final void clear_value()
    {
        _value_type = null;
        _value_tid  = -1;
        _value_is_null = false;
        _value_lob_is_ready = false;
        _annotations.clear();
        _v.clear();
        _annotation_count = 0;
        _value_field_id = UnifiedSymbolTable.UNKNOWN_SID;
    }
    private final int read_field_id() throws IOException
    {
        int field_id = readVarUIntOrEOF();
        return field_id;
    }
    private final int read_type_id() throws IOException
    {
        int td = read();
        if (td < 0) {
            return UnifiedInputStreamX.EOF;
        }

        int tid = IonConstants.getTypeCode(td);
        int len = IonConstants.getLowNibble(td);
        if (len == IonConstants.lnIsVarLen) {
            len = readVarUInt();
        }
        else if (tid == IonConstants.tidNull) {
            if (len != IonConstants.lnIsNull) {
                error_at("invalid null type descriptor");
            }
            _value_is_null = true;
            len = 0;
            _state = State.S_AFTER_VALUE;
        }
        else if (len == IonConstants.lnIsNull) {
            _value_is_null = true;
            len = 0;
            _state = State.S_AFTER_VALUE;
        }
        else if (tid == IonConstants.tidBoolean) {
            switch (len) {
                case IonConstants.lnBooleanFalse:
                    _value_is_true = false;
                    break;
                case IonConstants.lnBooleanTrue:
                    _value_is_true = true;
                    break;
                default:
                    error_at("invalid length nibble in boolean value: "+len);
                    break;
            }
            len = 0;
            _state = State.S_AFTER_VALUE;
        }
        else if (tid == IonConstants.tidStruct) {
            if ((_struct_is_ordered = (len == 1))) {
                // special case of an ordered struct, it gets the
                // otherwise impossible to have length of 1
                len = readVarUInt();
            }
        }

        _value_tid = tid;
        _value_len = len;

        return tid;
    }
    private final IonType get_iontype_from_tid(int tid)
    {
        IonType t = null;
        switch (tid) {
        case IonConstants.tidNull:      // 0
            t = IonType.NULL;
            break;
        case IonConstants.tidBoolean:   // 1
            t = IonType.BOOL;
            break;
        case IonConstants.tidPosInt:    // 2
        case IonConstants.tidNegInt:    // 3
            t = IonType.INT;
            break;
        case IonConstants.tidFloat:     // 4
            t = IonType.FLOAT;
            break;
        case IonConstants.tidDecimal:   // 5
            t = IonType.DECIMAL;
            break;
        case IonConstants.tidTimestamp: // 6
            t = IonType.TIMESTAMP;
            break;
        case IonConstants.tidSymbol:    // 7
            t = IonType.SYMBOL;
            break;
        case IonConstants.tidString:    // 8
            t = IonType.STRING;
            break;
        case IonConstants.tidClob:      // 9
            t = IonType.CLOB;
            break;
        case IonConstants.tidBlob:      // 10 A
            t = IonType.BLOB;
            break;
        case IonConstants.tidList:      // 11 B
            t = IonType.LIST;
            break;
        case IonConstants.tidSexp:      // 12 C
            t = IonType.SEXP;
            break;
        case IonConstants.tidStruct:    // 13 D
            t = IonType.STRUCT;
            break;
        case IonConstants.tidTypedecl:  // 14 E
            t = null;  // we don't know yet
            break;
        default:
            throw new IonException("unrecognized value type encountered: "+tid);
        }
        return t;
    }

    public void stepIn()
    {
        if (_value_type == null || _eof) {
            throw new IllegalStateException();
        }
        switch (_value_type) {
        case STRUCT:
        case LIST:
        case SEXP:
            break;
        default:
            throw new IllegalStateException();
        }
        assert( _state == State.S_BEFORE_VALUE );

        // first push place where we'll take up our next
        // value processing when we step out
        long curr_position = getPosition();
        long next_position = curr_position + _value_len;
        int  next_remaining = _local_remaining;
        if (next_remaining != NO_LIMIT) {
            next_remaining -= _value_len;
            if (next_remaining < 0) {
                next_remaining = 0; // we'll see and EOF down the road  TODO: should we error now?
            }
        }
        push(_parent_tid, next_position, next_remaining);

        _is_in_struct = (_value_tid == IonConstants.tidStruct);
        _local_remaining = _value_len;
        _state = _is_in_struct ? State.S_BEFORE_FIELD : State.S_BEFORE_TID;
        _parent_tid = _value_tid;

        clear_value();
        _has_next_needed = true;
    }
    public void stepOut()
    {
        if (getDepth() < 1) {
            throw new IllegalStateException(IonMessages.CANNOT_STEP_OUT);
        }

        // first we get the top values, then we
        // pop them all off in one fell swoop.
        long next_position   = get_top_position();
        int  local_remaining = get_top_local_remaining();
        int  parent_tid      = get_top_type();

        pop();

        _eof = false;
        _parent_tid = parent_tid;
        _local_remaining = local_remaining;
        if (_parent_tid == IonConstants.tidStruct) {
            _is_in_struct = true;
            _state = State.S_BEFORE_FIELD;
        }
        else {
            _is_in_struct = false;
            _state = State.S_BEFORE_TID;
        }
        _has_next_needed = true;

        long curr_position = getPosition();
        if (next_position > curr_position) {
            try {
                long distance = next_position - curr_position;
                int  max_skip = Integer.MAX_VALUE - 1; // -1 just in case
                while (distance > max_skip) {
                    skip(max_skip);
                    distance -= max_skip;
                }
                if (distance > 0) {
                    assert( distance < Integer.MAX_VALUE );
                    skip((int)distance);
                }
            }
            catch (IOException e) {
                error(e);
            }
        }
        else if (next_position < curr_position) {
            String message = "invalid position during stepOut, current position "
                           + curr_position
                           + " next value at "
                           + next_position;
            error(message);
        }
        assert(next_position == getPosition());
    }
    public int byteSize()
    {
        int len;

        switch (_value_type) {
        case BLOB:
        case CLOB:
            break;
        default:
            throw new IllegalStateException("only valid for LOB values");
        }
        if (!_value_lob_is_ready) {
            if (_value_is_null) {
                len = 0;
            }
            else {
                len = _value_len;
            }
            _value_lob_remaining = len;
            _value_lob_is_ready = true;
        }
        return _value_lob_remaining;
    }
    public byte[] newBytes()
    {
        int len = byteSize(); // does out validation for us
        byte[] bytes;
        if (_value_is_null) {
            bytes = null;
        }
        else {
            bytes = new byte[len];
            getBytes(bytes, 0, len);
        }
        return bytes;
    }
    public int getBytes(byte[] buffer, int offset, int len)
    {
        int value_len = byteSize(); // again validation
        if (value_len > len) {
            value_len = len;
        }

        int read_len = readBytes(buffer, offset, value_len);

        return read_len;
    }
    public int readBytes(byte[] buffer, int offset, int len)
    {
        if (offset < 0 || len < 0) {
            throw new IllegalArgumentException();
        }

        int value_len = byteSize(); // again validation
        if (_value_lob_remaining > len) {
            len = _value_lob_remaining;
        }
        if (len < 1) {
            return 0;
        }

        int read_len;
        try {
            read_len = read(buffer, offset, value_len);
            _value_lob_remaining -= read_len;
        }
        catch (IOException e) {
            read_len = -1;
            error(e);
        }

        if (_value_lob_remaining == 0) {
            _state = State.S_AFTER_VALUE;
        }
        else {
            _value_len = _value_lob_remaining;
        }
        return read_len;
    }

    public int getDepth()
    {
        return (_container_top / POS_STACK_STEP);
    }
    public IonType getType()
    {
        if (_has_next_needed) {
            throw new IllegalStateException("getType() isn't valid until you have called next()");
        }
        return _value_type;
    }
    public boolean isInStruct()
    {
        return _is_in_struct;
    }
    public boolean isNullValue()
    {
        return _value_is_null;
    }

    //
    //  helper read routines - these were lifted
    //  from SimpleByteBuffer.SimpleByteReader
    //
    private final int read() throws IOException
    {
        if (_local_remaining != NO_LIMIT) {
            if (_local_remaining < 1) {
                return UnifiedInputStreamX.EOF;
            }
            _local_remaining--;
        }
        return _input.read();
    }
    private final int read(byte[] dst, int start, int len) throws IOException
    {
        if (dst == null || start < 0 || len < 0 || start + len > dst.length) {
            // no need to test this start >= dst.length ||
            // since we test start+len > dst.length which is the correct test
            throw new IllegalArgumentException();
        }
        int read;
        if (_local_remaining == NO_LIMIT) {
            read = _input.read(dst, start, len);
        }
        else {
            if (len > _local_remaining) {
                if (_local_remaining < 1) {
                    throwUnexpectedEOFException();
                }
                len = _local_remaining;
            }
            read = _input.read(dst, start, len);
            _local_remaining -= read;
        }
        return read;
    }
    private final boolean isEOF() {
        if (_local_remaining > 0) return false;
        if (_local_remaining == NO_LIMIT) {
            return _input.isEOF();
        }
        return true;
    }
    private final long getPosition() {
        long pos = _input.getPosition();
        return pos;
    }
    private final void skip(int len) throws IOException
    {
        if (len < 0) {
            // no need to test this start >= dst.length ||
            // since we test start+len > dst.length which is the correct test
            throw new IllegalArgumentException();
        }
        if (_local_remaining == NO_LIMIT) {
            _input.skip(len);
        }
        else {
            if (len > _local_remaining) {
                if (_local_remaining < 1) {
                    throwUnexpectedEOFException();
                }
                len = _local_remaining;
            }
            _input.skip(len);
            _local_remaining -= len;
        }
        return;
    }

    protected final long readULong(int len) throws IOException
    {
        long    retvalue = 0;
        int b;

        switch (len) {
        default:
            throw new IonException("value too large for Java long");
        case 8:
            if ((b = read()) < 0) throwUnexpectedEOFException();
            retvalue = (retvalue << 8) | b;
        case 7:
            if ((b = read()) < 0) throwUnexpectedEOFException();
            retvalue = (retvalue << 8) | b;
        case 6:
            if ((b = read()) < 0) throwUnexpectedEOFException();
            retvalue = (retvalue << 8) | b;
        case 5:
            if ((b = read()) < 0) throwUnexpectedEOFException();
            retvalue = (retvalue << 8) | b;
        case 4:
            if ((b = read()) < 0) throwUnexpectedEOFException();
            retvalue = (retvalue << 8) | b;
        case 3:
            if ((b = read()) < 0) throwUnexpectedEOFException();
            retvalue = (retvalue << 8) | b;
        case 2:
            if ((b = read()) < 0) throwUnexpectedEOFException();
            retvalue = (retvalue << 8) | b;
        case 1:
            if ((b = read()) < 0) throwUnexpectedEOFException();
            retvalue = (retvalue << 8) | b;
        case 0:
            // do nothing, it's just a 0 length is a 0 value
        }
        return retvalue;
    }

    // TODO: untested (as yet)
    protected final BigInteger readBigInteger(int len, boolean is_negative) throws IOException
    {
        int bitlen = len;

        BigInteger value;
        if (bitlen > 0) {
            byte[] bits = new byte[bitlen];
            read(bits, 0, bitlen);

            int signum = is_negative ? -1 : 1;
            value = new BigInteger(signum, bits);
        }
        else {
            value = BigInteger.ZERO;
        }
        return value;
    }

    protected final int readVarInt() throws IOException
    {
        int     retvalue = 0;
        boolean is_negative = false;
        int     b;

        // synthetic label "done" (yuck)
done:   for (;;) {
            // read the first byte - it has the sign bit
            if ((b = read()) < 0) throwUnexpectedEOFException();
            if ((b & 0x40) != 0) {
                is_negative = true;
            }
            retvalue = (b & 0x3F);
            if ((b & 0x80) != 0) break done;

            // for the second byte we shift our eariler bits just as much,
            // but there are fewer of them there to shift
            if ((b = read()) < 0) throwUnexpectedEOFException();
            retvalue = (retvalue << 7) | (b & 0x7F);
            if ((b & 0x80) != 0) break done;

            // for the rest, they're all the same
            if ((b = read()) < 0) throwUnexpectedEOFException();
            retvalue = (retvalue << 7) | (b & 0x7F);
            if ((b & 0x80) != 0) break done;

            // for the rest, they're all the same
            if ((b = read()) < 0) throwUnexpectedEOFException();
            retvalue = (retvalue << 7) | (b & 0x7F);
            if ((b & 0x80) != 0) break done;

            // for the rest, they're all the same
            if ((b = read()) < 0) throwUnexpectedEOFException();
            retvalue = (retvalue << 7) | (b & 0x7F);
            if ((b & 0x80) != 0) break done;

            // if we get here we have more bits than we have room for :(
            throwIntOverflowExeption();
        }
        if (is_negative) {
            retvalue = -retvalue;
        }
        return retvalue;
    }

    protected final long readVarLong() throws IOException
    {
        long    retvalue = 0;
        boolean is_negative = false;
        int     b;

        // synthetic label "done" (yuck)
done:   for (;;) {
            // read the first byte - it has the sign bit
            if ((b = read()) < 0) throwUnexpectedEOFException();
            if ((b & 0x40) != 0) {
                is_negative = true;
            }
            retvalue = (b & 0x3F);
            if ((b & 0x80) != 0) break done;

            // for the second byte we shift our eariler bits just as much,
            // but there are fewer of them there to shift
            if ((b = read()) < 0) throwUnexpectedEOFException();
            retvalue = (retvalue << 7) | (b & 0x7F);
            if ((b & 0x80) != 0) break done;

            // for the rest, they're all the same
            for (;;) {
                if ((b = read()) < 0) throwUnexpectedEOFException();
                if ((retvalue & 0xFE00000000000000L) != 0) throwIntOverflowExeption();
                retvalue = (retvalue << 7) | (b & 0x7F);
                if ((b & 0x80) != 0) break done;
            }
        }
        if (is_negative) {
            retvalue = -retvalue;
        }
        return retvalue;
    }

    /**
     * Reads an integer value, returning null to mean -0.
     * @throws IOException
     */
    protected final Integer readVarInteger() throws IOException
    {
        int     retvalue = 0;
        boolean is_negative = false;
        int     b;

        // Synthetic label "done" (yuck)
done:   for (;;) {
            // read the first byte - it has the sign bit
            if ((b = read()) < 0) throwUnexpectedEOFException();
            if ((b & 0x40) != 0) {
                is_negative = true;
            }
            retvalue = (b & 0x3F);
            if ((b & 0x80) != 0) break done;

            // for the second byte we shift our eariler bits just as much,
            // but there are fewer of them there to shift
            if ((b = read()) < 0) throwUnexpectedEOFException();
            retvalue = (retvalue << 7) | (b & 0x7F);
            if ((b & 0x80) != 0) break done;

            // for the rest, they're all the same
            if ((b = read()) < 0) throwUnexpectedEOFException();
            retvalue = (retvalue << 7) | (b & 0x7F);
            if ((b & 0x80) != 0) break done;

            // for the rest, they're all the same
            if ((b = read()) < 0) throwUnexpectedEOFException();
            retvalue = (retvalue << 7) | (b & 0x7F);
            if ((b & 0x80) != 0) break done;

            // for the rest, they're all the same
            if ((b = read()) < 0) throwUnexpectedEOFException();
            retvalue = (retvalue << 7) | (b & 0x7F);
            if ((b & 0x80) != 0) break done;

            // if we get here we have more bits than we have room for :(
            throwIntOverflowExeption();
        }

        Integer retInteger = null;
        if (is_negative) {
            if (retvalue != 0) {
                retInteger = new Integer(-retvalue);
            }
        }
        else {
            retInteger = new Integer(retvalue);
        }
        return retInteger;
    }

    protected final int readVarUIntOrEOF() throws IOException
    {
        int retvalue = 0;
        int  b;

        for (;;) { // fake loop to create a "goto done"
            if ((b = read()) < 0) {
                return UnifiedInputStreamX.EOF;
            }
            retvalue = (retvalue << 7) | (b & 0x7F);
            if ((b & 0x80) != 0) break;

            if ((b = read()) < 0) throwUnexpectedEOFException();
            retvalue = (retvalue << 7) | (b & 0x7F);
            if ((b & 0x80) != 0) break;

            if ((b = read()) < 0) throwUnexpectedEOFException();
            retvalue = (retvalue << 7) | (b & 0x7F);
            if ((b & 0x80) != 0) break;

            if ((b = read()) < 0) throwUnexpectedEOFException();
            retvalue = (retvalue << 7) | (b & 0x7F);
            if ((b & 0x80) != 0) break;

            if ((b = read()) < 0) throwUnexpectedEOFException();
            retvalue = (retvalue << 7) | (b & 0x7F);
            if ((b & 0x80) != 0) break;

            // if we get here we have more bits than we have room for :(
            throwIntOverflowExeption();
        }
        return retvalue;
    }

    protected final int readVarUInt() throws IOException
    {
        int retvalue = 0;
        int  b;

        for (;;) { // fake loop to create a "goto done"
            if ((b = read()) < 0) throwUnexpectedEOFException();
            retvalue = (retvalue << 7) | (b & 0x7F);
            if ((b & 0x80) != 0) break;

            if ((b = read()) < 0) throwUnexpectedEOFException();
            retvalue = (retvalue << 7) | (b & 0x7F);
            if ((b & 0x80) != 0) break;

            if ((b = read()) < 0) throwUnexpectedEOFException();
            retvalue = (retvalue << 7) | (b & 0x7F);
            if ((b & 0x80) != 0) break;

            if ((b = read()) < 0) throwUnexpectedEOFException();
            retvalue = (retvalue << 7) | (b & 0x7F);
            if ((b & 0x80) != 0) break;

            if ((b = read()) < 0) throwUnexpectedEOFException();
            retvalue = (retvalue << 7) | (b & 0x7F);
            if ((b & 0x80) != 0) break;

            // if we get here we have more bits than we have room for :(
            throwIntOverflowExeption();
        }
        return retvalue;
    }

    protected final double readFloat(int len) throws IOException
    {
        if (len == 0)
        {
            // special case, return pos zero
            return 0.0d;
        }

        if (len != 8)
        {
            throw new IOException("Length of float read must be 0 or 8");
        }

        long dBits = this.readULong(len);
        return Double.longBitsToDouble(dBits);
    }

    protected final long readVarULong() throws IOException
    {
        long retvalue = 0;
        int  b;

        for (;;) {
            if ((b = read()) < 0) throwUnexpectedEOFException();
            if ((retvalue & 0xFE00000000000000L) != 0) throwIntOverflowExeption();
            retvalue = (retvalue << 7) | (b & 0x7F);
            if ((b & 0x80) != 0) break;
        }
        return retvalue;
    }

    /**
     * Near clone of {@link SimpleByteBuffer.SimpleByteReader#readDecimal(int)}
     * and {@link IonBinary.Reader#readDecimalValue(IonDecimalImpl, int)}
     * so keep them in sync!
     */
    protected final Decimal readDecimal(int len) throws IOException
    {
        // TODO this doesn't seem like the right math context
        MathContext mathContext = MathContext.DECIMAL128;

        Decimal bd;

        // we only write out the '0' value as the nibble 0
        if (len == 0) {
            bd = Decimal.valueOf(0, mathContext);
        }
        else {
            // otherwise we to it the hard way ....
            int  save_limit = _local_remaining - len;
            _local_remaining = len;

            int  exponent = readVarInt();

            BigInteger value;
            int signum;
            if (_local_remaining > 0)
            {
                byte[] bits = new byte[_local_remaining];
                read(bits, 0, _local_remaining);

                signum = 1;
                if (bits[0] < 0)
                {
                    // value is negative, clear the sign
                    bits[0] &= 0x7F;
                    signum = -1;
                }
                value = new BigInteger(signum, bits);
            }
            else {
                signum = 0;
                value = BigInteger.ZERO;
            }

            // Ion stores exponent, BigDecimal uses the negation "scale"
            int scale = -exponent;
            if (value.signum() == 0 && signum == -1)
            {
                assert value.equals(BigInteger.ZERO);
                bd = Decimal.negativeZero(scale, mathContext);
            }
            else
            {
                bd = Decimal.valueOf(value, scale, mathContext);
            }

            _local_remaining = save_limit;
        }
        return bd;
    }

    protected final Timestamp readTimestamp(int len) throws IOException
    {
        if (len < 1) {
            // nothing to do here - and the timestamp will be NULL
            return null;
        }

        Timestamp val;
        Precision   p = null;
        Integer     offset = null;
        int         year = 0, month = 0, day = 0, hour = 0, minute = 0, second = 0;
        BigDecimal  frac = null;
        int         save_limit = _local_remaining - len;

        _local_remaining = len;

        // first up is the offset, which requires a special int reader
        // to return the -0 as a null Integer
        offset = readVarInteger(); // this.readVarInt7WithNegativeZero();

        // now we'll read the struct values from the input stream
        if (_local_remaining > 0) {  // FIXME remove
            // year is from 0001 to 9999
            // or 0x1 to 0x270F or 14 bits - 1 or 2 bytes
            year  = readVarUInt();
            p = Precision.YEAR; // our lowest significant option

            // now we look for hours and minutes
            if (_local_remaining > 0) {
                month = readVarUInt();
                p = Precision.MONTH;

                // now we look for hours and minutes
                if (_local_remaining > 0) {
                    day   = readVarUInt();
                    p = Precision.DAY; // our lowest significant option

                    // now we look for hours and minutes
                    if (_local_remaining > 0) {
                        hour   = readVarUInt();
                        minute = readVarUInt();
                        p = Precision.MINUTE;

                        if (_local_remaining > 0) {
                        second = readVarUInt();
                        p = Precision.SECOND;

                        if (_local_remaining > 0) {
                            // now we read in our actual "milliseconds since the epoch"
                            frac = readDecimal(_local_remaining);
                            p = Precision.FRACTION;
                            }
                        }
                     }
                }
            }
        }
        // restore out outer limit(s)
        _local_remaining  = save_limit;

        // now we let timestamp put it all together
        val = Timestamp.createFromUtcFields(p, year, month, day, hour, minute, second, frac, offset);
        return val;
    }

    protected final String readString(int len) throws IOException
    {
        // len is bytes, which is greater than or equal to java
        // chars even after utf8 to utf16 decoding nonsense
        // the char array is way faster than using string buffer
        char[] chars = new char[len];
        int    c, ii = 0;
        int    save_limit = _local_remaining - len;

        _local_remaining = len;


        while (!isEOF()) {
            c = readUnicodeScalar();
            if (c < 0) throwUnexpectedEOFException();
            if (c < 0x10000) {
                chars[ii++] = (char)c;
            }
            else { // when c is >= 0x10000 we need surrogate encoding
                chars[ii++] = (char)IonConstants.makeHighSurrogate(c);
                chars[ii++] = (char)IonConstants.makeLowSurrogate(c);
            }
        }

        _local_remaining = save_limit;

        return new String(chars, 0, ii);
    }

    private final int readUnicodeScalar() throws IOException
    {
        int c = -1, b;

        b = read();

        // ascii is all good, even -1 (eof)
        if (IonUTF8.isOneByteUTF8(b)) {
            return b;
        }

        switch(IonUTF8.getUTF8LengthFromFirstByte(b)) {
        case 2:
            // now we start gluing the multi-byte value together
            assert((b & 0xe0) == 0xc0);
            // for values from 0x80 to 0x7FF (all legal)
            int b2 = read();
            if (!IonUTF8.isContinueByteUTF8(b2)) throwUTF8Exception();
            c = IonUTF8.twoByteScalar(b, b2);
            break;
        case 3:
            assert((b & 0xf0) == 0xe0);
            // for values from 0x800 to 0xFFFFF (NOT all legal)
            b2 = read();
            if (!IonUTF8.isContinueByteUTF8(b2)) throwUTF8Exception();
            int b3 = read();
            if (!IonUTF8.isContinueByteUTF8(b3)) throwUTF8Exception();
            c = IonUTF8.threeByteScalar(b, b2, b3);
            break;
        case 4:
            assert((b & 0xf8) == 0xf0);
            // for values from 0x010000 to 0x1FFFFF (NOT all legal)
            b2 = read();
            if (!IonUTF8.isContinueByteUTF8(b2)) throwUTF8Exception();
            b3 = read();
            if (!IonUTF8.isContinueByteUTF8(b3)) throwUTF8Exception();
            int b4 = read();
            if (!IonUTF8.isContinueByteUTF8(b4)) throwUTF8Exception();
            c = IonUTF8.fourByteScalar(b, b2, b3, b4);
            if (c > 0x10FFFF) {
                throw new IonException("illegal utf value encountered in input utf-8 stream");
            }
            break;
        default:
            throwUTF8Exception();
        }
        return c;
    }
    private final void throwUTF8Exception() throws IOException
    {
        error_at("Invalid UTF-8 character encounter in a string at position ");
    }
    private final void throwUnexpectedEOFException() throws IOException {
        error_at("unexpected EOF in value");
    }
    private final void throwIntOverflowExeption() throws IOException {
        error_at("int in stream is too long for a Java int 32 use readLong()");
    }


    //
    // public methods that typically user level methods
    // these are filled in by either the system reader
    // or the user reader.  Here they just fail.
    //
    public BigInteger bigIntegerValue()
    {
        throw new IonReaderBinaryExceptionX("E_NOT_IMPL");
    }
    public BigDecimal bigDecimalValue()
    {
        throw new IonReaderBinaryExceptionX("E_NOT_IMPL");
    }
    public boolean booleanValue()
    {
        throw new IonReaderBinaryExceptionX("E_NOT_IMPL");
    }
    public Date dateValue()
    {
        throw new IonReaderBinaryExceptionX("E_NOT_IMPL");
    }
    public double doubleValue()
    {
        throw new IonReaderBinaryExceptionX("E_NOT_IMPL");
    }
    public int getFieldId()
    {
        throw new IonReaderBinaryExceptionX("E_NOT_IMPL");
    }
    public String getFieldName()
    {
        throw new IonReaderBinaryExceptionX("E_NOT_IMPL");
    }
    public int getSymbolId()
    {
        throw new IonReaderBinaryExceptionX("E_NOT_IMPL");
    }
    public SymbolTable getSymbolTable()
    {
        throw new IonReaderBinaryExceptionX("E_NOT_IMPL");
    }
    public int[] getTypeAnnotationIds()
    {
        throw new IonReaderBinaryExceptionX("E_NOT_IMPL");
    }
    public String[] getTypeAnnotations()
    {
        throw new IonReaderBinaryExceptionX("E_NOT_IMPL");
    }
    public int intValue()
    {
        throw new IonReaderBinaryExceptionX("E_NOT_IMPL");
    }
    public Iterator<Integer> iterateTypeAnnotationIds()
    {
        throw new IonReaderBinaryExceptionX("E_NOT_IMPL");
    }
    public Iterator<String> iterateTypeAnnotations()
    {
        throw new IonReaderBinaryExceptionX("E_NOT_IMPL");
    }
    public long longValue()
    {
        throw new IonReaderBinaryExceptionX("E_NOT_IMPL");
    }

    public String stringValue()
    {
        throw new IonReaderBinaryExceptionX("E_NOT_IMPL");
    }
    public Timestamp timestampValue()
    {
        throw new IonReaderBinaryExceptionX("E_NOT_IMPL");
    }

    protected void error_at(String msg) {
        String msg2 = msg + " at position " + getPosition();
        throw new IonReaderBinaryExceptionX(msg2);
    }
    protected void error(String msg) {
        throw new IonReaderBinaryExceptionX(msg);
    }
    protected void error(Exception e) {
        throw new IonReaderBinaryExceptionX(e);
    }
}