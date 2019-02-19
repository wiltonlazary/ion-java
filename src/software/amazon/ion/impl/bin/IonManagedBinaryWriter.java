/*
 * Copyright 2015-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at:
 *
 *     http://aws.amazon.com/apache2.0/
 *
 * or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 */

package software.amazon.ion.impl.bin;

import static java.util.Collections.unmodifiableList;
import static software.amazon.ion.IonType.LIST;
import static software.amazon.ion.IonType.STRUCT;
import static software.amazon.ion.SystemSymbols.IMPORTS_SID;
import static software.amazon.ion.SystemSymbols.ION_1_0_MAX_ID;
import static software.amazon.ion.SystemSymbols.ION_1_0_SID;
import static software.amazon.ion.SystemSymbols.ION_SYMBOL_TABLE_SID;
import static software.amazon.ion.SystemSymbols.MAX_ID_SID;
import static software.amazon.ion.SystemSymbols.NAME_SID;
import static software.amazon.ion.SystemSymbols.SYMBOLS_SID;
import static software.amazon.ion.SystemSymbols.VERSION_SID;
import static software.amazon.ion.impl.bin.Symbols.symbol;
import static software.amazon.ion.impl.bin.Symbols.systemSymbol;
import static software.amazon.ion.impl.bin.Symbols.systemSymbolTable;
import static software.amazon.ion.impl.bin.Symbols.systemSymbols;

import software.amazon.ion.*;
import software.amazon.ion.impl.PrivateIonWriter;
import software.amazon.ion.impl.PrivateLocalSymbolTableTrampoline;
import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.*;

import software.amazon.ion.impl.bin.IonRawBinaryWriter.StreamCloseMode;
import software.amazon.ion.impl.bin.IonRawBinaryWriter.StreamFlushMode;



/** Wraps {@link IonRawBinaryWriter} with symbol table management. */
/*package*/ final class IonManagedBinaryWriter extends AbstractIonWriter
{

    private interface SymbolResolver
    {
        /** Resolves a {@link SymbolToken} or returns <code>null</code> if the mapping does not exist. */
        SymbolToken get(String text);
    }

    private interface SymbolResolverBuilder
    {
        /**
         * Adds the given table's mappings to the resolver to be constructed.
         *
         * @param  startSid     The starting local ID.
         * @return the next available ID.
         */
        int addSymbolTable(SymbolTable table, int startSid);

        /** Constructs the resolver from the symbols tables added prior to this call. */
        SymbolResolver build();
    }
    private enum State {preImp, imp, impMID, impName, impVer,  preSym, sym}
    private class SSTImport {
        String name;
        int version;
        int maxID;

    }

    private class LSTFactory implements PrivateIonWriter {

        private int depth;
        private SymbolTable symbolTable;
        private State state;
        private LinkedList<SSTImport> imports;

        LSTFactory(SymbolTable currentLST) {
            symbolTable = currentLST;
        }

        LSTFactory(List<SymbolTable> imports, Iterator<String> symbols) {
            symbolTable = PrivateLocalSymbolTableTrampoline.LST(imports, symbols);
        }

        public SymbolTable getSymbolTable() {
            return symbolTable;
        }

        public void stepIn(IonType containerType) throws IOException {
            depth++;
            switch(state) {
                case preImp:
                    //entering the imports list
                    state = State.imp;
                    imports = new LinkedList<SSTImport>();
                    //we need to delete all context when we step out?
                    break;
                case imp:
                    //entering an import struct
                    imports.add(new SSTImport());
                    break;
                case preSym:
                    //entering the local symbols list
                    break;
                case impMID:
                case impVer:
                case impName:
                    throw new UnsupportedOperationException();
                default:
                    switch(containerType) {
                        case LIST:

                        case STRUCT:

                        case SEXP:
                            throw new UnsupportedOperationException();
                    }
            }
        }

        public void stepOut() {
            depth--;
            switch(state) {
                case imp:
                    SSTImport temp = imports.getLast();
                    if(temp.maxID == 0 || temp.name == null || temp.version == 0) throw new UnsupportedOperationException("Illegal Shared Symbol Table Import declared in local symbol table.");
                    break;
                case sym:

                case preSym:
                case impMID:
                case impVer:
                case impName:
                    throw new UnsupportedOperationException();
            }
            if(depth == 1) state = null;
        }

        public void setFieldName(String name) {
            if(state == null) {
                if (name.equals("imports")) {
                    state = State.preImp;
                } else if (name.equals("symbols")) {
                    state = State.preSym;
                } else {
                    throw new UnsupportedOperationException();
                }
            } else if(state == State.imp) {

            }else if(state == State.sym) {

            } else {
                throw new UnsupportedOperationException();
            }
        }

        public void writeString(String value) throws IOException {
            if state == null;
            switch(state) {

            }
        }

        public void writeInt(long value){
            if(state == State.impVer) {
                version = (int) value;
            } else if(state == State.impMID) {
                maxID = (int) value;
            } else {
                throw new UnsupportedOperationException();
            }
        }

        public void writeInt(BigInteger value) throws IOException {
            writeInt(value.longValue());
        }

        public int getDepth() {
            return depth;
        }

        public void writeNull() throws IOException {

        }

        public void writeNull(IonType type) throws IOException {

        }
        //This isn't really fulfilling the contract, but we're destroying any open content anyway so screw'em.
        public boolean isInStruct() {
            return state == null || state == State.imp;
        }

        public void addTypeAnnotation(String annotation) {
            throw new UnsupportedOperationException();
        }

        public void setFieldNameSymbol(SymbolToken name) {
           setFieldName(name.getText());
        }

        public IonCatalog getCatalog() {
            throw new UnsupportedOperationException();
        }

        public void setTypeAnnotations(String... annotations) {
            throw new UnsupportedOperationException();
        }

        public void setTypeAnnotationSymbols(SymbolToken... annotations){
            throw new UnsupportedOperationException();
        }

        public void flush() throws IOException {
            throw new UnsupportedOperationException();
        }

        public void finish() throws IOException {
            throw new UnsupportedOperationException();
        }

        public void close() throws IOException {
            throw new UnsupportedOperationException();
        }

        public boolean isFieldNameSet() {
            throw new UnsupportedOperationException();
        }

        public void writeIonVersionMarker(){
            throw new UnsupportedOperationException();
        }

        public boolean isStreamCopyOptimized(){
            throw new UnsupportedOperationException();
        }

        public void writeValue(IonReader reader) throws IOException {
            throw new UnsupportedOperationException();
        }

        public void writeValues(IonReader reader) throws IOException {
            throw new UnsupportedOperationException();
        }

        public void writeBool(boolean value) throws IOException {
            throw new UnsupportedOperationException();
        }

        public void writeFloat(double value) throws IOException {
            throw new UnsupportedOperationException();
        }

        public void writeDecimal(BigDecimal value) throws IOException {
            throw new UnsupportedOperationException();
        }

        public void writeTimestamp(Timestamp value) throws IOException {
            throw new UnsupportedOperationException();
        }

        public void writeSymbol(String content) throws IOException {
            throw new UnsupportedOperationException();
        }

        public void writeSymbolToken(SymbolToken content) throws IOException {
            throw new UnsupportedOperationException();
        }

        public void writeClob(byte[] value) throws IOException {
            throw new UnsupportedOperationException();
        }

        public void writeClob(byte[] value, int start, int len) throws IOException {
            throw new UnsupportedOperationException();
        }

        public void writeBlob(byte[] value) throws IOException {
            throw new UnsupportedOperationException();
        }

        public void writeBlob(byte[] value, int start, int len) throws IOException {
            throw new UnsupportedOperationException();
        }

    }

    private static final class ImportTablePosition
    {
        public final SymbolTable table;
        public final int startId;

        public ImportTablePosition(final SymbolTable table, final int startId)
        {
            this.table = table;
            this.startId = startId;
        }
    }

    /** Determines how imported symbols are resolved (including system symbols). */
    /*package*/ enum ImportedSymbolResolverMode
    {
        /** Symbols are copied into a flat map, this is useful if the context can be reused across builders. */
        FLAT
        {
            @Override
            /*package*/ SymbolResolverBuilder createBuilder()
            {
                final Map<String, SymbolToken> symbols = new HashMap<String, SymbolToken>();

                // add in system tokens
                for (final SymbolToken token : systemSymbols())
                {
                    symbols.put(token.getText(), token);
                }

                return new SymbolResolverBuilder()
                {
                    public int addSymbolTable(final SymbolTable table, final int startSid)
                    {
                        int maxSid = startSid;
                        final Iterator<String> iter = table.iterateDeclaredSymbolNames();
                        while (iter.hasNext())
                        {
                            final String text = iter.next();
                            if (text != null && !symbols.containsKey(text))
                            {
                                symbols.put(text, symbol(text, maxSid));
                            }
                            maxSid++;
                        }
                        return maxSid;
                    }

                    public SymbolResolver build()
                    {
                        return new SymbolResolver()
                        {
                            public SymbolToken get(final String text)
                            {
                                return symbols.get(text);
                            }
                        };
                    }
                };
            }
        },
        /** Delegates to a set of symbol tables for symbol resolution, this is useful if the context is thrown away frequently. */
        DELEGATE
        {
            @Override
            /*package*/ SymbolResolverBuilder createBuilder()
            {
                final List<ImportTablePosition> imports = new ArrayList<ImportTablePosition>();
                imports.add(new ImportTablePosition(systemSymbolTable(), 1));
                return new SymbolResolverBuilder()
                {
                    public int addSymbolTable(final SymbolTable table, final int startId)
                    {
                        imports.add(new ImportTablePosition(table, startId));
                        return startId + table.getMaxId();
                    }

                    public SymbolResolver build()
                    {
                        return new SymbolResolver() {
                            public SymbolToken get(final String text) {
                                for (final ImportTablePosition tableImport : imports)
                                {
                                    final SymbolToken token = tableImport.table.find(text);
                                    if (token != null)
                                    {
                                        return symbol(text, token.getSid() + tableImport.startId - 1);
                                    }
                                }
                                return null;
                            }
                        };
                    }
                };
            }
        };

        /*package*/ abstract SymbolResolverBuilder createBuilder();
    }

    /**
     * Provides the import context for the writer.
     * This class is immutable and shareable across instances.
     */
    /*package*/ static final class ImportedSymbolContext
    {
        public final List<SymbolTable>          parents;
        public final SymbolResolver             importedSymbols;
        public final int                        localSidStart;

        /*package*/ ImportedSymbolContext(final ImportedSymbolResolverMode mode, final List<SymbolTable> imports)
        {

            final List<SymbolTable> mutableParents = new ArrayList<SymbolTable>(imports.size());

            final SymbolResolverBuilder builder = mode.createBuilder();

            // add in imports
            int maxSid = ION_1_0_MAX_ID + 1;
            for (final SymbolTable st : imports)
            {
                if (!st.isSharedTable())
                {
                    throw new IonException("Imported symbol table is not shared: " + st);
                }
                if (st.isSystemTable())
                {
                    // ignore
                    continue;
                }
                mutableParents.add(st);
                maxSid = builder.addSymbolTable(st, maxSid);
            }

            this.parents = unmodifiableList(mutableParents);
            this.importedSymbols = builder.build();
            this.localSidStart = maxSid;
        }
    }
    /*package*/ static final ImportedSymbolContext ONLY_SYSTEM_IMPORTS =
        new ImportedSymbolContext(ImportedSymbolResolverMode.FLAT, Collections.<SymbolTable>emptyList());

    private static final SymbolTable[] EMPTY_SYMBOL_TABLE_ARRAY = new SymbolTable[0];


    private final IonCatalog                    catalog;
    private final ImportedSymbolContext         bootstrapImports;
    private ImportedSymbolContext               imports;
    private int                                 lstIndex;
    private SymbolTable                         lst;
    private boolean                             localsLocked;
    private final IonRawBinaryWriter            symbols;
    private final IonRawBinaryWriter            user;
    private boolean                             closed;
    private boolean                             IVM;
    private boolean                             writeLST;
    private IonRawBinaryWriter                  user;
    private LSTWriter                           symbols;
    private PrivateIonWriter                    currentWriter;


    /*package*/ IonManagedBinaryWriter(final PrivateIonManagedBinaryWriterBuilder builder,
                                       final OutputStream out)
                                       throws IOException
    {
        super(builder.optimization);
        this.symbols = new IonRawBinaryWriter(
            builder.provider,
            builder.symbolsBlockSize,
            out,
            WriteValueOptimization.NONE, // optimization is not relevant for the nested raw writer
            StreamCloseMode.NO_CLOSE,
            StreamFlushMode.NO_FLUSH,
            builder.preallocationMode,
            builder.isFloatBinary32Enabled
        );
        this.user = new IonRawBinaryWriter(
            builder.provider,
            builder.userBlockSize,
            out,
            WriteValueOptimization.NONE, // optimization is not relevant for the nested raw writer
            StreamCloseMode.CLOSE,
            StreamFlushMode.FLUSH,
            builder.preallocationMode,
            builder.isFloatBinary32Enabled
        );

        this.catalog = builder.catalog;
        this.bootstrapImports = builder.imports;

        this.lstIndex = 0;
        this.localsLocked = false;
        this.lst = PrivateLocalSymbolTableTrampoline.LST(Arrays.asList(builder.initialSymbolTable.getImportedTables()), builder.initialSymbolTable.iterateDeclaredSymbolNames());
        this.closed = false;
        this.IVM = true;
        this.writeLST = false;


        if (builder.initialSymbolTable != null) {
            // build import context from seeded LST
            final List<SymbolTable> lstImportList = Arrays.asList(builder.initialSymbolTable.getImportedTables());
            // TODO determine if the resolver mode should be configurable for this use case
            this.imports = new ImportedSymbolContext(ImportedSymbolResolverMode.DELEGATE, lstImportList);

            // intern all of the local symbols provided from LST
            final Iterator<String> symbolIter = builder.initialSymbolTable.iterateDeclaredSymbolNames();
            while (symbolIter.hasNext())
            {
                final String text = symbolIter.next();
                intern(text);
            }
        }
        else
        {
            this.imports = builder.imports;
        }
    }

    // Compatibility with Implementation Writer Interface

    public IonCatalog getCatalog()
    {
        return catalog;
    }

    public boolean isFieldNameSet()
    {
        return user.isFieldNameSet();
    }

    public void writeIonVersionMarker() throws IOException
    {
        // this has to force a reset of symbol table context
        // this seems wierd.
        finish();
    }

    public int getDepth()
    {
        return user.getDepth();
    }

    private void writeLocalSymbolTable() throws IOException {
        if (IVM) symbols.writeIonVersionMarker();
        symbols.addTypeAnnotationSymbol(systemSymbol(ION_SYMBOL_TABLE_SID));
        symbols.stepIn(STRUCT);
        if (imports.parents.size() > 0 && this.hasNotWritten) {
            symbols.setFieldNameSymbol(systemSymbol(IMPORTS_SID));
            symbols.stepIn(LIST);
            for (final SymbolTable st : imports.parents) {
                symbols.stepIn(STRUCT);
                symbols.setFieldNameSymbol(systemSymbol(NAME_SID));
                symbols.writeString(st.getName());
                symbols.setFieldNameSymbol(systemSymbol(VERSION_SID));
                symbols.writeInt(st.getVersion());
                symbols.setFieldNameSymbol(systemSymbol(MAX_ID_SID));
                symbols.writeInt(st.getMaxId());
                symbols.stepOut();
            }
            symbols.stepOut();
        }
        int maxId = this.lst.getMaxId();
        int importMaxId = this.lst.getImportedMaxId();
        if(importMaxId != maxId) {

            symbols.setFieldNameSymbol(systemSymbol(SYMBOLS_SID));
            symbols.stepIn(LIST);

            for(int i = this.lstIndex; i < maxId; i++){
                symbols.writeString(this.lst.findKnownSymbol(i));
            }
        }

    }
    //these should be inverted so that calling intern x text results in a new symboltoken if none currently exist, thus we can support repeated symboltokens
    private SymbolToken intern(final String text) {
        if (text == null)
        {
            return null;
        }
            SymbolToken token = imports.importedSymbols.get(text);
            if (token != null)
            {
                if (token.getSid() > ION_1_0_MAX_ID)
                {
                    // using a symbol from an import triggers emitting locals
                    writeLST = true;
                }
                return token;
            }
            // try the locals
            token = locals.get(text);
            if (token == null)
            {
                if (localsLocked)
                {
                    throw new IonException("Local symbol table was locked (made read-only)");
                }
                // if we got here, this is a new symbol and we better start up the locals
                token = symbol(text, imports.localSidStart + locals.size());
                locals.put(text, token);
                writeLST = true;
            }
            return token;
    }

    private SymbolToken intern(final SymbolToken token)
    {
        if (token == null)
        {
            return null;
        }
        final String text = token.getText();
        if (text != null)
        {
            // string content always makes us intern
            return intern(text);
        }
        final int sid = token.getSid();
        if (sid > getSymbolTable().getMaxId()) {
            // There is no slot for this symbol ID in the symbol table,
            // so an error would be raised on read. Fail early on write.
            throw new UnknownSymbolException(sid);
        }
        // no text, we just return what we got
        return token;
    }

    public SymbolTable getSymbolTable() {
        return
    }

    // Current Value Meta

    public void setFieldName(final String name)
    {
        if (!isInStruct())
        {
            throw new IllegalStateException("IonWriter.setFieldName() must be called before writing a value into a struct.");
        }
        if (name == null)
        {
            throw new NullPointerException("Null field name is not allowed.");
        }
        final SymbolToken token = intern(name);
        user.setFieldNameSymbol(token);
    }

    public void setFieldNameSymbol(SymbolToken token)
    {
        token = intern(token);
        user.setFieldNameSymbol(token);
    }

    public void setTypeAnnotations(final String... annotations)
    {
        if (annotations == null)
        {
            user.setTypeAnnotationSymbols((SymbolToken[]) null);
        }
        else
        {
            final SymbolToken[] tokens = new SymbolToken[annotations.length];
            for (int i = 0; i < tokens.length; i++)
            {
                tokens[i] = intern(annotations[i]);
            }
            user.setTypeAnnotationSymbols(tokens);
        }
    }

    public void setTypeAnnotationSymbols(final SymbolToken... annotations)
    {
        if (annotations == null)
        {
            user.setTypeAnnotationSymbols((SymbolToken[]) null);
        }
        else
        {
            for (int i = 0; i < annotations.length; i++)
            {
                annotations[i] = intern(annotations[i]);
            }
            user.setTypeAnnotationSymbols(annotations);
        }
    }

    public void addTypeAnnotation(final String annotation)
    {
        final SymbolToken token = intern(annotation);
        user.addTypeAnnotationSymbol(token);
    }

    // Container Manipulation

    public void stepIn(final IonType containerType) throws IOException
    {
        userState.beforeStepIn(this, containerType);
        user.stepIn(containerType);
    }

    public void stepOut() throws IOException
    {
        user.stepOut();
        userState.afterStepOut(this);
    }

    public boolean isInStruct()
    {
        return user.isInStruct();
    }

    // Write Value Methods

    public void writeNull() throws IOException
    {
        user.writeNull();
    }

    public void writeNull(final IonType type) throws IOException
    {
        user.writeNull(type);
    }

    public void writeBool(final boolean value) throws IOException
    {
        user.writeBool(value);
    }

    public void writeInt(long value) throws IOException
    {
        userState.writeInt(this, value);
        user.writeInt(value);
    }

    public void writeInt(final BigInteger value) throws IOException
    {
        userState.writeInt(this, value);
        user.writeInt(value);
    }

    public void writeFloat(final double value) throws IOException
    {
        user.writeFloat(value);
    }

    public void writeDecimal(final BigDecimal value) throws IOException
    {
        user.writeDecimal(value);
    }

    public void writeTimestamp(final Timestamp value) throws IOException
    {
        user.writeTimestamp(value);
    }

    public void writeSymbol(String content) throws IOException
    {
        final SymbolToken token = intern(content);
        writeSymbolToken(token);
    }

    public void writeSymbolToken(SymbolToken token) throws IOException
    {
        token = intern(token);
        if (token != null && token.getSid() == ION_1_0_SID && user.getDepth() == 0 && !user.hasAnnotations())
        {
            if (user.hasWrittenValuesSinceFinished())
            {
                // this explicitly translates SID 2 to an IVM and flushes out local symbol state
                finish();
            }
            else
            {
                // TODO determine if redundant IVM writes need to actually be surfaced
                // we need to signal that we need to write out the IVM even if nothing else is written
                forceSystemOutput = true;
            }
            return;
        }
        user.writeSymbolToken(token);
    }

    public void writeString(final String value) throws IOException
    {
        userState.writeString(this, value);
        user.writeString(value);
    }

    public void writeClob(byte[] data) throws IOException
    {
        user.writeClob(data);
    }

    public void writeClob(final byte[] data, final int offset, final int length) throws IOException
    {
        user.writeClob(data, offset, length);
    }

    public void writeBlob(byte[] data) throws IOException
    {
        user.writeBlob(data);
    }

    public void writeBlob(final byte[] data, final int offset, final int length) throws IOException
    {
        user.writeBlob(data, offset, length);
    }

    public void writeBytes(byte[] data, int off, int len) throws IOException
    {
        user.writeBytes(data, off, len);
    }

    // Stream Terminators

    public void flush() throws IOException
    {
        if (getDepth() != 0) throw new IllegalStateException("IonWriter.flush() can only be called at top-level.");
        // make sure that until the local symbol state changes we no-op the table closing routine
        // push the data out
        writeLocalSymbolTable();
        symbols.flush();
        user.flush();
    }

    public void finish() throws IOException
    {
        if (getDepth() != 0)
        {
            throw new IllegalStateException("IonWriter.finish() can only be called at top-level.");
        }
        flush();
        locals.clear();
        localsLocked = false;
        symbolState = SymbolState.SYSTEM_SYMBOLS;
        imports = bootstrapImports;
    }

    public void close() throws IOException
    {
        if (closed)
        {
            return;
        }
        closed = true;
        try
        {
            finish();
        }
        catch (IllegalStateException e)
        {
            // callers do not expect this...
        }
        finally
        {
            try
            {
                symbols.close();
            }
            finally
            {
                user.close();
            }
        }
    }
}
