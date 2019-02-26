package software.amazon.ion.impl;
import software.amazon.ion.IonWriter;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import software.amazon.ion.IonCatalog;
import software.amazon.ion.IonReader;
import software.amazon.ion.IonType;
import software.amazon.ion.SymbolToken;
import software.amazon.ion.SymbolTable;
import software.amazon.ion.Timestamp;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.ArrayDeque;
import java.util.LinkedList;


public class PrivateWriterLSTFactory {

    private enum State {preImp, imp, impMID, impName, impVer,  preSym, sym}

    private class SSTImport {
        String name;
        int version;
        int maxID;

    }

    public class WriterLSTFactory implements PrivateIonWriter {
        private int depth;
        private SymbolTable symbolTable;
        private State state;
        private LinkedList<SSTImport> decImports;
        private LinkedList<String> decSymbols;
        private IonCatalog catalog;
        private boolean seenImports;

        WriterLSTFactory(SymbolTable currentLST, IonCatalog inCatalog) {
            symbolTable = currentLST;
            catalog = inCatalog;
        }

        WriterLSTFactory(List<SymbolTable> imports, List<String> symbols, IonCatalog inCatalog) {
            catalog = inCatalog;
            symbolTable = new LocalSymbolTable(new LocalSymbolTableImports(imports), symbols);
        }

        public SymbolTable getSymbolTable() {
            return symbolTable;
        }

        public void stepIn(IonType containerType) throws IOException {
            depth++;
            switch(state) {
                case preImp:
                    //entering the decImports list
                    state = State.imp;
                    decImports = new LinkedList<SSTImport>();
                    //we need to delete all context when we step out?
                    break;
                case imp:
                    //entering an import struct
                    decImports.add(new SSTImport());
                    break;
                case preSym:
                    decSymbols = new LinkedList<String>();
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
                            throw new UnsupportedOperationException("Open content unsupported via the managed binary writer");
                    }
            }
        }

        public void stepOut() {
            depth--;
            switch(state) {
                case imp:
                    SSTImport temp = decImports.getLast();
                    if(temp.maxID == 0 || temp.name == null || temp.version == 0) throw new UnsupportedOperationException("Illegal Shared Symbol Table Import declared in local symbol table." + temp.name + "." + temp.version);
                    LinkedList<SymbolTable> tempImports = new LinkedList<SymbolTable>();
                    SymbolTable tempTable = null;
                    for(SSTImport desc : decImports) {
                        tempTable = catalog.getTable(desc.name, desc.version);
                        if(tempTable == null) tempTable = new SubstituteSymbolTable(desc.name, desc.version, desc.maxID);
                        tempImports.add(tempTable);
                    }

                    symbolTable = new LocalSymbolTable(new LocalSymbolTableImports(tempImports), decSymbols);
                    break;
                case sym:
                    if(seenImports) {
                        for(String sym: decSymbols) {
                            symbolTable.intern(sym);
                        }

                    }
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
                if (name.equals("decImports")) {
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
                case sym:


            }
        }

        public void writeInt(long value){
            if(state == State.impVer) {
                decImports.getLast().version = (int) value;
            } else if(state == State.impMID) {
                decImports.getLast().maxID = (int) value;
            } else {
                throw new UnsupportedOperationException("Open content is unsupported via these APIs.");
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
        //we aren't really a writer
        public IonCatalog getCatalog() {
            throw new UnsupportedOperationException();
        }
        public void setTypeAnnotations(String... annotations) {
            throw new UnsupportedOperationException();
        }
        public void setTypeAnnotationSymbols(SymbolToken... annotations){
            throw new UnsupportedOperationException();
        }
        public void flush() throws IOException { throw new UnsupportedOperationException(); }
        public void finish() throws IOException { throw new UnsupportedOperationException(); }
        public void close() throws IOException { throw new UnsupportedOperationException(); }
        public boolean isFieldNameSet() {
            throw new UnsupportedOperationException();
        }
        public void writeIonVersionMarker(){
            throw new UnsupportedOperationException();
        }
        public boolean isStreamCopyOptimized(){
            throw new UnsupportedOperationException();
        }
        public void writeValue(IonReader reader) throws IOException { throw new UnsupportedOperationException(); }
        public void writeValues(IonReader reader) throws IOException { throw new UnsupportedOperationException(); }
        public void writeBool(boolean value) throws IOException { throw new UnsupportedOperationException(); }
        public void writeFloat(double value) throws IOException { throw new UnsupportedOperationException(); }
        public void writeDecimal(BigDecimal value) throws IOException { throw new UnsupportedOperationException(); }
        public void writeTimestamp(Timestamp value) throws IOException { throw new UnsupportedOperationException(); }
        public void writeSymbol(String content) throws IOException { throw new UnsupportedOperationException(); }
        public void writeSymbolToken(SymbolToken content) throws IOException { throw new UnsupportedOperationException(); }
        public void writeClob(byte[] value) throws IOException { throw new UnsupportedOperationException(); }
        public void writeClob(byte[] value, int start, int len) throws IOException { throw new UnsupportedOperationException(); }
        public void writeBlob(byte[] value) throws IOException { throw new UnsupportedOperationException(); }
        public void writeBlob(byte[] value, int start, int len) throws IOException { throw new UnsupportedOperationException(); }
    }
}
