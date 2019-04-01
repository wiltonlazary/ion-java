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
import java.util.List;
import java.util.LinkedList;

public class LSTWriter implements PrivateIonWriter {
    private int depth;
    private SymbolTable symbolTable;
    private State state;
    private LinkedList<SSTImport> decImports;
    private LinkedList<String> decSymbols;
    private IonCatalog catalog;
    private boolean seenImports;

    private enum State {preImp, imp, impMID, impName, impVer,  preSym, sym}

    private static class SSTImport {
        String name;
        int version;
        int maxID;

    }

    public LSTWriter(SymbolTable currentLST, IonCatalog inCatalog) {
        if(!currentLST.isLocalTable()) throw new Error("LSTWriter can only be instantiated with LSTs.");
        symbolTable = currentLST;
        catalog = inCatalog;
        depth = 0;
    }

    public LSTWriter(List<SymbolTable> imports, List<String> symbols, IonCatalog inCatalog) {
        catalog = inCatalog;
        if(imports.isEmpty()){
            imports.add(PrivateUtils.systemSymtab(1));
        } else if(!imports.get(0).isSystemTable()) {
            LinkedList<SymbolTable> tempImports = new LinkedList<SymbolTable>();
            tempImports.add(PrivateUtils.systemSymtab(1));
            tempImports.addAll(imports);
            imports = tempImports;
        }
        symbolTable = new LocalSymbolTable(new LocalSymbolTableImports(imports), symbols);
        depth = 0;
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
                seenImports = true;
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
            if (name.equals("imports")) {
                state = State.preImp;
            } else if (name.equals("symbols")) {
                state = State.preSym;
            } else {
                throw new UnsupportedOperationException();
            }
        } else if(state == State.imp) {
            if(name.equals("name")) {
                state = State.impName;
            } else if(name.equals("version")) {
                state = State.impVer;
            } else if(name.equals("max_id")) {
                state = State.impMID;
            }
        } else {
            throw new UnsupportedOperationException();
        }
    }

    public void writeString(String value) {
        if(state == null) throw new UnsupportedOperationException("Open content unsupported via the managed binary writer");
        switch(state) {
            case sym:
                symbolTable.intern(value);
            default:
                throw new UnsupportedOperationException("Open content unsupported via the managed binary writer");

        }
    }

    public void writeSymbol(String content){
        if(state == State.preImp && content.equals("$ion_symbol_table")) {
            //we are appending to our current context
            seenImports = true;
            state = null;
        } else {
            throw new UnsupportedOperationException("Open content unsupported via the managed binary writer");
        }
    }
    public void writeSymbolToken(SymbolToken content) throws IOException {
        writeSymbol(content.getText());
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

    public void writeInt(BigInteger value){
        writeInt(value.longValue());
    }

    public int getDepth() {
        return depth;
    }

    public void writeNull() {

    }

    public void writeNull(IonType type){

    }
    //This isn't really fulfilling the contract, but we're destroying any open content anyway so screw'em.
    public boolean isInStruct() {
        return state == null || state == State.imp;
    }

    public void addTypeAnnotation(String annotation) {
        throw new UnsupportedOperationException();
    }

    public void addTypeAnnotationSymbol(SymbolToken annotation) {
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


    public void writeClob(byte[] value) throws IOException { throw new UnsupportedOperationException(); }
    public void writeClob(byte[] value, int start, int len) throws IOException { throw new UnsupportedOperationException(); }
    public void writeBlob(byte[] value) throws IOException { throw new UnsupportedOperationException(); }
    public void writeBlob(byte[] value, int start, int len) throws IOException { throw new UnsupportedOperationException(); }
}
