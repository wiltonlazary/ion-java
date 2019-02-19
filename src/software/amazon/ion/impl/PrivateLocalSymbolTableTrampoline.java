package software.amazon.ion.impl;
import software.amazon.ion.SymbolTable;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class PrivateLocalSymbolTableTrampoline {

    public static SymbolTable LST(List<SymbolTable> imports, Iterator<String> symbols) {
        ArrayList<String> temp =  new ArrayList<String>();
        while(symbols.hasNext()){
            temp.add(symbols.next());
        }
        return new LocalSymbolTable(new LocalSymbolTableImports(imports), temp);
    }

    public static SymbolTable substitute(SymbolTable original, int version, int maxId) {
        return new SubstituteSymbolTable(original, version, maxId);
    }
}
