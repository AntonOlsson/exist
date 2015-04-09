package org.exist.indexing.rdf;

import java.util.HashMap;
import org.apache.log4j.Logger;
import org.exist.indexing.AbstractIndex;
import org.exist.storage.DBBroker;

/**
 *
 * @author Anton Olsson <abc2386@gmail.com>
 */
public abstract class RDFIndex extends AbstractIndex {

    public final static String ID = RDFIndex.class.getName();
    private final static Logger LOG = Logger.getLogger(RDFIndex.class);
    protected HashMap workers = new HashMap();

    @Override
    public boolean checkIndex(DBBroker broker) {
        return getWorker(broker).checkIndex(broker);
    }

    
    @Override
    public String getIndexId(){
        return ID;
    }
    
    

}
