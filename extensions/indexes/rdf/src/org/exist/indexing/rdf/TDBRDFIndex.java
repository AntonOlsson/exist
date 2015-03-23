package org.exist.indexing.rdf;

import org.exist.indexing.IndexWorker;
import org.exist.storage.DBBroker;
import org.exist.storage.btree.DBException;
import org.exist.util.DatabaseConfigurationException;
import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.tdb.StoreConnection;
import com.hp.hpl.jena.tdb.TDB;
import com.hp.hpl.jena.tdb.TDBFactory;

/**
 *
 * @author Anton Olsson <abc2386@gmail.com>
 */
public class TDBRDFIndex extends RDFIndex {
    
    private Dataset dataset;
    private StoreConnection connection;

    @Override
    public void open() throws DatabaseConfigurationException {
//        this.dataset = TDBFactory.createDataset(getDataDir());
        connection = StoreConnection.make(getDataDir() + "/" + "tdb");
    }

    @Override
    public void close() throws DBException {
        if (dataset != null){
            dataset.close();
        }
        TDB.closedown();
    }

    @Override
    public void sync() throws DBException {
        if (dataset != null){
            dataset.commit();
        }
    }

    @Override
    public void remove() throws DBException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public IndexWorker getWorker(DBBroker broker) {
        TDBIndexWorker worker = (TDBIndexWorker) workers.get(broker);
        if (worker == null) {
            worker = new TDBIndexWorker(this, broker);
            workers.put(broker, worker);
        }
        return worker;
    }

    public Dataset getDataset() {
        if (dataset == null)
            dataset = TDBFactory.createDataset(connection.getLocation());
        return dataset;
    }

}
