package org.exist.indexing.rdf;

import org.exist.indexing.IndexWorker;
import org.exist.storage.DBBroker;
import org.exist.storage.btree.DBException;
import org.exist.util.DatabaseConfigurationException;
import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.tdb.StoreConnection;
import com.hp.hpl.jena.tdb.TDB;
import com.hp.hpl.jena.tdb.TDBFactory;
import java.io.File;
import java.io.IOException;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

/**
 *
 * @author Anton Olsson <abc2386@gmail.com>
 */
public class TDBRDFIndex extends RDFIndex {

    private Dataset dataset;
    private StoreConnection connection;
    public static String ID = "tdb-index";
    private final static Logger LOG = Logger.getLogger(TDBRDFIndex.class);

    @Override
    public void open() throws DatabaseConfigurationException {
//        this.dataset = TDBFactory.createDataset(getDataDir());
        connection = StoreConnection.make(getMyDataDir());
    }

    @Override
    public void close() throws DBException {
        if (dataset != null) {
            dataset.close();
        }
        TDB.closedown();
    }

    @Override
    public void sync() throws DBException {
        if (dataset != null) {
        }
    }

    @Override
    public void remove() throws DBException {
        close();
        // delete data directory
        try {
            String dir = getMyDataDir();
            File file = new File(dir);
            FileUtils.deleteDirectory(file);
        } catch (IOException ex) {
            LOG.error(ex);
            throw new DBException(ex.toString());
        }
    }

//    @Override
//    public String getDataDir() {
//        return super.getDataDir() + "/tdb";
//    }

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

    @Override
    public String getIndexId() {
        return ID;
    }

    private String getMyDataDir() {
        return getDataDir() + "/tdb";
    }
}
