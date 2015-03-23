package org.exist.indexing.rdf;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdfxml.xmlinput.SAX2Model;
import java.util.Map;
import org.apache.log4j.Logger;
import org.exist.collections.Collection;
import org.exist.dom.persistent.AbstractCharacterData;
import org.exist.dom.persistent.AttrImpl;
import org.exist.dom.persistent.DocumentImpl;
import org.exist.dom.persistent.DocumentSet;
import org.exist.dom.persistent.ElementImpl;
import org.exist.dom.persistent.IStoredNode;
import org.exist.dom.persistent.NodeProxy;
import org.exist.dom.persistent.NodeSet;
import org.exist.indexing.AbstractStreamListener;
import org.exist.indexing.IndexController;
import org.exist.indexing.IndexWorker;
import org.exist.indexing.MatchListener;
import org.exist.indexing.StreamListener;
import org.exist.security.PermissionDeniedException;
import org.exist.storage.DBBroker;
import org.exist.storage.IndexSpec;
import org.exist.storage.NodePath;
import org.exist.storage.txn.Txn;
import org.exist.util.DatabaseConfigurationException;
import org.exist.util.Occurrences;
import org.exist.xquery.QueryRewriter;
import org.exist.xquery.XQueryContext;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXParseException;
import org.xml.sax.helpers.AttributesImpl;

/**
 *
 * @author Anton Olsson <abc2386@gmail.com>
 */
public class TDBIndexWorker implements IndexWorker {

    private static final Logger LOG = Logger.getLogger(TDBIndexWorker.class);

    private final DBBroker broker;
    private final TDBRDFIndex index;
    private DocumentImpl currentDoc;
    private Model currentModel;
    private RDFIndexConfig config;
    private int mode;
    private final TDBStreamListener listener = new TDBStreamListener();
    private IndexController controller;

    TDBIndexWorker(TDBRDFIndex index, DBBroker broker) {
        this.index = index;
        this.broker = broker;
    }

    @Override
    public String getIndexId() {
        return index.getIndexId();
    }

    @Override
    public String getIndexName() {
        return index.getIndexName();
    }

    @Override
    public Object configure(IndexController controller, NodeList configNodes, Map<String, String> namespaces) throws DatabaseConfigurationException {
        this.controller = controller;
        LOG.debug("Configuring TDB index...");
        return new TDBIndexConfig(configNodes, namespaces);
    }

    private RDFIndexConfig getIndexConfig(Collection coll) {
        IndexSpec indexConf = coll.getIndexConfiguration(broker);
        if (indexConf != null) {
            return (RDFIndexConfig) indexConf.getCustomIndexSpec(getIndexId());
        }
        return null;
    }

    @Override
    public void setDocument(DocumentImpl doc) {
        this.currentDoc = doc;
        config = getIndexConfig(doc.getCollection());
        if (config != null) {
            // Create a copy of the original RDFIndexConfig (there's only one per db instance),
            // so we can safely work with it.
            config = new RDFIndexConfig(config);
        }
        currentModel = index.getDataset().getNamedModel(doc.getDocumentURI());
        listener.setModel(currentModel);
    }

    @Override
    public void setDocument(DocumentImpl document, int mode) {
        setDocument(document);
        setMode(mode);
    }

    @Override
    public void setMode(int mode) {
        this.mode = mode;
    }

    @Override
    public DocumentImpl getDocument() {
        return currentDoc;
    }

    @Override
    public int getMode() {
        return mode;
    }

    @Override
    public <T extends IStoredNode> IStoredNode getReindexRoot(IStoredNode<T> node, NodePath path, boolean insert, boolean includeSelf) {
        return null;
    }

    @Override
    public StreamListener getListener() {
        return listener;
    }

    @Override
    public MatchListener getMatchListener(DBBroker broker, NodeProxy proxy) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void flush() {
        currentModel.close();
    }

    @Override
    public void removeCollection(Collection collection, DBBroker broker, boolean reindex) throws PermissionDeniedException {
        RDFIndexConfig cfg = getIndexConfig(collection);
        if (cfg != null) {
            
        }
    }

    @Override
    public boolean checkIndex(DBBroker broker) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Occurrences[] scanIndex(XQueryContext context, DocumentSet docs, NodeSet contextSet, Map<?, ?> hints) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public QueryRewriter getQueryRewriter(XQueryContext context) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    private class TDBStreamListener extends AbstractStreamListener {

        private ElementImpl deferredElement;
        private AttributesImpl deferredAttribs = new AttributesImpl();
        private SAX2Model saxHandler;

        public void setModel(Model model) {
            saxHandler = null;
            try {
                //            ARPHandlers handlers = saxHandler.getHandlers();
//            handlers.setStatementHandler(model.get);
                saxHandler = SAX2Model.create(null, model);
            } catch (SAXParseException ex) {
                LOG.error(ex);
            }
        }

        @Override
        public IndexWorker getWorker() {
            return TDBIndexWorker.this;
        }

        @Override
        public void startElement(Txn transaction, ElementImpl element, NodePath path) {
            if (deferredElement != null) {
                processDeferredElement();
            }
            deferredElement = element;
            super.startElement(transaction, element, path);
        }

        @Override
        public void endElement(Txn transaction, ElementImpl element, NodePath path) {
            if (deferredElement != null) {
                processDeferredElement();
            }
            try {
                saxHandler.endElement(
                        element.getNamespaceURI(),
                        element.getLocalName(),
                        element.getQName().getStringValue());
            } catch (Exception ex) {
                LOG.error(ex);
            }
            super.endElement(transaction, element, path);
        }

        @Override
        public void attribute(Txn transaction, AttrImpl attrib, NodePath path) {
            deferredAttribs.addAttribute(
                    attrib.getNamespaceURI(),
                    attrib.getLocalName(),
                    attrib.getQName().getStringValue(),
                    AttrImpl.getAttributeType(attrib.getType()),
                    attrib.getValue()
            );
            super.attribute(transaction, attrib, path);
        }

        @Override
        public void characters(Txn transaction, AbstractCharacterData text, NodePath path) {
            try {
                saxHandler.characters(text.getData().toCharArray(), 0, text.getLength());
            } catch (Exception ex) {
                LOG.error(ex);
            }
            super.characters(transaction, text, path);
        }

        private void processDeferredElement() {
            try {
                saxHandler.startElement(
                        deferredElement.getNamespaceURI(),
                        deferredElement.getLocalName(),
                        deferredElement.getQName().getStringValue(),
                        deferredAttribs
                );
            } catch (Exception ex) {
                LOG.error(ex);
            }
        }

    }

}
