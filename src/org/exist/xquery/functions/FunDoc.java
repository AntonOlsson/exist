/*
 *  eXist Open Source Native XML Database
 *  Copyright (C) 2001-03 Wolfgang M. Meier
 *  wolfgang@exist-db.org
 *  http://exist.sourceforge.net
 *  
 *  This program is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public License
 *  as published by the Free Software Foundation; either version 2
 *  of the License, or (at your option) any later version.
 *  
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Lesser General Public License for more details.
 *  
 *  You should have received a copy of the GNU Lesser General Public License
 *  along with this program; if not, write to the Free Software
 *  Foundation, Inc., 675 Mass Ave, Cambridge, MA 02139, USA.
 *  
 *  $Id$
 */
package org.exist.xquery.functions;

import java.io.IOException;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.exist.dom.DocumentImpl;
import org.exist.dom.NodeProxy;
import org.exist.dom.QName;
import org.exist.memtree.NodeImpl;
import org.exist.memtree.SAXAdapter;
import org.exist.security.Permission;
import org.exist.security.PermissionDeniedException;
import org.exist.storage.lock.Lock;
import org.exist.util.LockException;
import org.exist.xquery.Cardinality;
import org.exist.xquery.Dependency;
import org.exist.xquery.Function;
import org.exist.xquery.FunctionSignature;
import org.exist.xquery.Module;
import org.exist.xquery.XPathException;
import org.exist.xquery.XQueryContext;
import org.exist.xquery.value.Item;
import org.exist.xquery.value.Sequence;
import org.exist.xquery.value.SequenceType;
import org.exist.xquery.value.Type;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;

/**
 * Implements the built-in fn:doc() function.
 * 
 * This will be replaced by XQuery's fn:doc() function.
 * 
 * @author wolf
 */
public class FunDoc extends Function {

	public final static FunctionSignature signature =
		new FunctionSignature(
			new QName("doc", Module.BUILTIN_FUNCTION_NS),
			"Includes a document "
				+ "into the input sequence. "
				+ "eXist interprets the argument as a path pointing to a "
				+ "document in the database, as for example, '/db/shakespeare/plays/hamlet.xml'. "
				+ "If the path is relative, "
				+ "it is resolved relative to the base URI property from the static context."
				+ "Understands also standard URLs, starting with http:// , file:// , etc.",
			new SequenceType[] { new SequenceType(Type.STRING, Cardinality.ZERO_OR_ONE)},
			new SequenceType(Type.NODE, Cardinality.ZERO_OR_ONE));

	private NodeProxy cachedNode = null;
	private String cachedPath = null;
	
	/**
	 * @param context
	 * @param signature
	 */
	public FunDoc(XQueryContext context) {
		super(context, signature);
	}

	/**
	 * @see org.exist.xquery.Function#getDependencies()
	 */
	public int getDependencies() {
		return Dependency.CONTEXT_SET;
	}

	/**
	 * @see org.exist.xquery.Expression#eval(org.exist.dom.DocumentSet, org.exist.xquery.value.Sequence, org.exist.xquery.value.Item)
	 */
	public Sequence eval(Sequence contextSequence, Item contextItem)
			throws XPathException {
		Sequence arg = getArgument(0).eval(contextSequence, contextItem);
		if (arg.getLength() == 0)
			return Sequence.EMPTY_SEQUENCE;
		String path = arg.itemAt(0).getStringValue();
		if (path.length() == 0)
			throw new XPathException(getASTNode(),
					"Invalid argument to fn:doc function: empty string is not allowed here.");

		//TODO : use a better pattern matcher (Java lacks a convenient method to achieve this)
		if (path.matches("^[a-z]+://.*")) {
			
			// === standard URL ===
			//TODO : process pseudo-protocols URLs differently.
			try {
				return processURL(path);
			} catch (XPathException e) {
				throw new XPathException(getASTNode(), e.getMessage());
			}

		} else {
			
			// === document in the database ===		
		
			// relative collection Path: add the current base URI
			if (path.charAt(0) != '/')
				path = context.getBaseURI() + '/' + path;

			// check if the loaded documents should remain locked
			boolean lockOnLoad = context.lockDocumentsOnLoad();
			Lock dlock = null;

			// if the expression occurs in a nested context, we might have cached the
			// document set
			if (path.equals(cachedPath) && cachedNode != null) {
				dlock = cachedNode.getDocument().getUpdateLock();
				try {
					// wait for pending updates by acquiring a lock
					dlock.acquire(Lock.READ_LOCK);
					return cachedNode;
				} catch (LockException e) {
					throw new XPathException(getASTNode(),
							"Failed to acquire lock on document " + path);
				} finally {
					dlock.release(Lock.READ_LOCK);
				}
			}

			DocumentImpl doc = null;
			try {
				// try to open the document and acquire a lock
				doc = (DocumentImpl) context.getBroker().openDocument(path,
						Lock.READ_LOCK);
				if (doc == null) {
					//return Sequence.EMPTY_SEQUENCE;
					//no reason to have a behaviour different from docs obtained via URLs
					throw new XPathException(getASTNode(),
							"Document not found " + path);					
				}
				if (!doc.getPermissions().validate(context.getUser(),
						Permission.READ)) {
					doc.getUpdateLock().release(Lock.READ_LOCK);
					doc = null;
					throw new XPathException(getASTNode(),
							"Insufficient privileges to read resource " + path);
				}
				cachedPath = path;
				cachedNode = new NodeProxy(doc);
				if (lockOnLoad) {
					// add the document to the list of locked documents
					context.getLockedDocuments().add(doc);
				}
				return cachedNode;
			} catch (PermissionDeniedException e) {
				throw new XPathException(getASTNode(),
						"Permission denied: unable to load document " + path);
			} finally {
				// release all locks unless lockOnLoad is true
				if (!lockOnLoad && doc != null)
					doc.getUpdateLock().release(Lock.READ_LOCK);
			}
		}
	}

	/** process a standard URL :  http: , file: , ftp:   */
	private Sequence processURL(String path) throws XPathException {
		org.exist.memtree.DocumentImpl memtreeDoc = null;

        try {
			// we use eXist's in-memory DOM implementation
			SAXParserFactory factory = SAXParserFactory.newInstance();
			factory.setNamespaceAware(true);
			InputSource src = new InputSource(path);
			SAXParser parser = factory.newSAXParser();
			XMLReader reader = parser.getXMLReader();
			SAXAdapter adapter = new SAXAdapter();
			reader.setContentHandler(adapter);
			reader.parse(src);
			
			Document doc = adapter.getDocument();
			memtreeDoc = (org.exist.memtree.DocumentImpl)doc;
			memtreeDoc.setContext(context);
			return memtreeDoc;		
			
		} catch (ParserConfigurationException e) {
			throw new XPathException(e.getMessage(), e);		
		} catch (SAXException e) {
			throw new XPathException(e.getMessage(), e);	
		} catch (IOException e) {
			throw new XPathException(e.getMessage(), e);	
		}
	}

	/**
	 * @see org.exist.xquery.PathExpr#resetState()
	 */
	public void resetState() {
		cachedNode = null;
		cachedPath = null;
		getArgument(0).resetState();
	}
}
