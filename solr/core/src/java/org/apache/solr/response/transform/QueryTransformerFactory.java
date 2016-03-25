/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.response.transform;

import java.io.IOException;

import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.Query;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.BasicResultContext;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.DocList;
import org.apache.solr.search.QParser;
import org.apache.solr.search.QParserPlugin;
import org.apache.solr.search.SolrReturnFields;
import org.apache.solr.search.SyntaxError;


public class QueryTransformerFactory extends TransformerFactory
{ 
  public static String getString(String k, SolrDocument doc) {
    Object v = doc.getFirstValue(k);
    if(v != null) {
      if(v instanceof IndexableField) {
        return ((IndexableField)v).stringValue();
      }
      return v.toString();
    }
    return null;
  }
  
  @Override
  public DocTransformer create(String field, SolrParams params, SolrQueryRequest req) {
    
    Query filter = null;
    String fq = params.get("fq");
    if(fq!=null) {
      QParserPlugin qpp = req.getCore().getQueryPlugin(QParserPlugin.DEFAULT_QTYPE);
      
      QParser pp = qpp.createParser(fq, null, params, req);
      try {
        filter = pp.getQuery();
      } 
      catch (SyntaxError e) {
        throw new RuntimeException(e);
      }
    }
    
    String queryFieldName = params.get("f");
    if(queryFieldName == null) {
      throw new SolrException(ErrorCode.BAD_REQUEST, "Missing param 'j' for join field");
    }
    SchemaField sf = req.getSchema().getField(queryFieldName);
    
    
    String subfl = params.get("q.fl");
    if(subfl==null) {
      subfl = req.getParams().get("q.fl");
    }
    if(subfl==null) {
      throw new SolrException(ErrorCode.BAD_REQUEST, "Missing param 'q.fl'");
    }
    
    SolrReturnFields rf = new SolrReturnFields(subfl, req);
    return new SubQueryAugmenter(field, sf, filter, rf);
  }
}

class SubQueryAugmenter extends DocTransformer
{
  final Query filter;
  final String name;
  final SchemaField sf;
  final SolrReturnFields rf;
  
  public SubQueryAugmenter(String name, SchemaField sf, Query filter, SolrReturnFields rf) {
    this.name = name;
    this.sf = sf;
    this.rf = rf;
    this.filter = filter;
  }

  @Override
  public String[] getExtraRequestFields() {
    return new String[] {sf.getName()};
  }
  
  @Override
  public String getName() {
    return "SubQueryAugmenter";
  }

  @Override
  public void transform(SolrDocument doc, int docid, float score) throws IOException {
    String val = QueryTransformerFactory.getString("id", doc);
    
    if(val!=null) {
      Query q = sf.getType().getFieldQuery(null, sf, val);
      
      DocList docs = context.getSearcher().getDocList(q, filter, null, 0, 10);
      if(docs!=null && docs.size()>0) {
        BasicResultContext rc = new BasicResultContext(docs, rf, context.getSearcher(), q, null);
        doc.setField(name, rc);
      }
    }
  } 
}
