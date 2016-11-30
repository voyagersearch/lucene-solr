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
package org.apache.solr.update.processor;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.component.RealTimeGetComponent;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.CopyField;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.NumericValueFieldType;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.util.RefCounted;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @lucene.experimental
 */
public class AtomicUpdateDocumentMerger {
  
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  
  protected final IndexSchema schema;
  protected final SchemaField idField;
  
  public AtomicUpdateDocumentMerger(SolrQueryRequest queryReq) {
    schema = queryReq.getSchema();
    idField = schema.getUniqueKeyField();
  }
  
  /**
   * Utility method that examines the SolrInputDocument in an AddUpdateCommand
   * and returns true if the documents contains atomic update instructions.
   */
  public static boolean isAtomicUpdate(final AddUpdateCommand cmd) {
    SolrInputDocument sdoc = cmd.getSolrInputDocument();
    for (SolrInputField sif : sdoc.values()) {
      if (sif.getValue() instanceof Map) {
        return true;
      }
    }
    
    return false;
  }
  
  /**
   * Merges the fromDoc into the toDoc using the atomic update syntax.
   * 
   * @param fromDoc SolrInputDocument which will merged into the toDoc
   * @param toDoc the final SolrInputDocument that will be mutated with the values from the fromDoc atomic commands
   * @return toDoc with mutated values
   */
  public SolrInputDocument merge(final SolrInputDocument fromDoc, SolrInputDocument toDoc) {
    for (SolrInputField sif : fromDoc.values()) {
     Object val = sif.getValue();
      if (val instanceof Map) {
        for (Entry<String,Object> entry : ((Map<String,Object>) val).entrySet()) {
          String key = entry.getKey();
          Object fieldVal = entry.getValue();
          boolean updateField = false;
          switch (key) {
            case "add":
              updateField = true;
              doAdd(toDoc, sif, fieldVal);
              break;
            case "set":
              updateField = true;
              doSet(toDoc, sif, fieldVal);
              break;
            case "remove":
              updateField = true;
              doRemove(toDoc, sif, fieldVal);
              break;
            case "removeregex":
              updateField = true;
              doRemoveRegex(toDoc, sif, fieldVal);
              break;
            case "inc":
              updateField = true;
              doInc(toDoc, sif, fieldVal);
              break;
            default:
              //Perhaps throw an error here instead?
              log.warn("Unknown operation for the an atomic update, operation ignored: " + key);
              break;
          }
          // validate that the field being modified is not the id field.
          if (updateField && idField.getName().equals(sif.getName())) {
            throw new SolrException(ErrorCode.BAD_REQUEST, "Invalid update of id field: " + sif);
          }

        }
      } else {
        // normal fields are treated as a "set"
        toDoc.put(sif.getName(), sif);
      }
    }
    
    return toDoc;
  }

  /**
   * Given a schema field, return whether or not such a field is supported for an in-place update.
   * Note: If an update command has updates to only supported fields (and _version_ is also supported),
   * only then is such an update command executed as an in-place update.
   */
  private static boolean isSupportedFieldForInPlaceUpdate(SchemaField schemaField) {
    if (schemaField == null)
      return false;
    return !(schemaField.indexed() || schemaField.stored() || !schemaField.hasDocValues() || 
        schemaField.multiValued() || !(schemaField.getType() instanceof NumericValueFieldType));
  }
  
  /**
   * Get the realtime searcher's view of the non stored dvs in the index.
   */
  private static Set<String> getSearcherNonStoredDVs(SolrCore core) {
    RefCounted<SolrIndexSearcher> holder = core.getRealtimeSearcher();
    try {
      SolrIndexSearcher searcher = holder.get();
      return Collections.unmodifiableSet(searcher.getNonStoredDVs(false));
    } finally {
      holder.decref();
    }
  }
  
  /**
   * Given an add update command, is it suitable for an in-place update operation? If so, return the updated fields
   * 
   * @return If this is an in-place update, return a set of fields that require in-place update.
   *         If this is not an in-place update, return an empty set.
   */
  public static Set<String> isInPlaceUpdate(AddUpdateCommand cmd) {
    SolrInputDocument sdoc = cmd.getSolrInputDocument();
    BytesRef id = cmd.getIndexedId();
    IndexSchema schema = cmd.getReq().getSchema();
    
    Set<String> candidateFields = new HashSet<>();

    // Whether this update command has any update to a supported field. A supported update requires the value be a map.
    boolean hasAMap = false;

    // first pass, check the things that are virtually free,
    // and bail out early if anything is obviously not a valid in-place update
    for (String fieldName : sdoc.getFieldNames()) {
      if (schema.getUniqueKeyField().getName().equals(fieldName)
          || fieldName.equals(DistributedUpdateProcessor.VERSION_FIELD)) {
        continue;
      }
      Object fieldValue = sdoc.getField(fieldName).getValue();
      if (! (fieldValue instanceof Map) ) {
        // not even an atomic update, definitely not an in-place update
        return Collections.emptySet();
      }
      // else it's a atomic update map...
      for (String op : ((Map<String, Object>)fieldValue).keySet()) {
        if (!op.equals("set") && !op.equals("inc")) {
          // not a supported in-place update op
          return Collections.emptySet();
        }
      }
      candidateFields.add(fieldName);
    }

    if (candidateFields.isEmpty()) {
      return Collections.emptySet();
    }

    // second pass over the candidates for in-place updates
    // this time more expensive checks
    for (String fieldName: candidateFields) {
      SchemaField schemaField = schema.getFieldOrNull(fieldName);

      if (!isSupportedFieldForInPlaceUpdate(schemaField)) {
        return Collections.emptySet();
      } 

      // if this field has copy target which is not supported for in place, then false
      for (CopyField copyField: schema.getCopyFieldsList(fieldName)) {
        if (!isSupportedFieldForInPlaceUpdate(copyField.getDestination()))
          return Collections.emptySet();
      }

      if (schema.isDynamicField(fieldName)) {
        try {
          RefCounted<IndexWriter> holder = cmd.getReq().getCore().getSolrCoreState().getIndexWriter(cmd.getReq().getCore());
          try {
            IndexWriter iw = holder.get();
            if (iw.getFieldNames().contains(fieldName) == false)
              return Collections.emptySet(); // if dynamic field and this field doesn't exist, DV update can't work
          } finally {
            holder.decref();
          }
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    }

    return candidateFields;
  }
  
  /**
   * Given an AddUpdateCommand containing update operations (e.g. set, inc), merge and resolve the operations into
   * a partial document that can be used for indexing the in-place updates. The AddUpdateCommand is modified to contain
   * the partial document (instead of the original document which contained the update operations) and also
   * the prevVersion that this in-place update depends on.
   * Note: updatedFields passed into the method can be changed, i.e. the version field can be added to the set.
   * @return If in-place update cannot succeed, e.g. if the old document is deleted recently, then false is returned. A false
   *        return indicates that this update can be re-tried as a full atomic update. Returns true if the in-place update
   *        succeeds.
   */
  public boolean doInPlaceUpdateMerge(AddUpdateCommand cmd, Set<String> updatedFields) throws IOException {
    SolrInputDocument inputDoc = cmd.getSolrInputDocument();
    BytesRef idBytes = cmd.getIndexedId();

    updatedFields.add(DistributedUpdateProcessor.VERSION_FIELD); // add the version field so that it is fetched too
    SolrInputDocument oldDocument = RealTimeGetComponent.getInputDocument(cmd.getReq().getCore(),
                                              idBytes, true, updatedFields, false); // avoid stored fields from index
    if (oldDocument == RealTimeGetComponent.DELETED || oldDocument == null) {
      // This doc was deleted recently. In-place update cannot work, hence a full atomic update should be tried.
      return false;
    }

    // If oldDocument doesn't have a field that is present in updatedFields, 
    // then fetch the field from RT searcher into oldDocument.
    // This can happen if the oldDocument was fetched from tlog, but the DV field to be
    // updated was not in that document.
    if (oldDocument.getFieldNames().containsAll(updatedFields) == false) {
      RefCounted<SolrIndexSearcher> searcherHolder = null;
      try {
        searcherHolder = cmd.getReq().getCore().getRealtimeSearcher();
        SolrIndexSearcher searcher = searcherHolder.get();
        int docid = searcher.getFirstMatch(new Term(idField.getName(), idBytes));
        if (docid >= 0) {
          searcher.decorateDocValueFields(oldDocument, docid, updatedFields);
        } else {
          // Not all fields needed for DV updates were found in the document obtained
          // from tlog, and the document wasn't found in the index.
          return false; // do a full atomic update
        }
      } finally {
        if (searcherHolder != null) {
          searcherHolder.decref();
        }
      }
    }

    if (oldDocument.containsKey(DistributedUpdateProcessor.VERSION_FIELD) == false) {
      throw new SolrException (ErrorCode.INVALID_STATE, "There is no _version_ in previous document. id=" + 
          cmd.getPrintableId());
    }
    Long oldVersion = (Long) oldDocument.remove(DistributedUpdateProcessor.VERSION_FIELD).getValue();

    // Copy over all supported DVs from oldDocument to partialDoc
    SolrInputDocument partialDoc = new SolrInputDocument();
    String uniqueKeyField = schema.getUniqueKeyField().getName();
    for (String fieldName : oldDocument.getFieldNames()) {
      SchemaField schemaField = schema.getField(fieldName);
      if (fieldName.equals(uniqueKeyField) || isSupportedFieldForInPlaceUpdate(schemaField)) {
        partialDoc.addField(fieldName, oldDocument.getFieldValue(fieldName));
      }
    }
    
    merge(inputDoc, partialDoc);

    // Populate the id field if not already populated (this can happen since stored fields were avoided during fetch from RTGC)
    if (!partialDoc.containsKey(schema.getUniqueKeyField().getName())) {
      partialDoc.addField(idField.getName(), 
          inputDoc.getField(schema.getUniqueKeyField().getName()).getFirstValue());
    }

    cmd.prevVersion = oldVersion;
    cmd.solrDoc = partialDoc;
    return true;
  }

  protected void doSet(SolrInputDocument toDoc, SolrInputField sif, Object fieldVal) {
    SchemaField sf = schema.getField(sif.getName());
    toDoc.setField(sif.getName(), sf.getType().toNativeType(fieldVal), sif.getBoost());
  }

  protected void doAdd(SolrInputDocument toDoc, SolrInputField sif, Object fieldVal) {
    SchemaField sf = schema.getField(sif.getName());
    toDoc.addField(sif.getName(), sf.getType().toNativeType(fieldVal), sif.getBoost());
  }

  protected void doInc(SolrInputDocument toDoc, SolrInputField sif, Object fieldVal) {
    SolrInputField numericField = toDoc.get(sif.getName());
    if (numericField == null) {
      toDoc.setField(sif.getName(),  fieldVal, sif.getBoost());
    } else {
      // TODO: fieldtype needs externalToObject?
      String oldValS = numericField.getFirstValue().toString();
      SchemaField sf = schema.getField(sif.getName());
      BytesRefBuilder term = new BytesRefBuilder();
      sf.getType().readableToIndexed(oldValS, term);
      Object oldVal = sf.getType().toObject(sf, term.get());

      String fieldValS = fieldVal.toString();
      Number result;
      if (oldVal instanceof Long) {
        result = ((Long) oldVal).longValue() + Long.parseLong(fieldValS);
      } else if (oldVal instanceof Float) {
        result = ((Float) oldVal).floatValue() + Float.parseFloat(fieldValS);
      } else if (oldVal instanceof Double) {
        result = ((Double) oldVal).doubleValue() + Double.parseDouble(fieldValS);
      } else {
        // int, short, byte
        result = ((Integer) oldVal).intValue() + Integer.parseInt(fieldValS);
      }

      toDoc.setField(sif.getName(),  result, sif.getBoost());
    }
  }

  protected void doRemove(SolrInputDocument toDoc, SolrInputField sif, Object fieldVal) {
    final String name = sif.getName();
    SolrInputField existingField = toDoc.get(name);
    if (existingField == null) return;
    SchemaField sf = schema.getField(name);

    if (sf != null) {
      final Collection<Object> original = existingField.getValues();
      if (fieldVal instanceof Collection) {
        for (Object object : (Collection) fieldVal) {
          Object o = sf.getType().toNativeType(object);
          original.remove(o);
        }
      } else {
        original.remove(sf.getType().toNativeType(fieldVal));
      }

      toDoc.setField(name, original);
    }
  }

  protected void doRemoveRegex(SolrInputDocument toDoc, SolrInputField sif, Object valuePatterns) {
    final String name = sif.getName();
    final SolrInputField existingField = toDoc.get(name);
    if (existingField != null) {
      final Collection<Object> valueToRemove = new HashSet<>();
      final Collection<Object> original = existingField.getValues();
      final Collection<Pattern> patterns = preparePatterns(valuePatterns);
      for (Object value : original) {
        for(Pattern pattern : patterns) {
          final Matcher m = pattern.matcher(value.toString());
          if (m.matches()) {
            valueToRemove.add(value);
          }
        }
      }
      original.removeAll(valueToRemove);
      toDoc.setField(name, original);
    }
  }

  private Collection<Pattern> preparePatterns(Object fieldVal) {
    final Collection<Pattern> patterns = new LinkedHashSet<>(1);
    if (fieldVal instanceof Collection) {
      Collection<String> patternVals = (Collection<String>) fieldVal;
      for (String patternVal : patternVals) {
        patterns.add(Pattern.compile(patternVal));
      }
    } else {
      patterns.add(Pattern.compile(fieldVal.toString()));
    }
    return patterns;
  }
  
}
