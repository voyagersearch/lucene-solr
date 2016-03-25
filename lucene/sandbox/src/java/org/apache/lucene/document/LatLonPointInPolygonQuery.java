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
package org.apache.lucene.document;

import java.io.IOException;
import java.util.Arrays;

import org.apache.lucene.index.PointValues.IntersectVisitor;
import org.apache.lucene.index.PointValues.Relation;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.DocIdSetBuilder;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.SparseFixedBitSet;
import org.apache.lucene.spatial.util.GeoRelationUtils;
import org.apache.lucene.spatial.util.GeoUtils;

/** Finds all previously indexed points that fall within the specified polygon.
 *
 *  <p>The field must be indexed with using {@link org.apache.lucene.document.LatLonPoint} added per document.
 *
 *  @lucene.experimental */

final class LatLonPointInPolygonQuery extends Query {
  final String field;
  final double minLat;
  final double maxLat;
  final double minLon;
  final double maxLon;
  final double[] polyLats;
  final double[] polyLons;

  /** The lats/lons must be clockwise or counter-clockwise. */
  public LatLonPointInPolygonQuery(String field, double[] polyLats, double[] polyLons) {
    this.field = field;
    if (field == null) {
      throw new IllegalArgumentException("field cannot be null");
    }
    if (polyLats == null) {
      throw new IllegalArgumentException("polyLats cannot be null");
    }
    if (polyLons == null) {
      throw new IllegalArgumentException("polyLons cannot be null");
    }
    if (polyLats.length != polyLons.length) {
      throw new IllegalArgumentException("polyLats and polyLons must be equal length");
    }
    if (polyLats.length < 4) {
      throw new IllegalArgumentException("at least 4 polygon points required");
    }
    if (polyLats[0] != polyLats[polyLats.length-1]) {
      throw new IllegalArgumentException("first and last points of the polygon must be the same (it must close itself): polyLats[0]=" + polyLats[0] + " polyLats[" + (polyLats.length-1) + "]=" + polyLats[polyLats.length-1]);
    }
    if (polyLons[0] != polyLons[polyLons.length-1]) {
      throw new IllegalArgumentException("first and last points of the polygon must be the same (it must close itself): polyLons[0]=" + polyLons[0] + " polyLons[" + (polyLons.length-1) + "]=" + polyLons[polyLons.length-1]);
    }

    this.polyLats = polyLats;
    this.polyLons = polyLons;

    // TODO: we could also compute the maximal inner bounding box, to make relations faster to compute?

    double minLon = Double.POSITIVE_INFINITY;
    double minLat = Double.POSITIVE_INFINITY;
    double maxLon = Double.NEGATIVE_INFINITY;
    double maxLat = Double.NEGATIVE_INFINITY;
    for(int i=0;i<polyLats.length;i++) {
      double lat = polyLats[i];
      if (GeoUtils.isValidLat(lat) == false) {
        throw new IllegalArgumentException("polyLats[" + i + "]=" + lat + " is not a valid latitude");
      }
      minLat = Math.min(minLat, lat);
      maxLat = Math.max(maxLat, lat);
      double lon = polyLons[i];
      if (GeoUtils.isValidLon(lon) == false) {
        throw new IllegalArgumentException("polyLons[" + i + "]=" + lat + " is not a valid longitude");
      }
      minLon = Math.min(minLon, lon);
      maxLon = Math.max(maxLon, lon);
    }
    this.minLon = minLon;
    this.maxLon = maxLon;
    this.minLat = minLat;
    this.maxLat = maxLat;
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, boolean needsScores) throws IOException {

    // I don't use RandomAccessWeight here: it's no good to approximate with "match all docs"; this is an inverted structure and should be
    // used in the first pass:

    return new ConstantScoreWeight(this) {

      @Override
      public Scorer scorer(LeafReaderContext context) throws IOException {
        LeafReader reader = context.reader();
        PointValues values = reader.getPointValues();
        if (values == null) {
          // No docs in this segment had any points fields
          return null;
        }
        FieldInfo fieldInfo = reader.getFieldInfos().fieldInfo(field);
        if (fieldInfo == null) {
          // No docs in this segment indexed this field at all
          return null;
        }
        LatLonPoint.checkCompatible(fieldInfo);

        // approximation (postfiltering has not yet been applied)
        DocIdSetBuilder result = new DocIdSetBuilder(reader.maxDoc());
        // subset of documents that need no postfiltering, this is purely an optimization
        final BitSet preApproved;
        // dumb heuristic: if the field is really sparse, use a sparse impl
        if (values.getDocCount(field) * 100L < reader.maxDoc()) {
          preApproved = new SparseFixedBitSet(reader.maxDoc());
        } else {
          preApproved = new FixedBitSet(reader.maxDoc());
        }
        values.intersect(field,
                         new IntersectVisitor() {
                           @Override
                           public void visit(int docID) {
                             result.add(docID);
                             preApproved.set(docID);
                           }

                           @Override
                           public void visit(int docID, byte[] packedValue) {
                             // TODO: range checks
                             result.add(docID);
                           }

                           @Override
                           public Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
                             double cellMinLat = LatLonPoint.decodeLatitude(minPackedValue, 0);
                             double cellMinLon = LatLonPoint.decodeLongitude(minPackedValue, Integer.BYTES);
                             double cellMaxLat = LatLonPoint.decodeLatitude(maxPackedValue, 0);
                             double cellMaxLon = LatLonPoint.decodeLongitude(maxPackedValue, Integer.BYTES);

                             if (cellMinLat <= minLat && cellMaxLat >= maxLat && cellMinLon <= minLon && cellMaxLon >= maxLon) {
                               // Cell fully encloses the query
                               return Relation.CELL_CROSSES_QUERY;
                             } else  if (GeoRelationUtils.rectWithinPolyPrecise(cellMinLat, cellMaxLat, cellMinLon, cellMaxLon,
                                                                                polyLats, polyLons,
                                                                                minLat, maxLat, minLon, maxLon)) {
                               return Relation.CELL_INSIDE_QUERY;
                             } else if (GeoRelationUtils.rectCrossesPolyPrecise(cellMinLat, cellMaxLat, cellMinLon, cellMaxLon,
                                                                                polyLats, polyLons,
                                                                                minLat, maxLat, minLon, maxLon)) {
                               return Relation.CELL_CROSSES_QUERY;
                             } else {
                               return Relation.CELL_OUTSIDE_QUERY;
                             }
                           }
                         });

        DocIdSet set = result.build();
        final DocIdSetIterator disi = set.iterator();
        if (disi == null) {
          return null;
        }

        // return two-phase iterator using docvalues to postfilter candidates
        SortedNumericDocValues docValues = DocValues.getSortedNumeric(reader, field);
        TwoPhaseIterator iterator = new TwoPhaseIterator(disi) {
          @Override
          public boolean matches() throws IOException {
            int docId = disi.docID();
            if (preApproved.get(docId)) {
              return true;
            } else {
              docValues.setDocument(docId);
              int count = docValues.count();
              for (int i = 0; i < count; i++) {
                long encoded = docValues.valueAt(i);
                double docLatitude = LatLonPoint.decodeLatitude((int)(encoded >> 32));
                double docLongitude = LatLonPoint.decodeLongitude((int)(encoded & 0xFFFFFFFF));
                if (GeoRelationUtils.pointInPolygon(polyLats, polyLons, docLatitude, docLongitude)) {
                  return true;
                }
              }
              return false;
            }
          }

          @Override
          public float matchCost() {
            return 20 * polyLons.length; // TODO: make this fancier, but currently linear with number of vertices
          }
        };
        return new ConstantScoreScorer(this, score(), iterator);
      }
    };
  }

  public String getField() {
    return field;
  }

  public double getMinLat() {
    return minLat;
  }

  public double getMaxLat() {
    return maxLat;
  }

  public double getMinLon() {
    return minLon;
  }

  public double getMaxLon() {
    return maxLon;
  }

  public double[] getPolyLats() {
    return polyLats;
  }

  public double[] getPolyLons() {
    return polyLons;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;

    LatLonPointInPolygonQuery that = (LatLonPointInPolygonQuery) o;

    if (field.equals(that.field) == false) {
      return false;
    }
    if (Arrays.equals(polyLons, that.polyLons) == false) {
      return false;
    }
    if (Arrays.equals(polyLats, that.polyLats) == false) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + field.hashCode();
    result = 31 * result + Arrays.hashCode(polyLons);
    result = 31 * result + Arrays.hashCode(polyLats);
    return result;
  }

  @Override
  public String toString(String field) {
    final StringBuilder sb = new StringBuilder();
    sb.append(getClass().getSimpleName());
    sb.append(':');
    if (this.field.equals(field) == false) {
      sb.append(" field=");
      sb.append(this.field);
      sb.append(':');
    }
    sb.append(" Points: ");
    for (int i=0; i<polyLons.length; ++i) {
      sb.append("[")
        .append(polyLats[i])
        .append(", ")
        .append(polyLons[i])
        .append("] ");
    }
    return sb.toString();
  }
}
