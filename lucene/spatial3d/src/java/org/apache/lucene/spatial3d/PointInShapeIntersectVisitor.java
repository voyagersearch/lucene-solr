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

package org.apache.lucene.spatial3d;

import org.apache.lucene.index.PointValues.IntersectVisitor;
import org.apache.lucene.index.PointValues.Relation;
import org.apache.lucene.spatial3d.geom.GeoArea;
import org.apache.lucene.spatial3d.geom.GeoAreaFactory;
import org.apache.lucene.spatial3d.geom.GeoShape;
import org.apache.lucene.spatial3d.geom.PlanetModel;
import org.apache.lucene.spatial3d.geom.XYZBounds;
import org.apache.lucene.util.DocIdSetBuilder;
import org.apache.lucene.util.NumericUtils;

class PointInShapeIntersectVisitor implements IntersectVisitor {
  private final DocIdSetBuilder hits;
  private final GeoShape shape;
  private final XYZBounds shapeBounds;
  
  public PointInShapeIntersectVisitor(DocIdSetBuilder hits, GeoShape shape, XYZBounds shapeBounds) {
    this.hits = hits;
    this.shape = shape;
    this.shapeBounds = shapeBounds;
  }

  @Override
  public void visit(int docID) {
    hits.add(docID);
  }

  @Override
  public void visit(int docID, byte[] packedValue) {
    assert packedValue.length == 12;
    double x = Geo3DPoint.decodeDimension(packedValue, 0);
    double y = Geo3DPoint.decodeDimension(packedValue, Integer.BYTES);
    double z = Geo3DPoint.decodeDimension(packedValue, 2 * Integer.BYTES);
    if (x >= shapeBounds.getMinimumX() && x <= shapeBounds.getMaximumX() &&
      y >= shapeBounds.getMinimumY() && y <= shapeBounds.getMaximumY() &&
      z >= shapeBounds.getMinimumZ() && z <= shapeBounds.getMaximumZ()) {
      if (shape.isWithin(x, y, z)) {
        hits.add(docID);
      }
    }
  }
  
  @Override
  public Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
    // Because the dimensional format operates in quantized (64 bit -> 32 bit) space, and the cell bounds
    // here are inclusive, we need to extend the bounds to the largest un-quantized values that
    // could quantize into these bounds.  The encoding (Geo3DUtil.encodeValue) does
    // a Math.round from double to long, so e.g. 1.4 -> 1, and -1.4 -> -1:
    double xMin = Geo3DUtil.decodeValueFloor(NumericUtils.sortableBytesToInt(minPackedValue, 0));
    double xMax = Geo3DUtil.decodeValueCeil(NumericUtils.sortableBytesToInt(maxPackedValue, 0));
    double yMin = Geo3DUtil.decodeValueFloor(NumericUtils.sortableBytesToInt(minPackedValue, 1 * Integer.BYTES));
    double yMax = Geo3DUtil.decodeValueCeil(NumericUtils.sortableBytesToInt(maxPackedValue, 1 * Integer.BYTES));
    double zMin = Geo3DUtil.decodeValueFloor(NumericUtils.sortableBytesToInt(minPackedValue, 2 * Integer.BYTES));
    double zMax = Geo3DUtil.decodeValueCeil(NumericUtils.sortableBytesToInt(maxPackedValue, 2 * Integer.BYTES));

    //System.out.println("  compare: x=" + cellXMin + "-" + cellXMax + " y=" + cellYMin + "-" + cellYMax + " z=" + cellZMin + "-" + cellZMax);
    assert xMin <= xMax;
    assert yMin <= yMax;
    assert zMin <= zMax;

    // First, check bounds.  If the shape is entirely contained, return CELL_CROSSES_QUERY.
    if (shapeBounds.getMinimumX() >= xMin && shapeBounds.getMaximumX() <= xMax &&
      shapeBounds.getMinimumY() >= yMin && shapeBounds.getMaximumY() <= yMax &&
      shapeBounds.getMinimumZ() >= zMin && shapeBounds.getMaximumZ() <= zMax) {
      return Relation.CELL_CROSSES_QUERY;
    }

    // Quick test failed so do slower one...
    GeoArea xyzSolid = GeoAreaFactory.makeGeoArea(PlanetModel.WGS84, xMin, xMax, yMin, yMax, zMin, zMax);

    switch(xyzSolid.getRelationship(shape)) {
    case GeoArea.CONTAINS:
      // Shape fully contains the cell
      //System.out.println("    inside");
      return Relation.CELL_INSIDE_QUERY;
    case GeoArea.OVERLAPS:
      // They do overlap but neither contains the other:
      //System.out.println("    crosses1");
      return Relation.CELL_CROSSES_QUERY;
    case GeoArea.WITHIN:
      // Cell fully contains the shape:
      //System.out.println("    crosses2");
      // return Relation.SHAPE_INSIDE_CELL;
      return Relation.CELL_CROSSES_QUERY;
    case GeoArea.DISJOINT:
      // They do not overlap at all
      //System.out.println("    outside");
      return Relation.CELL_OUTSIDE_QUERY;
    default:
      assert false;
      return Relation.CELL_CROSSES_QUERY;
    }
  }
}
