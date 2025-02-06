/*
 * Copyright 2025 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cm.mrp.dataprocessor.transformations;

import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.INVALID_INPUT_FILE_ERROR;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.TRANSFORMATION_CONFIG_ERROR;

import com.google.cm.mrp.JobProcessorException;
import com.google.cm.mrp.backend.DataRecordProto.DataRecord;
import com.google.cm.mrp.backend.MatchConfigProto.MatchConfig;
import com.google.cm.mrp.backend.MatchConfigProto.MatchConfig.Column;
import com.google.cm.mrp.backend.MatchConfigProto.MatchConfig.MatchCondition;
import com.google.cm.mrp.backend.MatchConfigProto.MatchConfig.MatchTransformation;
import com.google.cm.mrp.backend.SchemaProto.Schema;
import com.google.cm.mrp.dataprocessor.transformations.Transformation.TransformationException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.assistedinject.Assisted;
import com.google.inject.assistedinject.AssistedInject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Transforms a DataRecord based on given transformations */
public final class DataRecordTransformerImpl implements DataRecordTransformer {

  private static final Logger logger = LoggerFactory.getLogger(DataRecordTransformerImpl.class);

  // Map of DataRecord.KeyValue index to transformations to be applied to that KeyValue.
  private final ImmutableMap<Integer, List<Transformation>> indexToTransformations;

  // Map of DataRecord.KeyValue index to Map of Transformation index (index of list above) to
  // indices of KeyValues on which the Transformation depends.
  private final ImmutableMap<Integer, Map<Integer, List<Integer>>> indexToDependentIndices;

  @AssistedInject
  public DataRecordTransformerImpl(@Assisted MatchConfig matchConfig, @Assisted Schema schema) {
    ImmutableMap<Integer, List<MatchTransformation>> indexToMatchTransformationsMap =
        getColumnIndexToMatchTransformationsMap(schema, matchConfig);
    this.indexToTransformations = convertToTransformations(indexToMatchTransformationsMap);
    this.indexToDependentIndices =
        getColumnIndexToDependentColumnIndices(indexToMatchTransformationsMap, matchConfig, schema);
  }

  /** Transform a DataRecord using the transformations stored in this Transformer. */
  public DataRecord transform(DataRecord dataRecord) {
    if (indexToTransformations.isEmpty()) {
      return dataRecord;
    }
    var transformedRecordBuilder = dataRecord.toBuilder();
    // Each index corresponds to the KeyValue to transform
    indexToTransformations.forEach(
        (idx, transformationList) -> {
          var transformedKeyValue = dataRecord.getKeyValues(idx);
          for (int transformationIdx = 0;
              transformationIdx < transformationList.size();
              ++transformationIdx) {
            Transformation transformation = transformationList.get(transformationIdx);
            List<Integer> dependentIndices = ImmutableList.of();
            // First look for the index in the map. Then the transformation.
            if (indexToDependentIndices.containsKey(idx)) {
              dependentIndices =
                  indexToDependentIndices
                      .get(idx)
                      .getOrDefault(transformationIdx, ImmutableList.of());
            }

            // Get the KeyValues the transformation depends, if any
            var dependentKeyValues =
                dependentIndices.stream()
                    .map(dataRecord::getKeyValues)
                    .collect(ImmutableList.toImmutableList());
            try {
              // transform the KeyValue
              transformedKeyValue =
                  transformation.transform(transformedKeyValue, dependentKeyValues);
            } catch (TransformationException e) {
              String message = "Could not transform input KeyValue";
              logger.info(message);
              throw new JobProcessorException(message, e, INVALID_INPUT_FILE_ERROR);
            }
          }
          transformedRecordBuilder.setKeyValues(idx, transformedKeyValue);
        });

    return transformedRecordBuilder.build();
  }

  private ImmutableMap<Integer, List<Transformation>> convertToTransformations(
      ImmutableMap<Integer, List<MatchTransformation>> matchTransformations) {
    return matchTransformations.entrySet().stream() // maps entries to new types
        .collect(
            ImmutableMap.toImmutableMap(
                Entry::getKey, // keep same key
                entry -> mapMatchTransformationsToTransformations(entry.getValue())));
  }

  /* Given a list of match transformation, map to transformation instances */
  private List<Transformation> mapMatchTransformationsToTransformations(
      List<MatchTransformation> matchTransformations) {
    // Map each transformationProto to an instance
    return matchTransformations.stream()
        .map(
            matchTransformation -> {
              try {
                // Get the actual transformation instance
                return TransformationProvider.getTransformationFromId(
                    matchTransformation.getTransformationId());
              } catch (TransformationException e) {
                String message = "Invalid transformation name in match config";
                logger.error(message);
                throw new JobProcessorException(message, e, TRANSFORMATION_CONFIG_ERROR);
              }
            })
        .collect(Collectors.toList());
  }

  /* Gets the all the transformations for matchConditions, keyed by index for the
   * column found the Schema */
  private ImmutableMap<Integer, List<MatchTransformation>> getColumnIndexToMatchTransformationsMap(
      Schema schema, MatchConfig matchConfig) {
    ImmutableMap.Builder<Integer, List<MatchTransformation>> columnIndexToTransformationsMap =
        ImmutableMap.builder();

    for (MatchCondition matchCondition : matchConfig.getMatchConditionsList()) {
      // For each MatchCondition, try to find the respective column in the given schema
      for (Column column : matchCondition.getDataSource1Column().getColumnsList()) {
        // Column found
        for (int i = 0; i < schema.getColumnsCount(); i++) {
          if (schema.getColumns(i).getColumnAlias().equalsIgnoreCase(column.getColumnAlias())) {
            if (column.getMatchTransformationsCount() > 0) {
              // Save transformations for column
              columnIndexToTransformationsMap.put(i, column.getMatchTransformationsList());
            }
          }
        }
      }
    }
    return columnIndexToTransformationsMap.build();
  }

  private ImmutableMap<Integer, Map<Integer, List<Integer>>> getColumnIndexToDependentColumnIndices(
      ImmutableMap<Integer, List<MatchTransformation>> matchTransformations,
      MatchConfig matchConfig,
      Schema schema) {
    ImmutableMap.Builder<Integer, Map<Integer, List<Integer>>> columnIndexToDependentIndicesMap =
        ImmutableMap.builder();

    // Get transformations that have dependents
    ImmutableMap<Integer, List<MatchTransformation>> matchTransformationsWithDependents =
        matchTransformations.entrySet().stream()
            .filter(
                entry ->
                    entry.getValue().stream()
                        .anyMatch(
                            matchTransformation ->
                                matchTransformation.getDependentColumnAliasesCount() > 0))
            .collect(ImmutableMap.toImmutableMap(Entry::getKey, Entry::getValue));

    if (matchTransformationsWithDependents.isEmpty()) {
      return ImmutableMap.of();
    }

    // A map of all columns groups
    HashMap<Integer, List<Integer>> columnGroups = new HashMap<>();
    // A map of index to the column group in which it belongs
    HashMap<Integer, Integer> indexToColumnGroupMap = new HashMap<>();

    matchConfig.getMatchConditionsList().stream()
        // get match conditions with multiple columns, so we can group indices together
        .filter(matchCondition -> matchCondition.getDataSource1Column().getColumnsCount() > 1)
        .flatMap(
            // flatten to all columns from all match conditions
            matchCondition -> matchCondition.getDataSource1Column().getColumnsList().stream())
        .forEach(
            column -> {
              // For each column in the match condition, try to find the respective column in the
              // schema
              for (int i = 0; i < schema.getColumnsCount(); i++) {
                // Column found
                if (column
                    .getColumnAlias()
                    .equalsIgnoreCase(schema.getColumns(i).getColumnAlias())) {
                  // Save column group
                  int columnGroup = schema.getColumns(i).getColumnGroup();
                  columnGroups.putIfAbsent(columnGroup, new ArrayList<>());
                  columnGroups.get(columnGroup).add(i);
                  indexToColumnGroupMap.put(i, columnGroup);
                }
              }
            });

    matchTransformationsWithDependents.forEach(
        // For each transformation, find the indices of its dependent columns
        (idx, transformations) ->
            columnIndexToDependentIndicesMap.put(
                idx,
                getTransformationIndexToDependentColumnIndicesMap(
                    transformations,
                    /* indicesInGroup= */ columnGroups.get(indexToColumnGroupMap.get(idx)),
                    schema)));

    return columnIndexToDependentIndicesMap.build();
  }

  /* For each transformation in list, search all column groups for its corresponding dependent
   * columns to get the index of each of these columns. Collect all these to a list and map back to the
   * transformation (using the transformation ID) */
  private Map<Integer, List<Integer>> getTransformationIndexToDependentColumnIndicesMap(
      List<MatchTransformation> matchTransformations, List<Integer> indicesInGroup, Schema schema) {
    return IntStream.range(0, matchTransformations.size())
        .boxed()
        .collect(
            // Create a map that groups matchTransformations by index
            // Apply mapping function to each matchTransformation to get indices of its dependent
            // columns
            Collectors.groupingBy(
                idx -> idx,
                Collectors.flatMapping(
                    idx ->
                        findIndicesOfDependentColumnsForTransformation(
                            matchTransformations.get(idx), indicesInGroup, schema),
                    Collectors.toList())));
  }

  /* Find dependent indices for each transformation */
  private Stream<Integer> findIndicesOfDependentColumnsForTransformation(
      MatchTransformation matchTransformation, List<Integer> indicesInGroup, Schema schema) {
    ImmutableList.Builder<Integer> dependentIndex = ImmutableList.builder();
    // Get aliases of dependent Columns
    var dependentAliases = matchTransformation.getDependentColumnAliasesList();
    // For each alias, try to get the index of that alias in the column group
    for (String dependentAlias : dependentAliases) {
      for (Integer indexForColumnInGroup : indicesInGroup) {
        if (schema
            .getColumns(indexForColumnInGroup)
            .getColumnAlias()
            .equalsIgnoreCase(dependentAlias)) {
          dependentIndex.add(indexForColumnInGroup);
        }
      }
    }
    return dependentIndex.build().stream();
  }
}
