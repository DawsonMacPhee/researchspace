/**
 * ResearchSpace
 * Copyright (C) 2021, © Trustees of the British Museum
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.

 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.researchspace.sail.rest.tna;

import java.util.List;

import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;

import org.apache.commons.compress.utils.Lists;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.iterator.CollectionIteration;
import org.eclipse.rdf4j.query.impl.MapBindingSet;
import org.researchspace.sail.rest.RESTSail;
import org.researchspace.sail.rest.RESTSailConnection;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

public class TnaDiscoveryApiRangeSearchSailConnection extends RESTSailConnection {

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ItemDetails {
        private String parentId;

        public ItemDetails() {
        }

        public void setParentId(String parentId) {
            this.parentId = parentId;
        }

        public String getParentId() {
            return parentId;
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ScopeContent {
        private String description;

        public ScopeContent() {
        }

        public String getDescription() {
            return description.replace("<scopecontent><p>", "").replace("</p></scopecontent>", "");
        }

        public void setDescription(String description) {
            this.description = description;
        }

    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ChildDetails {
        private String citableReference;
        private String id;
        private String referencePart;
        private boolean isParent;

        private ScopeContent scopeContent;

        public ChildDetails() {
        }

        public String getCitableReference() {
            return citableReference;
        }

        public void setCitableReference(String citableReference) {
            this.citableReference = citableReference;
        }

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public ScopeContent getScopeContent() {
            return scopeContent;
        }

        public void setScopeContent(ScopeContent scopeContent) {
            this.scopeContent = scopeContent;
        }

        public String getReferencePart() {
            return referencePart;
        }

        public void setReferencePart(String referencePart) {
            this.referencePart = referencePart;
        }

        public boolean getIsParent() {
            return isParent;
        }

        public void setIsParent(boolean isParent) {
            this.isParent = isParent;
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ChildrenDetails {
        private List<ChildDetails> assets;
        private boolean hasMoreAfterLast;

        public ChildrenDetails() {
        }

        public void setAssets(List<ChildDetails> assets) {
            this.assets = assets;
        }

        public List<ChildDetails> getAssets() {
            return assets;
        }

        public boolean getHasMoreAfterLast() {
            return hasMoreAfterLast;
        }

        public void setHasMoreAfterLast(boolean hasMoreAfterLast) {
            this.hasMoreAfterLast = hasMoreAfterLast;
        }
    }

    private final String DISCOVERY_DETAILS_URL = "https://discovery.nationalarchives.gov.uk/API/records/v1/details/";
    private final String DISCOVERY_CHILDREN_URL = "https://discovery.nationalarchives.gov.uk/API/records/v1/children/";

    public TnaDiscoveryApiRangeSearchSailConnection(RESTSail sailBase) {
        super(sailBase);
    }

    @Override
    protected CloseableIteration<? extends BindingSet, QueryEvaluationException> executeAndConvertResultsToBindingSet(
            ServiceParametersHolder parametersHolder) {

        try {
            IRI hasCitableReference = VF.createIRI(
                    "http://www.researchspace.com/resource/system/services/tnadiscovery/hasCitableReference");
            IRI hasDescription = VF
                    .createIRI("http://www.researchspace.com/resource/system/services/tnadiscovery/hasDescription");

            String fromItemId = parametersHolder.getInputParameters().get("from");
            String toItemId = parametersHolder.getInputParameters().get("to");
            boolean includeItems = Boolean.valueOf(parametersHolder.getInputParameters().get("includeItems"));

            WebTarget fromItemTarget = this.client.target(DISCOVERY_DETAILS_URL + fromItemId);
            ItemDetails fromItemDetails = fromItemTarget.request(MediaType.APPLICATION_JSON).get(ItemDetails.class);

            List<BindingSet> bindings = Lists.newArrayList();
            String lastId = fromItemId;
            String parentId = fromItemDetails.getParentId();

            while (!lastId.equals(toItemId)) {
                ChildrenDetails children = this.client.target(DISCOVERY_CHILDREN_URL + parentId)
                        .queryParam("batchStartRecordId", lastId).queryParam("limit", 100)
                        .request(MediaType.APPLICATION_JSON).get(ChildrenDetails.class);

                for (ChildDetails child : children.getAssets()) {
                    if (includeItems && child.isParent) {
                        bindings.addAll(
                                this.getPieceItems(child.id, parametersHolder, hasCitableReference, hasDescription));
                    } else {
                        MapBindingSet mapBindingSet = new MapBindingSet();
                        mapBindingSet.addBinding(parametersHolder.getOutputVariables().get(hasCitableReference),
                                VF.createLiteral(child.getCitableReference()));
                        mapBindingSet.addBinding(parametersHolder.getOutputVariables().get(hasDescription),
                                VF.createLiteral(child.getScopeContent().getDescription()));
                        bindings.add(mapBindingSet);
                    }

                    lastId = child.id;
                    if (child.getId().equals(toItemId)) {
                        break;
                    }
                }

                if (children.hasMoreAfterLast != true && !lastId.equals(toItemId)) {
                    // if we didn't get all pieces that we wanted from the range, then we need to
                    // find the next "parent folder" that has them in place
                    WebTarget parentOfaParentTarget = this.client.target(DISCOVERY_DETAILS_URL + parentId);
                    ItemDetails parentOfaParentDetails = parentOfaParentTarget.request(MediaType.APPLICATION_JSON)
                            .get(ItemDetails.class);

                    ChildrenDetails parentOfaParentChildren = this.client
                            .target(DISCOVERY_CHILDREN_URL + parentOfaParentDetails.parentId)
                            .queryParam("batchStartRecordId", parentId).queryParam("limit", 2)
                            .request(MediaType.APPLICATION_JSON).get(ChildrenDetails.class);

                    parentId = parentOfaParentChildren.getAssets().get(1).getId();
                }
            }
            return new CollectionIteration<BindingSet, QueryEvaluationException>(bindings);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private List<BindingSet> getPieceItems(String parentId, ServiceParametersHolder parametersHolder,
            IRI hasCitableReference, IRI hasDescription) {
        boolean all = false;
        String lastId = null;
        List<BindingSet> bindings = Lists.newArrayList();
        while (!all) {
            WebTarget childrenTarget = this.client.target(DISCOVERY_CHILDREN_URL + parentId).queryParam("limit", 100);
            if (lastId != null) {
                childrenTarget = childrenTarget.queryParam("batchStartRecordId", lastId);
            }
            ChildrenDetails children = childrenTarget.request(MediaType.APPLICATION_JSON).get(ChildrenDetails.class);

            for (ChildDetails child : children.getAssets()) {
                MapBindingSet mapBindingSet = new MapBindingSet();
                mapBindingSet.addBinding(parametersHolder.getOutputVariables().get(hasCitableReference),
                        VF.createLiteral(child.getCitableReference()));
                mapBindingSet.addBinding(parametersHolder.getOutputVariables().get(hasDescription),
                        VF.createLiteral(child.getScopeContent().getDescription()));
                bindings.add(mapBindingSet);
                lastId = child.id;
            }

            if (children.hasMoreAfterLast == false) {
                all = true;
            }
        }
        return bindings;
    }
}
