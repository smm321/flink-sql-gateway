package com.ververica.flink.table.gateway.rest.message;

import org.apache.flink.runtime.rest.messages.RequestBody;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

/**
 * {@link RequestBody} for state parser.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class StateParserRequestBody implements RequestBody{

    private static final String STATE_URL = "state_url";

    private static final String STATE_TYPE = "state_type";

    private static final String STATE_UID = "state_uid";

    @JsonProperty(STATE_URL)
    private final String stateUrl;

    @JsonProperty(STATE_TYPE)
    private final String stateType;

    @JsonProperty(STATE_UID)
    private final String stateUid;

    public StateParserRequestBody(@JsonProperty(STATE_URL) String stateUrl,
                                  @JsonProperty(STATE_TYPE) String stateType,
                                  @JsonProperty(STATE_UID) String stateUid) {
        this.stateUrl = stateUrl;
        this.stateType = stateType;
        this.stateUid = stateUid;
    }

    public String getStateUrl(){
        return this.stateUrl;
    }

    public String getStateType(){
        return this.stateType;
    }

    public String getStateUid(){
        return this.stateUid;
    }
}
