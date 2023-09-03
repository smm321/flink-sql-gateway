package com.ververica.flink.table.gateway.rest.entity;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.gson.annotations.SerializedName;
import com.ververica.flink.table.gateway.rest.result.ResultSet;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * FlinkJob for {@link ResultSet}.
 */
@NoArgsConstructor
@Data
public class FlinkJob {

    @JsonProperty("jobs")
    private List<JobsDTO> jobs;

    /**
     * JobsDTO for {@link ResultSet}.
     */
    @NoArgsConstructor
    @Data
    public static class JobsDTO {
        @JsonProperty("jid")
        private String jid;
        @JsonProperty("name")
        private String name;
        @JsonProperty("state")
        private String state;
        @JsonProperty("start-time")
        @SerializedName("start-time")
        private Long starttime;
        @JsonProperty("end-time")
        @SerializedName("end-time")
        private Long endtime;
        @JsonProperty("duration")
        private Long duration;
        @JsonProperty("last-modification")
        @SerializedName("last-modification")
        private Long lastmodification;
        @JsonProperty("tasks")
        private TasksDTO tasks;

        /**
         * TaskDTO for {@link ResultSet}.
         */
        @NoArgsConstructor
        @Data
        public static class TasksDTO {
            @JsonProperty("total")
            private Integer total;
            @JsonProperty("created")
            private Integer created;
            @JsonProperty("scheduled")
            private Integer scheduled;
            @JsonProperty("deploying")
            private Integer deploying;
            @JsonProperty("running")
            private Integer running;
            @JsonProperty("finished")
            private Integer finished;
            @JsonProperty("canceling")
            private Integer canceling;
            @JsonProperty("canceled")
            private Integer canceled;
            @JsonProperty("failed")
            private Integer failed;
            @JsonProperty("reconciling")
            private Integer reconciling;
            @JsonProperty("initializing")
            private Integer initializing;
        }
    }
}
