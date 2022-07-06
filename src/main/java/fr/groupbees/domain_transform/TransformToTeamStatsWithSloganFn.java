package fr.groupbees.domain_transform;

import fr.groupbees.asgarde.Failure;
import fr.groupbees.domain.TeamStats;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.TupleTag;

import static java.util.Objects.requireNonNull;

public class TransformToTeamStatsWithSloganFn extends DoFn<TeamStats, TeamStats> {

    private final String pipelineStep;

    public TransformToTeamStatsWithSloganFn(String pipelineStep) {
        this.pipelineStep = pipelineStep;
    }

    private final TupleTag<TeamStats> outputTag = new TupleTag<TeamStats>() {
    };
    private final TupleTag<Failure> failuresTag = new TupleTag<Failure>() {
    };

    @ProcessElement
    public void processElement(ProcessContext ctx) {
        try {

            ctx.output(requireNonNull(ctx.element()).addSloganToStats());
        } catch (Throwable throwable) {
            final Failure failure = Failure.from(pipelineStep, ctx.element(), throwable);
            ctx.output(failuresTag, failure);
        }
    }

    public TupleTag<TeamStats> getOutputTag() {
        return outputTag;
    }

    public TupleTag<Failure> getFailuresTag() {
        return failuresTag;
    }
}
