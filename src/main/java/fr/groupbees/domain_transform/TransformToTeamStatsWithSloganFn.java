package fr.groupbees.domain_transform;

import com.fasterxml.jackson.core.type.TypeReference;
import fr.groupbees.asgarde.Failure;
import fr.groupbees.domain.TeamStats;
import lombok.val;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static java.util.Objects.requireNonNull;

public class TransformToTeamStatsWithSloganFn extends DoFn<TeamStats, TeamStats> {

    private static final Logger LOGGER = LoggerFactory.getLogger(TransformToTeamStatsWithSloganFn.class);

    private final String pipelineStep;
    private final PCollectionView<String> slogansSideInput;

    public TransformToTeamStatsWithSloganFn(String pipelineStep,
                                            PCollectionView<String> slogansSideInput) {
        this.pipelineStep = pipelineStep;
        this.slogansSideInput = slogansSideInput;
    }

    private final TupleTag<TeamStats> outputTag = new TupleTag<TeamStats>() {
    };
    private final TupleTag<Failure> failuresTag = new TupleTag<Failure>() {
    };

    @Setup
    public void setup() {
        LOGGER.info("####################Start add slogan");
    }

    @ProcessElement
    public void processElement(ProcessContext ctx) {
        try {
            final String slogans = ctx.sideInput(slogansSideInput);

            val ref = new TypeReference<Map<String, String>>() {
            };

            final Map<String, String> slogansAsMap = JsonUtil.deserializeToMap(slogans, ref);

            ctx.output(requireNonNull(ctx.element()).addSloganToStats(slogansAsMap));
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
