package fr.groupbees.domain_transform;

import fr.groupbees.asgarde.Failure;
import fr.groupbees.domain.TeamStats;
import fr.groupbees.domain.TeamStatsRaw;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.WithFailures.Result;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;

import static org.apache.beam.sdk.values.TypeDescriptor.of;

public class TeamStatsTransform extends PTransform<PCollection<TeamStatsRaw>, Result<PCollection<TeamStats>, Failure>> {

    @Override
    public Result<PCollection<TeamStats>, Failure> expand(PCollection<TeamStatsRaw> input) {
        Result<PCollection<TeamStatsRaw>, Failure> res1 = input.apply("Validate fields", MapElements
                .into(of(TeamStatsRaw.class))
                .via(TeamStatsRaw::validateFields)
                .exceptionsInto(of(Failure.class))
                .exceptionsVia(exElt -> Failure.from("Validate fields", exElt)));

        PCollection<TeamStatsRaw> output1 = res1.output();
        PCollection<Failure> failure1 = res1.failures();

        Result<PCollection<TeamStats>, Failure> res2 = output1.apply("Compute team stats", MapElements
                .into(of(TeamStats.class))
                .via(TeamStats::computeTeamStats)
                .exceptionsInto(of(Failure.class))
                .exceptionsVia(exElt -> Failure.from("Compute team stats", exElt)));

        PCollection<TeamStats> output2 = res2.output();
        PCollection<Failure> failure2 = res2.failures();

        Result<PCollection<TeamStats>, Failure> res3 = output2.apply("Add team slogan", MapElements
                .into(of(TeamStats.class))
                .via(TeamStats::addSloganToStats)
                .exceptionsInto(of(Failure.class))
                .exceptionsVia(exElt -> Failure.from("Add team slogan", exElt)));

        PCollection<TeamStats> output3 = res3.output();
        PCollection<Failure> failure3 = res3.failures();

        PCollection<Failure> allFailures = PCollectionList
                .of(failure1)
                .and(failure2)
                .and(failure3)
                .apply(Flatten.pCollections());

        return Result.of(output3, allFailures);
    }
}
