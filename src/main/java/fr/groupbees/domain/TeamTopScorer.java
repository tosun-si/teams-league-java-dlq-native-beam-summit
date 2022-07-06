package fr.groupbees.domain;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Builder
@Data
@AllArgsConstructor
@NoArgsConstructor
public class TeamTopScorer implements Serializable {

    private String scorerFirstName;
    private String scorerLastName;
    private int scoreNumber;
    private int matchNumber;
}
