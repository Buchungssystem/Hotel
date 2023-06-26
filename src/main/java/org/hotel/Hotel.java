package org.hotel;

import org.utils.Participant;
import org.utils.VoteOptions;

import java.time.LocalDate;

public class Hotel extends Participant {

    @Override
    public VoteOptions Vote() {
        return VoteOptions.ABORT;
    }

    @Override
    public void book() {

    }

    @Override
    public void getAvailableItems(LocalDate startDate, LocalDate endDate) {

    }
}