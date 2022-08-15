package com.emard.batchwriter.tasket;

import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;

public class BusinessTasklet3 implements Tasklet{

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext)
    throws Exception {
        System.out.println("Business task 3 started");
        Thread.sleep(1000);
        System.out.println("Business task 3 completed");
        return RepeatStatus.FINISHED;
    }
    
}
