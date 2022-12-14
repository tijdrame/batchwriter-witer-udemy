package com.emard.batchwriter.tasket;

import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;

public class FileProcessTasklet implements Tasklet{

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) 
    throws Exception {
        System.out.println("File process task started");
        Thread.sleep(1000);
        System.out.println("File process task completed");
        return RepeatStatus.FINISHED;
    }
    
}
