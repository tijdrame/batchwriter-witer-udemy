package com.emard.batchwriter.listener;

import java.io.FileOutputStream;
import java.io.IOException;

import org.springframework.batch.core.annotation.OnSkipInProcess;
import org.springframework.batch.core.annotation.OnSkipInRead;
import org.springframework.batch.core.annotation.OnSkipInWrite;
import org.springframework.batch.item.file.FlatFileParseException;
import org.springframework.stereotype.Component;

@Component
public class ProductSkipListener {
    //chemin et nom du fichier
    private String readErrorFileName = "error/read_skipped";
    private String processErrorFileName = "error/process_skipped";

    @OnSkipInRead//pour l'exception en lecture
    public void onSkippRead(Throwable t){
        FlatFileParseException flat = (FlatFileParseException) t;
        onSkip(flat, readErrorFileName);
    }

    @OnSkipInProcess
    public void onSkippinProcess(Object item, Throwable t){
        if(t instanceof RuntimeException){
            onSkip(item, processErrorFileName);
        }
    }

    @OnSkipInWrite
    public void onSkippinWrite(Object item, Throwable t){
        if(t instanceof RuntimeException){
            onSkip(item, processErrorFileName);
        }
    }

    public void onSkip(Object o, String fName)  {
        FileOutputStream fos;
        try {
            fos = new FileOutputStream(fName, true);
            fos.write(o.toString().getBytes());
            fos.write("\r\n".getBytes());
            fos.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        
    }
}
