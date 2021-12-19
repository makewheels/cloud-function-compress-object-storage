package com.eg.compress;

import com.aliyun.fc.runtime.Context;
import com.aliyun.fc.runtime.StreamRequestHandler;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class AliyunRun implements StreamRequestHandler {
    @Override
    public void handleRequest(InputStream input, OutputStream output, Context context) throws IOException {
        System.out.println(System.currentTimeMillis() + "FWEF");
    }
}
